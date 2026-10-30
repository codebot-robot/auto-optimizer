// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package syncer polls registered repositories, discovers goals in .goals/,
// and records them in the state store. The syncer is the only writer of
// repo.json and goal.json.
package syncer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

const (
	// DefaultPollInterval is the baseline polling period.
	DefaultPollInterval = 60 * time.Second
	// DefaultJitter is the maximum deviation applied to the polling period.
	DefaultJitter = 10 * time.Second
)

// Options configures a Syncer.
type Options struct {
	Store        state.Store
	Forge        forge.Forge
	PollInterval time.Duration
	Jitter       time.Duration
}

// Syncer periodically polls registered repositories to discover and update goals.
type Syncer struct {
	store        state.Store
	forge        forge.Forge
	pollInterval time.Duration
	jitter       time.Duration
}

// New creates a new Syncer.
func New(opts Options) (*Syncer, error) {
	if opts.Store == nil {
		return nil, errors.New("state store is required")
	}
	if opts.Forge == nil {
		return nil, errors.New("forge is required")
	}

	interval := opts.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	jitter := opts.Jitter
	if jitter <= 0 {
		jitter = DefaultJitter
	}

	return &Syncer{
		store:        opts.Store,
		forge:        opts.Forge,
		pollInterval: interval,
		jitter:       jitter,
	}, nil
}

// Run begins the polling loop until ctx is cancelled.
func (s *Syncer) Run(ctx context.Context) error {
	log := scoped.Log(ctx)
	log.Info("starting syncer loop", "interval", s.pollInterval)

	// Perform initial sync immediately
	if err := s.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error(err, "initial sync encountered errors")
	}

	for {
		wait := s.nextInterval()
		select {
		case <-ctx.Done():
			log.Info("syncer stopped")
			return ctx.Err()
		case <-time.After(wait):
			if err := s.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error(err, "periodic sync encountered errors")
			}
		}
	}
}

func (s *Syncer) nextInterval() time.Duration {
	if s.jitter <= 0 {
		return s.pollInterval
	}
	// Random offset between [-jitter, +jitter]
	rangeMs := int64(2 * s.jitter / time.Millisecond)
	if rangeMs <= 0 {
		return s.pollInterval
	}
	n, err := rand.Int(rand.Reader, big.NewInt(rangeMs))
	if err != nil {
		return s.pollInterval
	}
	offset := time.Duration(n.Int64())*time.Millisecond - s.jitter
	interval := s.pollInterval + offset
	if interval <= 0 {
		interval = time.Second
	}
	return interval
}

// SyncOnce polls all registered repositories once.
func (s *Syncer) SyncOnce(ctx context.Context) error {
	repos, err := s.store.ListRepos(ctx)
	if err != nil {
		return fmt.Errorf("listing registered repos: %w", err)
	}

	var errs []error
	for _, repo := range repos {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.SyncRepo(ctx, repo); err != nil {
			errs = append(errs, fmt.Errorf("syncing %s: %w", repo.URL, err))
		}
	}

	return errors.Join(errs...)
}

// SyncRepo checks for updates in a single repository and updates its goals.
func (s *Syncer) SyncRepo(ctx context.Context, repo *state.Repo) error {
	log := scoped.Log(ctx)

	refs, err := s.forge.ListRefs(ctx, repo.URL)
	if err != nil {
		return fmt.Errorf("listing refs: %w", err)
	}

	headCommit := s.findHeadCommit(refs, repo.DefaultBranch)
	if headCommit == "" {
		return fmt.Errorf("no head commit found for branch %q in %s", repo.DefaultBranch, repo.URL)
	}

	if headCommit == repo.HeadCommit {
		return nil
	}

	treeFS, err := s.forge.FetchTree(ctx, repo.URL, headCommit, ".goals")
	if err != nil {
		return fmt.Errorf("fetching tree at commit %s: %w", headCommit, err)
	}

	discoveredGoals, err := goals.LoadDir(treeFS, ".goals")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("loading goals: %w", err)
	}

	key := repo.Key()
	existingGoals, err := s.store.ListGoals(ctx, key)
	if err != nil {
		return fmt.Errorf("listing existing goals: %w", err)
	}

	existingMap := make(map[string]*state.GoalRecord, len(existingGoals))
	for _, eg := range existingGoals {
		if eg.Goal != nil && eg.Goal.Name != "" {
			existingMap[eg.Goal.Name] = eg
		}
	}

	foundMap := make(map[string]bool, len(discoveredGoals))
	for _, g := range discoveredGoals {
		foundMap[g.Name] = true
		existing, exists := existingMap[g.Name]

		if !exists || existing.Hash != g.Hash || existing.BaseCommit != headCommit {
			record := &state.GoalRecord{
				Goal:       g,
				Hash:       g.Hash,
				BaseCommit: headCommit,
			}
			if exists {
				record.BestValue = existing.BestValue
				record.LastRunID = existing.LastRunID
			}

			if err := s.store.PutGoal(ctx, key, record); err != nil {
				return fmt.Errorf("writing goal %s: %w", g.Name, err)
			}
			log.Info("goal updated", "repo", repo.URL, "goal", g.Name, "commit", headCommit)
		}
	}

	// Remove goals that no longer exist in the repository tree
	for _, eg := range existingGoals {
		if eg.Goal == nil || eg.Goal.Name == "" {
			continue
		}
		if !foundMap[eg.Goal.Name] {
			if err := s.store.DeleteGoal(ctx, key, eg.Goal.Name); err != nil {
				return fmt.Errorf("deleting removed goal %s: %w", eg.Goal.Name, err)
			}
			log.Info("goal removed", "repo", repo.URL, "goal", eg.Goal.Name, "commit", headCommit)
		}
	}

	// Update head commit in repo.json
	repo.HeadCommit = headCommit
	if err := s.store.PutRepo(ctx, repo); err != nil {
		return fmt.Errorf("updating repo head commit: %w", err)
	}

	return nil
}

func (s *Syncer) findHeadCommit(refs map[string]string, defaultBranch string) string {
	if defaultBranch != "" {
		if c, ok := refs["refs/heads/"+defaultBranch]; ok {
			return c
		}
		if c, ok := refs[defaultBranch]; ok {
			return c
		}
	}
	if c, ok := refs["refs/heads/main"]; ok {
		return c
	}
	if c, ok := refs["refs/heads/master"]; ok {
		return c
	}
	return ""
}
