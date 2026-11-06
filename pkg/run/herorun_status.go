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

package run

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

type dirtyKey struct {
	repo  state.RepoKey
	runID string
}

// heroRunStatusPublisher maintains HeroRun status records at a bounded rate,
// coalescing rapid state transitions to reduce Kubernetes API server writes.
type heroRunStatusPublisher struct {
	kubeClient     client.Client
	store          state.Store
	clock          func() time.Time
	interval       time.Duration
	namespace      string
	publishHeroRun func(ctx context.Context, repo *state.Repo, run *state.Run) *herorunv1alpha1.HeroRun

	mu    sync.Mutex
	dirty map[dirtyKey]bool
}

// newHeroRunStatusPublisher constructs a heroRunStatusPublisher.
func newHeroRunStatusPublisher(
	kubeClient client.Client,
	store state.Store,
	clock func() time.Time,
	interval time.Duration,
	namespace string,
	publishHeroRun func(ctx context.Context, repo *state.Repo, run *state.Run) *herorunv1alpha1.HeroRun,
) *heroRunStatusPublisher {
	if clock == nil {
		clock = time.Now
	}
	return &heroRunStatusPublisher{
		kubeClient:     kubeClient,
		store:          store,
		clock:          clock,
		interval:       interval,
		namespace:      namespace,
		publishHeroRun: publishHeroRun,
		dirty:          make(map[dirtyKey]bool),
	}
}

// MarkDirty marks a run record as dirty so its HeroRun status is written on the next tick.
func (p *heroRunStatusPublisher) MarkDirty(repo state.RepoKey, runID string) {
	if p == nil || p.kubeClient == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dirty[dirtyKey{repo: repo, runID: runID}] = true
}

// Run wakes every interval, takes the set of dirty runs, and publishes status.
// If kubeClient is nil, Run returns immediately.
func (p *heroRunStatusPublisher) Run(ctx context.Context) error {
	if p == nil || p.kubeClient == nil {
		return nil
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			p.flush(shutdownCtx)
			cancel()
			return nil
		case <-ticker.C:
			p.flush(ctx)
		}
	}
}

// flush processes all currently dirty runs, writing status once per run.
func (p *heroRunStatusPublisher) flush(ctx context.Context) {
	if p == nil || p.kubeClient == nil {
		return
	}
	p.mu.Lock()
	if len(p.dirty) == 0 {
		p.mu.Unlock()
		return
	}
	dirty := p.dirty
	p.dirty = make(map[dirtyKey]bool)
	p.mu.Unlock()

	for key := range dirty {
		if ctx.Err() != nil {
			p.mu.Lock()
			p.dirty[key] = true
			p.mu.Unlock()
			return
		}
		if err := p.publishStatusForRun(ctx, key.repo, key.runID); err != nil {
			scoped.Log(ctx).Error(err, "failed to update HeroRun status", "repo", key.repo, "run", key.runID)
			p.MarkDirty(key.repo, key.runID)
		}
	}
}

func (p *heroRunStatusPublisher) publishStatusForRun(ctx context.Context, repoKey state.RepoKey, runID string) error {
	run, err := p.store.GetRun(ctx, repoKey, runID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("getting run %s: %w", runID, err)
	}

	name := sandboxNameForRun(run.ID)
	var hr herorunv1alpha1.HeroRun
	err = p.kubeClient.Get(ctx, client.ObjectKey{Namespace: p.namespace, Name: name}, &hr)
	if k8serrors.IsNotFound(err) {
		if p.publishHeroRun == nil {
			return fmt.Errorf("heroRun %s not found and publishHeroRun is nil", name)
		}
		repo, getRepoErr := p.store.GetRepo(ctx, repoKey)
		if getRepoErr != nil {
			return fmt.Errorf("getting repo %s for HeroRun publish: %w", repoKey, getRepoErr)
		}
		published := p.publishHeroRun(ctx, repo, run)
		if published == nil {
			return fmt.Errorf("failed to publish missing HeroRun %s", name)
		}
		hr = *published
	} else if err != nil {
		return fmt.Errorf("getting HeroRun %s: %w", name, err)
	}

	desiredPhase := string(run.Phase)
	desiredSandboxName := run.SandboxName
	desiredBranch := run.Branch
	desiredPRURL := run.PRURL

	var desiredStartedAt *metav1.Time
	if t, ok := run.PhaseTimestamps[state.PhasePreparing]; ok && !t.IsZero() {
		mt := metav1.NewTime(t)
		desiredStartedAt = &mt
	}

	var desiredFinishedAt *metav1.Time
	if run.Phase.IsTerminal() {
		if t, ok := run.PhaseTimestamps[run.Phase]; ok && !t.IsZero() {
			mt := metav1.NewTime(t)
			desiredFinishedAt = &mt
		} else {
			for _, phase := range []state.RunPhase{
				state.PhaseSucceeded,
				state.PhaseNoImprovement,
				state.PhaseFailed,
				state.PhaseCancelled,
				state.PhaseExpired,
			} {
				if t, ok := run.PhaseTimestamps[phase]; ok && !t.IsZero() {
					mt := metav1.NewTime(t)
					desiredFinishedAt = &mt
					break
				}
			}
			if desiredFinishedAt == nil {
				mt := metav1.NewTime(p.clock().UTC())
				desiredFinishedAt = &mt
			}
		}
	}

	desiredReadyStatus := metav1.ConditionFalse
	if run.Phase == state.PhaseWorking {
		desiredReadyStatus = metav1.ConditionTrue
	}
	desiredReadyReason := string(run.Phase)
	if desiredReadyReason == "" {
		desiredReadyReason = "Unknown"
	}
	desiredReadyMessage := ""
	if run.Phase == state.PhaseFailed && run.Error != "" {
		desiredReadyMessage = run.Error
	}

	if isStatusEqual(&hr.Status, desiredPhase, desiredSandboxName, desiredBranch, desiredPRURL, desiredStartedAt, desiredFinishedAt, desiredReadyStatus, desiredReadyReason, desiredReadyMessage) {
		return nil
	}

	hr.Status.Phase = desiredPhase
	hr.Status.SandboxName = desiredSandboxName
	hr.Status.Branch = desiredBranch
	hr.Status.PullRequestURL = desiredPRURL
	hr.Status.StartedAt = desiredStartedAt
	hr.Status.FinishedAt = desiredFinishedAt
	meta.SetStatusCondition(&hr.Status.Conditions, metav1.Condition{
		Type:    "Ready",
		Status:  desiredReadyStatus,
		Reason:  desiredReadyReason,
		Message: desiredReadyMessage,
	})
	now := metav1.NewTime(p.clock().UTC())
	hr.Status.ObservedAt = &now

	if err := p.kubeClient.Status().Update(ctx, &hr); err != nil {
		return fmt.Errorf("updating HeroRun %s status: %w", name, err)
	}
	return nil
}

func isStatusEqual(curr *herorunv1alpha1.HeroRunStatus, phase, sandboxName, branch, prURL string, startedAt, finishedAt *metav1.Time, readyStatus metav1.ConditionStatus, readyReason, readyMessage string) bool {
	if curr.Phase != phase {
		return false
	}
	if curr.SandboxName != sandboxName {
		return false
	}
	if curr.Branch != branch {
		return false
	}
	if curr.PullRequestURL != prURL {
		return false
	}
	if !timeEqual(curr.StartedAt, startedAt) {
		return false
	}
	if !timeEqual(curr.FinishedAt, finishedAt) {
		return false
	}
	cond := meta.FindStatusCondition(curr.Conditions, "Ready")
	if cond == nil {
		return false
	}
	if cond.Status != readyStatus || cond.Reason != readyReason || cond.Message != readyMessage {
		return false
	}
	return true
}

func timeEqual(a, b *metav1.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(b)
}
