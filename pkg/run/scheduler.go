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

// Package run orchestrates runs for registered goals.
package run

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// ErrActiveRun is returned by StartRun when the goal already has an active run.
var ErrActiveRun = errors.New("goal already has an active run")

// ErrOpenPullRequest is returned by StartRun when the goal already has an open pull request.
var ErrOpenPullRequest = errors.New("goal already has an open pull request")

// ErrGoalNotFound is returned by StartRun when the goal is not found in the repository state.
var ErrGoalNotFound = errors.New("goal is no longer in the repository")

// DefaultSchedulerInterval is the default tick interval for the scheduler loop.
const DefaultSchedulerInterval = 60 * time.Second

// SchedulerOptions configures a Scheduler.
type SchedulerOptions struct {
	Store        state.Store
	Orchestrator *Orchestrator
	Clock        func() time.Time
	Interval     time.Duration
	ForgeFor     func(repoURL string) (forge.Forge, error)
}

// InitDefaults fills every zero-valued field that has a default with its Default* constant.
func (o *SchedulerOptions) InitDefaults() {
	if o == nil {
		return
	}
	if o.Clock == nil {
		o.Clock = func() time.Time { return time.Now().UTC() }
	}
	if o.Interval == 0 {
		o.Interval = DefaultSchedulerInterval
	}
}

// Validate reports any missing required dependencies or invalid option values.
func (o *SchedulerOptions) Validate() error {
	if o == nil {
		return errors.New("options are required")
	}
	if o.Store == nil {
		return errors.New("state store is required")
	}
	if o.Interval <= 0 {
		return errors.New("Interval must be positive")
	}
	return nil
}

// Scheduler inspects goals and creates pending runs for goals that have changed.
type Scheduler struct {
	Store        state.Store
	Orchestrator *Orchestrator
	Clock        func() time.Time
	Interval     time.Duration
	ForgeFor     func(repoURL string) (forge.Forge, error)
}

// NewScheduler creates a new Scheduler with the given options.
func NewScheduler(opts SchedulerOptions) (*Scheduler, error) {
	opts.InitDefaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &Scheduler{
		Store:        opts.Store,
		Orchestrator: opts.Orchestrator,
		Clock:        opts.Clock,
		Interval:     opts.Interval,
		ForgeFor:     opts.ForgeFor,
	}, nil
}

func (s *Scheduler) now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now().UTC()
}

func (s *Scheduler) interval() time.Duration {
	if s.Interval > 0 {
		return s.Interval
	}
	return DefaultSchedulerInterval
}

// Run begins the scheduler loop, ticking every interval until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	log := scoped.Log(ctx)
	interval := s.interval()
	log.Info("starting scheduler loop", "interval", interval)

	// Perform initial tick immediately
	if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error(err, "initial scheduler tick encountered errors")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("scheduler stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error(err, "periodic scheduler tick encountered errors")
			}
		}
	}
}

// Tick lists repositories and their goals, creating Pending runs for eligible changed goals.
func (s *Scheduler) Tick(ctx context.Context) error {
	if s.Store == nil {
		return errors.New("state store is required")
	}

	repos, err := s.Store.ListRepos(ctx)
	if err != nil {
		return fmt.Errorf("listing repositories: %w", err)
	}

	var errs []error
	for _, repo := range repos {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.tickRepo(ctx, repo); err != nil {
			errs = append(errs, fmt.Errorf("scheduling repo %s: %w", repo.URL, err))
		}
	}

	return errors.Join(errs...)
}

func (s *Scheduler) tickRepo(ctx context.Context, repo *state.Repo) error {
	log := scoped.Log(ctx)
	key := repo.Key()

	goalsList, err := s.Store.ListGoals(ctx, key)
	if err != nil {
		return fmt.Errorf("listing goals: %w", err)
	}

	runs, err := s.Store.ListRuns(ctx, key)
	if err != nil {
		return fmt.Errorf("listing runs: %w", err)
	}

	var errs []error
	for _, goalRec := range goalsList {
		if goalRec == nil || goalRec.Goal == nil {
			continue
		}
		g := goalRec.Goal
		if g.Name == "" {
			continue
		}

		sched := g.Schedule
		if sched == "" {
			sched = "on-change"
		}

		switch sched {
		case "on-change":
			// Evaluated below
		case "manual":
			log.Info("skipping goal: manual schedule goals are not triggered automatically", "repo", repo.URL, "goal", g.Name, "schedule", sched)
			continue
		case "daily", "weekly":
			log.Info("skipping goal: periodic schedule not yet implemented", "repo", repo.URL, "goal", g.Name, "schedule", sched)
			continue
		default:
			log.Info("skipping goal: unsupported schedule", "repo", repo.URL, "goal", g.Name, "schedule", sched)
			continue
		}

		// Look at the goal's most recent run (ListRuns filtered by goal name, newest first)
		latestRun := latestRunForGoal(runs, g.Name)

		// One active run per goal: if the latest run is not in a terminal phase, do nothing.
		if latestRun != nil && !latestRun.Phase.IsTerminal() {
			log.V(2).Info("goal already has an active run; doing nothing", "repo", repo.URL, "goal", g.Name, "run", latestRun.ID, "phase", latestRun.Phase)
			continue
		}

		// Create a new run when there is no run yet, or when hash or base commit differs.
		var reason string
		if latestRun == nil {
			reason = "new goal"
		} else {
			var latestGoalHash string
			if latestRun.Goal != nil {
				latestGoalHash = latestRun.Goal.Hash
			}
			if goalRec.Hash != latestGoalHash {
				reason = "hash changed"
			} else if goalRec.BaseCommit != latestRun.BaseCommit {
				reason = "base commit changed"
			}
		}

		if reason == "" {
			// Unchanged goal; no run needed
			continue
		}

		run, err := s.StartRun(ctx, repo, g.Name, reason)
		if err != nil {
			if errors.Is(err, ErrOpenPullRequest) {
				log.V(2).Info("goal already has an open pull request; doing nothing", "repo", repo.URL, "goal", g.Name, "error", err)
				continue
			}
			if errors.Is(err, ErrActiveRun) {
				log.V(2).Info("goal already has an active run; doing nothing", "repo", repo.URL, "goal", g.Name, "error", err)
				continue
			}
			errs = append(errs, fmt.Errorf("starting run for goal %s: %w", g.Name, err))
			continue
		}
		runs = append(runs, run)
	}

	return errors.Join(errs...)
}

// StartRun validates and creates a new Pending run for a named goal in repo.
// It refuses with an error if the goal already has a non-terminal run, an open pull request,
// or if the goal is no longer in the repository.
func (s *Scheduler) StartRun(ctx context.Context, repo *state.Repo, goalName string, reason string) (*state.Run, error) {
	if s.Store == nil {
		return nil, errors.New("state store is required")
	}
	if repo == nil {
		return nil, errors.New("repo is required")
	}
	if goalName == "" {
		return nil, errors.New("goal name is required")
	}

	key := repo.Key()
	goalRec, err := s.Store.GetGoal(ctx, key, goalName)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil, fmt.Errorf("goal %q is no longer in the repository: %w", goalName, ErrGoalNotFound)
		}
		return nil, fmt.Errorf("getting goal %s: %w", goalName, err)
	}
	if goalRec == nil || goalRec.Goal == nil {
		return nil, fmt.Errorf("goal %q is no longer in the repository: %w", goalName, ErrGoalNotFound)
	}
	if goalRec.BaseCommit == "" {
		return nil, fmt.Errorf("goal %q has no base commit", goalName)
	}

	runs, err := s.Store.ListRuns(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("listing runs: %w", err)
	}
	latestRun := latestRunForGoal(runs, goalName)
	if latestRun != nil && !latestRun.Phase.IsTerminal() {
		return nil, fmt.Errorf("%w: goal %q has run %s in phase %s", ErrActiveRun, goalName, latestRun.ID, latestRun.Phase)
	}

	openPR, err := s.findOpenPullRequest(ctx, repo, goalName)
	if err != nil {
		scoped.Log(ctx).Error(err, "listing pull requests; continuing", "repo", repo.URL, "goal", goalName)
	} else if openPR != nil {
		return nil, fmt.Errorf("%w: goal %q has pull request %s", ErrOpenPullRequest, goalName, openPR.URL)
	}

	now := s.now()
	runID, err := state.NewRunIDAt(now)
	if err != nil {
		return nil, fmt.Errorf("generating run id for goal %s: %w", goalName, err)
	}

	frozenGoal := cloneGoal(goalRec.Goal)
	run := &state.Run{
		ID:         runID,
		Repo:       key,
		GoalName:   goalName,
		BaseCommit: goalRec.BaseCommit,
		Goal:       frozenGoal,
	}
	run.SetPhase(state.PhasePending, now)

	if err := s.Store.PutRun(ctx, run); err != nil {
		return nil, fmt.Errorf("creating run %s for goal %s: %w", runID, goalName, err)
	}

	event := state.Event{
		Time:    now,
		Phase:   state.PhasePending,
		Kind:    "created",
		Message: fmt.Sprintf("Run created: %s", reason),
		Data:    map[string]any{"reason": reason},
	}
	if err := s.Store.AppendEvent(ctx, key, runID, event); err != nil {
		return nil, fmt.Errorf("appending event for run %s: %w", runID, err)
	}

	scoped.Log(ctx).Info("created pending run", "repo", repo.URL, "goal", goalName, "run", runID, "reason", reason)
	if s.Orchestrator != nil {
		s.Orchestrator.publishHeroRun(ctx, repo, run)
		s.Orchestrator.Notify(key, runID)
	}

	return run, nil
}

func (s *Scheduler) findOpenPullRequest(ctx context.Context, repo *state.Repo, goalName string) (*forge.PullRequest, error) {
	if s.ForgeFor == nil {
		return nil, nil
	}
	f, err := s.ForgeFor(repo.URL)
	if err != nil {
		return nil, nil
	}
	cr, ok := f.(forge.ChangeRequestCreator)
	if !ok {
		return nil, nil
	}
	prs, err := cr.ListPullRequests(ctx, repo.URL, "ao/"+goalName+"/")
	if err != nil {
		return nil, err
	}
	for i := range prs {
		if prs[i].State == "open" {
			return &prs[i], nil
		}
	}
	return nil, nil
}

func latestRunForGoal(runs []*state.Run, goalName string) *state.Run {
	var goalRuns []*state.Run
	for _, r := range runs {
		if r.GoalName == goalName {
			goalRuns = append(goalRuns, r)
		}
	}
	if len(goalRuns) == 0 {
		return nil
	}
	slices.SortFunc(goalRuns, func(a, b *state.Run) int {
		return strings.Compare(b.ID, a.ID)
	})
	return goalRuns[0]
}

func cloneGoal(g *goals.Goal) *goals.Goal {
	if g == nil {
		return nil
	}
	clone := *g
	if g.Metric != nil {
		m := *g.Metric
		clone.Metric = &m
	}
	if g.Budget != nil {
		b := *g.Budget
		clone.Budget = &b
	}
	if g.Agents != nil {
		clone.Agents = slices.Clone(g.Agents)
	}
	return &clone
}
