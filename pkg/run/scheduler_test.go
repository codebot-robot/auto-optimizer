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
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestScheduler_NewGoalCreatesRun(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "optimize-latency",
			Body:     "Optimize latency below 50ms",
			Hash:     "hash-v1",
			Schedule: "on-change",
		},
		Hash:       "hash-v1",
		BaseCommit: "commit-aaa",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	scheduler, err := NewScheduler(SchedulerOptions{
		Store: store,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	runs, err := store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	run := runs[0]
	if run.GoalName != "optimize-latency" {
		t.Errorf("expected goal name optimize-latency, got %q", run.GoalName)
	}
	if run.Phase != state.PhasePending {
		t.Errorf("expected phase %s, got %s", state.PhasePending, run.Phase)
	}
	if run.BaseCommit != "commit-aaa" {
		t.Errorf("expected BaseCommit commit-aaa, got %q", run.BaseCommit)
	}
	if run.Goal == nil || run.Goal.Hash != "hash-v1" {
		t.Errorf("expected frozen goal with hash hash-v1, got %+v", run.Goal)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Phase != state.PhasePending {
		t.Errorf("expected event phase Pending, got %s", events[0].Phase)
	}
	if events[0].Data.(map[string]any)["reason"] != "new goal" {
		t.Errorf("expected reason 'new goal', got %v", events[0].Data)
	}

	// Verify goal.json was NOT modified
	recAfter, err := store.GetGoal(ctx, repo.Key(), "optimize-latency")
	if err != nil {
		t.Fatalf("GetGoal: %v", err)
	}
	if recAfter.LastRunID != "" {
		t.Errorf("expected goal.json not to be touched; LastRunID = %q", recAfter.LastRunID)
	}
}

func TestScheduler_UnchangedGoalDoesNotCreateRun(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "optimize-latency",
			Body:     "Optimize latency below 50ms",
			Hash:     "hash-v1",
			Schedule: "on-change",
		},
		Hash:       "hash-v1",
		BaseCommit: "commit-aaa",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	scheduler, err := NewScheduler(SchedulerOptions{
		Store: store,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	// First tick creates run
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}

	runs, err := store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	// Mark first run as terminal
	runs[0].SetPhase(state.PhaseSucceeded, now)
	if err := store.PutRun(ctx, runs[0]); err != nil {
		t.Fatalf("PutRun terminal: %v", err)
	}

	// Second tick with unchanged goal should NOT create a second run
	now = now.Add(time.Minute)
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	runsAfter, err := store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns after: %v", err)
	}
	if len(runsAfter) != 1 {
		t.Fatalf("expected still 1 run for unchanged goal, got %d", len(runsAfter))
	}
}

func TestScheduler_ChangedHashCreatesSecondRunOnceFirstIsTerminal(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "optimize-latency",
			Body:     "Optimize latency below 50ms",
			Hash:     "hash-v1",
			Schedule: "on-change",
		},
		Hash:       "hash-v1",
		BaseCommit: "commit-aaa",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	currentTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	scheduler, err := NewScheduler(SchedulerOptions{
		Store: store,
		Clock: func() time.Time { return currentTime },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	// First tick -> Run 1 (Pending)
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}

	runs, err := store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	// Update goal with a new hash
	goalRec.Goal.Hash = "hash-v2"
	goalRec.Hash = "hash-v2"
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal v2: %v", err)
	}

	// First run is still Pending (non-terminal). Tick should NOT create a second run yet.
	currentTime = currentTime.Add(time.Minute)
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick with non-terminal: %v", err)
	}

	runs, err = store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected non-terminal run to block new run, got %d runs", len(runs))
	}

	// Now mark first run terminal
	runs[0].SetPhase(state.PhaseSucceeded, currentTime)
	if err := store.PutRun(ctx, runs[0]); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Tick again -> should create Run 2 because hash changed and Run 1 is terminal
	currentTime = currentTime.Add(time.Minute)
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick after terminal: %v", err)
	}

	runs, err = store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}

	secondRun := runs[1]
	if secondRun.Goal == nil || secondRun.Goal.Hash != "hash-v2" {
		t.Errorf("expected second run to have frozen goal hash-v2, got %+v", secondRun.Goal)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), secondRun.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data.(map[string]any)["reason"] != "hash changed" {
		t.Errorf("expected reason 'hash changed', got %v", events[0].Data)
	}
}

func TestScheduler_NonTerminalRunBlocksNewOne(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "task",
			Body:     "Task",
			Hash:     "hash-1",
			Schedule: "on-change",
		},
		Hash:       "hash-1",
		BaseCommit: "commit-1",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	nonTerminalPhases := []state.RunPhase{
		state.PhasePending,
		state.PhasePreparing,
		state.PhaseWorking,
		state.PhaseSelecting,
		state.PhaseOpeningPR,
	}

	for _, phase := range nonTerminalPhases {
		t.Run(string(phase), func(t *testing.T) {
			subDir := t.TempDir()
			subStore := state.NewFileStore(subDir)
			if err := subStore.PutRepo(ctx, repo); err != nil {
				t.Fatalf("PutRepo: %v", err)
			}
			if err := subStore.PutGoal(ctx, repo.Key(), goalRec); err != nil {
				t.Fatalf("PutGoal: %v", err)
			}

			// Pre-create a run in the non-terminal phase
			run := &state.Run{
				ID:         "20261030T100000Z-0001",
				Repo:       repo.Key(),
				GoalName:   "task",
				BaseCommit: "commit-1",
				Phase:      phase,
				Goal:       goalRec.Goal,
			}
			if err := subStore.PutRun(ctx, run); err != nil {
				t.Fatalf("PutRun: %v", err)
			}

			// Change the goal so it would otherwise trigger
			changedGoal := *goalRec
			changedGoal.Hash = "hash-different"
			changedGoal.Goal = &goals.Goal{
				Name:     "task",
				Body:     "Task",
				Hash:     "hash-different",
				Schedule: "on-change",
			}
			if err := subStore.PutGoal(ctx, repo.Key(), &changedGoal); err != nil {
				t.Fatalf("PutGoal: %v", err)
			}

			scheduler, err := NewScheduler(SchedulerOptions{Store: subStore})
			if err != nil {
				t.Fatalf("NewScheduler: %v", err)
			}
			if err := scheduler.Tick(ctx); err != nil {
				t.Fatalf("Tick: %v", err)
			}

			runs, err := subStore.ListRuns(ctx, repo.Key())
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			if len(runs) != 1 {
				t.Fatalf("expected non-terminal phase %s to block new run, got %d runs", phase, len(runs))
			}
		})
	}
}

func TestScheduler_ManualGoalNeverStarts(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	schedulesToSkip := []string{"manual", "daily", "weekly"}
	for _, sched := range schedulesToSkip {
		t.Run(sched, func(t *testing.T) {
			goalRec := &state.GoalRecord{
				Goal: &goals.Goal{
					Name:     "skipped-goal-" + sched,
					Body:     "Body",
					Hash:     "hash-1",
					Schedule: sched,
				},
				Hash:       "hash-1",
				BaseCommit: "commit-1",
			}
			if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
				t.Fatalf("PutGoal: %v", err)
			}

			scheduler, err := NewScheduler(SchedulerOptions{Store: store})
			if err != nil {
				t.Fatalf("NewScheduler: %v", err)
			}
			if err := scheduler.Tick(ctx); err != nil {
				t.Fatalf("Tick: %v", err)
			}

			runs, err := store.ListRuns(ctx, repo.Key())
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			for _, r := range runs {
				if r.GoalName == goalRec.Goal.Name {
					t.Fatalf("goal with schedule %q should not have started, found run %s", sched, r.ID)
				}
			}
		})
	}
}

func TestScheduler_BaseCommitChangedCreatesRun(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	store := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "task",
			Body:     "Task",
			Hash:     "hash-fixed",
			Schedule: "on-change",
		},
		Hash:       "hash-fixed",
		BaseCommit: "commit-old",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	currTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	scheduler, err := NewScheduler(SchedulerOptions{
		Store: store,
		Clock: func() time.Time { return currTime },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	runs, err := store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}

	// Mark first run terminal
	runs[0].SetPhase(state.PhaseSucceeded, currTime)
	if err := store.PutRun(ctx, runs[0]); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Update base commit on the goal record (e.g. main branch advanced), but keep hash the same
	goalRec.BaseCommit = "commit-new"
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	currTime = currTime.Add(time.Minute)
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}

	runs, err = store.ListRuns(ctx, repo.Key())
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs after base commit changed, got %d", len(runs))
	}

	secondRun := runs[1]
	if secondRun.BaseCommit != "commit-new" {
		t.Errorf("expected BaseCommit commit-new, got %q", secondRun.BaseCommit)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), secondRun.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Data.(map[string]any)["reason"] != "base commit changed" {
		t.Errorf("expected reason 'base commit changed', got %v", events[0].Data)
	}
}
