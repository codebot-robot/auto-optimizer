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

package run_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func sampleGoal() *goals.Goal {
	return &goals.Goal{
		Name: "optimize-latency",
		Body: "# Optimize latency\nMake things faster.",
		Hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Metric: &goals.Metric{
			Command:        "go test -bench=BenchmarkRun",
			Direction:      "lower",
			Unit:           "ns/op",
			MinImprovement: "5%",
			Runs:           3,
			Timeout:        "30s",
		},
		Verify: "go test ./...",
		Budget: &goals.Budget{
			MaxDuration: "2h",
			MaxCostUSD:  5.0,
		},
		Schedule: "on-change",
		Agents:   []string{"coder", "reviewer"},
	}
}

func TestRoundTrip_AllPhases_ThroughJSON(t *testing.T) {
	baseTime := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	deadline := baseTime.Add(2 * time.Hour)
	readyDeadline := baseTime.Add(10 * time.Minute)

	baselineVal := 120.5
	candidateVal := 98.2

	phases := []struct {
		name string
		run  *state.Run
	}{
		{
			name: "Pending",
			run: &state.Run{
				ID:         "20261122T100000Z-0001",
				GoalName:   "optimize-latency",
				Phase:      state.PhasePending,
				BaseCommit: "0123456789abcdef0123456789abcdef01234567",
				Deadline:   &deadline,
				Goal:       sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending: baseTime,
				},
			},
		},
		{
			name: "Preparing",
			run: &state.Run{
				ID:                 "20261122T100000Z-0002",
				GoalName:           "optimize-latency",
				Phase:              state.PhasePreparing,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				SandboxName:        "run-20261122t100000z-0002",
				WorkspaceClaimName: "run-20261122t100000z-0002",
				ReadyDeadline:      &readyDeadline,
				Deadline:           &deadline,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
				},
			},
		},
		{
			name: "Working",
			run: &state.Run{
				ID:                 "20261122T100000Z-0003",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseWorking,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				Branch:             "auto-optimizer/optimize-latency/20261122T100000Z-0003",
				SessionID:          "sess-work-0003",
				SandboxName:        "run-20261122t100000z-0003",
				WorkspaceClaimName: "run-20261122t100000z-0003",
				Deadline:           &deadline,
				CostUSD:            0.45,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseWorking:   baseTime.Add(3 * time.Minute),
				},
			},
		},
		{
			name: "Selecting",
			run: &state.Run{
				ID:                 "20261122T100000Z-0004",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseSelecting,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				Branch:             "auto-optimizer/optimize-latency/20261122T100000Z-0004",
				SessionID:          "sess-work-0004",
				SandboxName:        "run-20261122t100000z-0004",
				WorkspaceClaimName: "run-20261122t100000z-0004",
				Deadline:           &deadline,
				Baseline:           &baselineVal,
				Candidate:          &candidateVal,
				CostUSD:            1.20,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseWorking:   baseTime.Add(3 * time.Minute),
					state.PhaseSelecting: baseTime.Add(20 * time.Minute),
				},
			},
		},
		{
			name: "OpeningPR",
			run: &state.Run{
				ID:                 "20261122T100000Z-0005",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseOpeningPR,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				Branch:             "auto-optimizer/optimize-latency/20261122T100000Z-0005",
				PRTitle:            "optimize latency by 18%",
				PRBody:             "Reduces benchmark runtime from 120.5 to 98.2 ns/op.",
				PRURL:              "https://github.com/gke-labs/auto-optimizer/pull/123",
				SessionID:          "sess-work-0005",
				SandboxName:        "run-20261122t100000z-0005",
				WorkspaceClaimName: "run-20261122t100000z-0005",
				Deadline:           &deadline,
				Baseline:           &baselineVal,
				Candidate:          &candidateVal,
				CostUSD:            1.50,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseWorking:   baseTime.Add(3 * time.Minute),
					state.PhaseSelecting: baseTime.Add(20 * time.Minute),
					state.PhaseOpeningPR: baseTime.Add(25 * time.Minute),
				},
			},
		},
		{
			name: "Succeeded",
			run: &state.Run{
				ID:                 "20261122T100000Z-0006",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseSucceeded,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				Branch:             "auto-optimizer/optimize-latency/20261122T100000Z-0006",
				PRTitle:            "optimize latency by 18%",
				PRBody:             "Reduces benchmark runtime from 120.5 to 98.2 ns/op.",
				PRURL:              "https://github.com/gke-labs/auto-optimizer/pull/123",
				SessionID:          "sess-work-0006",
				SandboxName:        "run-20261122t100000z-0006",
				WorkspaceClaimName: "run-20261122t100000z-0006",
				Deadline:           &deadline,
				Summary:            "Successfully optimized benchmark and opened PR #123.",
				Baseline:           &baselineVal,
				Candidate:          &candidateVal,
				CostUSD:            1.85,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseWorking:   baseTime.Add(3 * time.Minute),
					state.PhaseSelecting: baseTime.Add(20 * time.Minute),
					state.PhaseOpeningPR: baseTime.Add(25 * time.Minute),
					state.PhaseSucceeded: baseTime.Add(30 * time.Minute),
				},
			},
		},
		{
			name: "NoImprovement",
			run: &state.Run{
				ID:                 "20261122T100000Z-0007",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseNoImprovement,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				Branch:             "auto-optimizer/optimize-latency/20261122T100000Z-0007",
				SessionID:          "sess-work-0007",
				SandboxName:        "run-20261122t100000z-0007",
				WorkspaceClaimName: "run-20261122t100000z-0007",
				Deadline:           &deadline,
				Summary:            "Explored 3 approaches; none beat baseline.",
				Baseline:           &baselineVal,
				Candidate:          &baselineVal,
				CostUSD:            0.95,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:       baseTime,
					state.PhasePreparing:     baseTime.Add(1 * time.Minute),
					state.PhaseWorking:       baseTime.Add(3 * time.Minute),
					state.PhaseNoImprovement: baseTime.Add(40 * time.Minute),
				},
			},
		},
		{
			name: "VerifyFailed",
			run: &state.Run{
				ID:                 "20261122T100000Z-0011",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseVerifyFailed,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				Branch:             "auto-optimizer/optimize-latency/20261122T100000Z-0011",
				SessionID:          "sess-work-0011",
				SandboxName:        "run-20261122t100000z-0011",
				WorkspaceClaimName: "run-20261122t100000z-0011",
				Deadline:           &deadline,
				CostUSD:            1.10,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:      baseTime,
					state.PhasePreparing:    baseTime.Add(1 * time.Minute),
					state.PhaseWorking:      baseTime.Add(3 * time.Minute),
					state.PhaseSelecting:    baseTime.Add(20 * time.Minute),
					state.PhaseVerifyFailed: baseTime.Add(22 * time.Minute),
				},
			},
		},
		{
			name: "Failed",
			run: &state.Run{
				ID:                 "20261122T100000Z-0008",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseFailed,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				SandboxName:        "run-20261122t100000z-0008",
				WorkspaceClaimName: "run-20261122t100000z-0008",
				ReadyDeadline:      &readyDeadline,
				Deadline:           &deadline,
				Error:              "sandbox failed to achieve readiness before deadline",
				ErrorKind:          "failed",
				Summary:            "Infrastructure error during sandbox preparation.",
				CostUSD:            0.05,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseFailed:    baseTime.Add(12 * time.Minute),
				},
			},
		},
		{
			name: "Cancelled",
			run: &state.Run{
				ID:                 "20261122T100000Z-0009",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseCancelled,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				SandboxName:        "run-20261122t100000z-0009",
				WorkspaceClaimName: "run-20261122t100000z-0009",
				Deadline:           &deadline,
				Summary:            "Run cancelled by administrator request.",
				CostUSD:            0.20,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseWorking:   baseTime.Add(3 * time.Minute),
					state.PhaseCancelled: baseTime.Add(10 * time.Minute),
				},
			},
		},
		{
			name: "Expired",
			run: &state.Run{
				ID:                 "20261122T100000Z-0010",
				GoalName:           "optimize-latency",
				Phase:              state.PhaseExpired,
				BaseCommit:         "0123456789abcdef0123456789abcdef01234567",
				SandboxName:        "run-20261122t100000z-0010",
				WorkspaceClaimName: "run-20261122t100000z-0010",
				Deadline:           &deadline,
				Summary:            "Run reached overall deadline before completion.",
				CostUSD:            2.50,
				Goal:               sampleGoal(),
				PhaseTimestamps: map[state.RunPhase]time.Time{
					state.PhasePending:   baseTime,
					state.PhasePreparing: baseTime.Add(1 * time.Minute),
					state.PhaseWorking:   baseTime.Add(3 * time.Minute),
					state.PhaseExpired:   baseTime.Add(2 * time.Hour),
				},
			},
		},
	}

	for _, tc := range phases {
		t.Run(tc.name, func(t *testing.T) {
			hr := run.HeroRunFromRun(tc.run)

			// Go through full JSON serialization and deserialization
			data, err := json.Marshal(hr)
			if err != nil {
				t.Fatalf("json.Marshal failed: %v", err)
			}

			var unmarshaled herorunv1alpha1.HeroRun
			if err := json.Unmarshal(data, &unmarshaled); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}

			// Convert back to state.Run
			roundTripped, err := run.HeroRunToRun(&unmarshaled)
			if err != nil {
				t.Fatalf("HeroRunToRun failed: %v", err)
			}

			// Verify zero loss across round trip through JSON
			if diff := cmp.Diff(tc.run, roundTripped); diff != "" {
				t.Errorf("round-trip through JSON mismatch for phase %s (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestHeroRunToRun_RepositoryParsing(t *testing.T) {
	hr := &herorunv1alpha1.HeroRun{
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      "test-run",
			Repository: "https://github.com/gke-labs/test-repo",
		},
	}

	r, err := run.HeroRunToRun(hr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantRepo := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	if r.Repo != wantRepo {
		t.Errorf("got repo %+v, want %+v", r.Repo, wantRepo)
	}

	// Invalid repository URL returns an error
	hr.Spec.Repository = "invalid://///repo"
	if _, err := run.HeroRunToRun(hr); err == nil {
		t.Errorf("expected error parsing invalid repository, got nil")
	}
}

func TestTimestampTruncation(t *testing.T) {
	// Timestamps with nanoseconds should be truncated to whole seconds
	nanoTime := time.Date(2026, 11, 22, 10, 0, 0, 123456789, time.UTC)
	expectedTime := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	r := &state.Run{
		ID:       "run-nano",
		Deadline: &nanoTime,
		PhaseTimestamps: map[state.RunPhase]time.Time{
			state.PhasePending: nanoTime,
		},
	}

	hr := run.HeroRunFromRun(r)
	if hr.Spec.Deadline.Time != expectedTime {
		t.Errorf("HeroRun deadline not truncated: got %v, want %v", hr.Spec.Deadline.Time, expectedTime)
	}
	if hr.Status.PhaseTimestamps["Pending"].Time != expectedTime {
		t.Errorf("HeroRun phase timestamp not truncated: got %v, want %v", hr.Status.PhaseTimestamps["Pending"].Time, expectedTime)
	}

	back, err := run.HeroRunToRun(hr)
	if err != nil {
		t.Fatalf("HeroRunToRun failed: %v", err)
	}
	if *back.Deadline != expectedTime {
		t.Errorf("state.Run deadline not truncated: got %v, want %v", *back.Deadline, expectedTime)
	}
	if back.PhaseTimestamps[state.PhasePending] != expectedTime {
		t.Errorf("state.Run phase timestamp not truncated: got %v, want %v", back.PhaseTimestamps[state.PhasePending], expectedTime)
	}
}

func TestGetRun_And_ListRuns(t *testing.T) {
	ctx := t.Context()
	scheme := runtime.NewScheme()
	_ = herorunv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	const ns = "test-sandboxes"

	// Non-existent run returns ErrNotFound
	if _, err := run.GetRun(ctx, fakeClient, ns, "nonexistent"); !errors.Is(err, state.ErrNotFound) {
		t.Errorf("GetRun nonexistent = %v, want state.ErrNotFound", err)
	}

	repoURL1 := "https://github.com/example/repo1"
	repoURL2 := "https://github.com/example/repo2"

	r1 := &state.Run{
		ID:       "20261123T100000Z-0001",
		GoalName: "goal-1",
		Phase:    state.PhaseWorking,
	}
	hr1 := run.HeroRunFromRun(r1)
	hr1.Namespace = ns
	hr1.Spec.Repository = repoURL1
	if err := fakeClient.Create(ctx, hr1); err != nil {
		t.Fatalf("Create hr1: %v", err)
	}

	r2 := &state.Run{
		ID:       "20261123T110000Z-0002",
		GoalName: "goal-2",
		Phase:    state.PhasePending,
	}
	hr2 := run.HeroRunFromRun(r2)
	hr2.Namespace = ns
	hr2.Spec.Repository = repoURL1
	if err := fakeClient.Create(ctx, hr2); err != nil {
		t.Fatalf("Create hr2: %v", err)
	}

	r3 := &state.Run{
		ID:       "20261123T090000Z-0003",
		GoalName: "goal-3",
		Phase:    state.PhaseSucceeded,
	}
	hr3 := run.HeroRunFromRun(r3)
	hr3.Namespace = ns
	hr3.Spec.Repository = repoURL2
	if err := fakeClient.Create(ctx, hr3); err != nil {
		t.Fatalf("Create hr3: %v", err)
	}

	// Test GetRun for existing run
	got1, err := run.GetRun(ctx, fakeClient, ns, r1.ID)
	if err != nil {
		t.Fatalf("GetRun r1: %v", err)
	}
	if got1.ID != r1.ID || got1.GoalName != r1.GoalName || got1.Phase != r1.Phase {
		t.Errorf("GetRun r1 mismatch: got %+v, want %+v", got1, r1)
	}

	// Test ListRuns for repo1
	repoKey1 := state.RepoKey{Host: "github.com", Owner: "example", Name: "repo1"}
	runs1, err := run.ListRuns(ctx, fakeClient, ns, repoKey1)
	if err != nil {
		t.Fatalf("ListRuns repo1: %v", err)
	}
	if len(runs1) != 2 {
		t.Fatalf("ListRuns repo1 count = %d, want 2", len(runs1))
	}
	// Verify ascending order by run ID
	if runs1[0].ID != r1.ID || runs1[1].ID != r2.ID {
		t.Errorf("ListRuns ordering: got [%s, %s], want [%s, %s]", runs1[0].ID, runs1[1].ID, r1.ID, r2.ID)
	}

	// Test ListRuns for repo2
	repoKey2 := state.RepoKey{Host: "github.com", Owner: "example", Name: "repo2"}
	runs2, err := run.ListRuns(ctx, fakeClient, ns, repoKey2)
	if err != nil {
		t.Fatalf("ListRuns repo2: %v", err)
	}
	if len(runs2) != 1 || runs2[0].ID != r3.ID {
		t.Errorf("ListRuns repo2 mismatch: got %+v", runs2)
	}

	// Test ListRuns all repos (zero RepoKey)
	allRuns, err := run.ListRuns(ctx, fakeClient, ns, state.RepoKey{})
	if err != nil {
		t.Fatalf("ListRuns all: %v", err)
	}
	if len(allRuns) != 3 {
		t.Fatalf("ListRuns all count = %d, want 3", len(allRuns))
	}
	if allRuns[0].ID != r3.ID || allRuns[1].ID != r1.ID || allRuns[2].ID != r2.ID {
		t.Errorf("ListRuns all ordering: got [%s, %s, %s], want [%s, %s, %s]", allRuns[0].ID, allRuns[1].ID, allRuns[2].ID, r3.ID, r1.ID, r2.ID)
	}
}
