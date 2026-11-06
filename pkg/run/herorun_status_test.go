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
	"sync/atomic"
	"testing"
	"time"

	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestHeroRunStatusPublisher_CoalescesPhasesWithinInterval(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-coalesce"
	prepTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "optimize-search",
		Phase:       state.PhasePending,
		BaseCommit:  "commit-base123",
		SandboxName: sandboxNameForRun(runID),
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	var statusWrites atomic.Int32
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" {
					statusWrites.Add(1)
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:                 store,
		Harness:               harness,
		KubeClient:            fakeClient,
		HeroRunStatusInterval: 3 * time.Second,
		Clock:                 func() time.Time { return prepTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Move run through Preparing and then Working before any status flush tick occurs.
	run.SetPhase(state.PhasePreparing, prepTime)
	if err := orch.putRun(ctx, repo, run); err != nil {
		t.Fatalf("putRun Preparing: %v", err)
	}

	workTime := prepTime.Add(time.Second)
	run.SetPhase(state.PhaseWorking, workTime)
	if err := orch.putRun(ctx, repo, run); err != nil {
		t.Fatalf("putRun Working: %v", err)
	}

	// Trigger flush (end of interval)
	orch.statusPublisher.flush(ctx)

	// Exactly one status write should have occurred carrying the last phase ("Working")
	if got := statusWrites.Load(); got != 1 {
		t.Errorf("expected exactly 1 status write, got %d", got)
	}

	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseWorking) {
		t.Errorf("expected status.phase %q, got %q", state.PhaseWorking, hr.Status.Phase)
	}
	if hr.Status.Branch != "" {
		t.Errorf("expected branch to be empty before push, got %q", hr.Status.Branch)
	}
	if hr.Status.StartedAt == nil || !hr.Status.StartedAt.Equal(&metav1.Time{Time: prepTime}) {
		t.Errorf("expected startedAt %v, got %v", prepTime, hr.Status.StartedAt)
	}
	if hr.Status.FinishedAt != nil {
		t.Errorf("expected finishedAt nil for working run, got %v", hr.Status.FinishedAt)
	}
	cond := meta.FindStatusCondition(hr.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("expected Ready condition, got nil")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("expected Ready condition status True, got %s", cond.Status)
	}
	if cond.Reason != string(state.PhaseWorking) {
		t.Errorf("expected Ready condition reason Working, got %s", cond.Reason)
	}
	if cond.Message != "" {
		t.Errorf("expected Ready condition message to be empty for non-failed run, got %q", cond.Message)
	}
	if hr.Status.ObservedAt == nil {
		t.Errorf("expected observedAt to be set")
	}

	// Second flush with nothing changed must NOT produce another status write
	orch.statusPublisher.flush(ctx)
	if got := statusWrites.Load(); got != 1 {
		t.Errorf("expected status writes to remain 1 after no-op flush, got %d", got)
	}
}

func TestHeroRunStatusPublisher_BranchMatchesRunBranch(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-branch"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "optimize-search",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base123",
		SandboxName: sandboxNameForRun(runID),
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		Build()

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	orch.statusPublisher.MarkDirty(repo.Key(), runID)
	orch.statusPublisher.flush(ctx)

	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if hr.Status.Branch != "" {
		t.Errorf("expected status.branch empty before push, got %q", hr.Status.Branch)
	}

	// After push, Selecting sets run.Branch
	pushedBranch := "ao/optimize-search/run-1"
	run.Branch = pushedBranch
	if err := orch.putRun(ctx, repo, run); err != nil {
		t.Fatalf("putRun with branch: %v", err)
	}

	orch.statusPublisher.flush(ctx)

	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun after push: %v", err)
	}
	if hr.Status.Branch != pushedBranch {
		t.Errorf("expected status.branch %q, got %q", pushedBranch, hr.Status.Branch)
	}
}

func TestHeroRunStatusPublisher_TerminalRunEndsWithFinishedAtAndReadyFalse(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-terminal"
	startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	endTime := time.Date(2026, 10, 30, 12, 5, 0, 0, time.UTC)

	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "optimize-search",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit-base123",
		SandboxName: sandboxNameForRun(runID),
	}
	run.SetPhase(state.PhasePreparing, startTime)
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		Build()

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:                 store,
		Harness:               harness,
		KubeClient:            fakeClient,
		HeroRunStatusInterval: 3 * time.Second,
		Clock:                 func() time.Time { return endTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Move run to terminal phase NoImprovement
	run.SetPhase(state.PhaseNoImprovement, endTime)
	if err := orch.putRun(ctx, repo, run); err != nil {
		t.Fatalf("putRun terminal: %v", err)
	}

	orch.statusPublisher.flush(ctx)

	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseNoImprovement) {
		t.Errorf("expected status.phase %q, got %q", state.PhaseNoImprovement, hr.Status.Phase)
	}
	if hr.Status.FinishedAt == nil {
		t.Fatalf("expected finishedAt to be set for terminal run")
	}
	if !hr.Status.FinishedAt.Equal(&metav1.Time{Time: endTime}) {
		t.Errorf("expected finishedAt %v, got %v", endTime, hr.Status.FinishedAt)
	}
	cond := meta.FindStatusCondition(hr.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("expected Ready condition, got nil")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("expected Ready condition status False, got %s", cond.Status)
	}
	if cond.Reason != string(state.PhaseNoImprovement) {
		t.Errorf("expected Ready condition reason NoImprovement, got %s", cond.Reason)
	}
	if cond.Message != "" {
		t.Errorf("expected Ready condition message empty for NoImprovement, got %q", cond.Message)
	}

	// Test Failed phase with error text
	failTime := endTime.Add(time.Minute)
	failErrMsg := "command execution timed out in sandbox"
	run.SetPhase(state.PhaseFailed, failTime)
	run.Error = failErrMsg
	if err := orch.putRun(ctx, repo, run); err != nil {
		t.Fatalf("putRun Failed: %v", err)
	}

	orch.statusPublisher.flush(ctx)

	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun after failure: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseFailed) {
		t.Errorf("expected status.phase %q, got %q", state.PhaseFailed, hr.Status.Phase)
	}
	condFail := meta.FindStatusCondition(hr.Status.Conditions, "Ready")
	if condFail == nil {
		t.Fatalf("expected Ready condition, got nil")
	}
	if condFail.Status != metav1.ConditionFalse {
		t.Errorf("expected Ready condition status False, got %s", condFail.Status)
	}
	if condFail.Reason != string(state.PhaseFailed) {
		t.Errorf("expected Ready condition reason Failed, got %s", condFail.Reason)
	}
	if condFail.Message != failErrMsg {
		t.Errorf("expected Ready condition message %q, got %q", failErrMsg, condFail.Message)
	}
}

func TestHeroRunStatusPublisher_WriteFailureRetriedOnNextTick(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-retry"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "optimize-search",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base123",
		SandboxName: sandboxNameForRun(runID),
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	var attempts atomic.Int32
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if subResourceName == "status" {
					if attempts.Add(1) == 1 {
						return errors.New("simulated API server status update error")
					}
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Pre-publish HeroRun so status update is attempted
	orch.publishHeroRun(ctx, repo, run)

	// Mark dirty
	orch.statusPublisher.MarkDirty(repo.Key(), runID)

	// First tick / flush fails status update
	orch.statusPublisher.flush(ctx)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("expected 1 update attempt, got %d", got)
	}

	// Status should not have been updated yet
	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if hr.Status.Phase == string(state.PhaseWorking) {
		t.Fatalf("expected HeroRun status to NOT be updated on failure")
	}

	// Run should still be marked dirty
	orch.statusPublisher.mu.Lock()
	dirtyCount := len(orch.statusPublisher.dirty)
	orch.statusPublisher.mu.Unlock()
	if dirtyCount != 1 {
		t.Errorf("expected run to remain dirty after failure, got %d dirty items", dirtyCount)
	}

	// Second tick / flush succeeds
	orch.statusPublisher.flush(ctx)
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 update attempts, got %d", got)
	}

	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseWorking) {
		t.Errorf("expected status.phase %q, got %q", state.PhaseWorking, hr.Status.Phase)
	}
}

func TestHeroRunStatusPublisher_NilKubeClientPublishesNothing(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-nilclient"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhaseWorking,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: nil,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Mark dirty and run
	orch.statusPublisher.MarkDirty(repo.Key(), runID)
	if err := orch.statusPublisher.Run(ctx); err != nil {
		t.Errorf("Run with nil KubeClient returned error: %v", err)
	}
	orch.statusPublisher.flush(ctx)
}

func TestHeroRunStatusPublisher_Resume_MarksAllRunsDirty(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	// Store contains a terminal run that completed while process was offline
	runID := "20261030T120000Z-resumed-terminal"
	termTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "offline-goal",
		Phase:       state.PhaseNoImprovement,
		BaseCommit:  "base-sha",
		SandboxName: sandboxNameForRun(runID),
	}
	run.SetPhase(state.PhaseNoImprovement, termTime)
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		Build()

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Resume should mark all runs dirty, including terminal runs
	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	orch.statusPublisher.mu.Lock()
	dirtyCount := len(orch.statusPublisher.dirty)
	orch.statusPublisher.mu.Unlock()
	if dirtyCount != 1 {
		t.Fatalf("expected 1 dirty run after Resume, got %d", dirtyCount)
	}

	// Flush updates the HeroRun status in Kubernetes
	orch.statusPublisher.flush(ctx)

	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseNoImprovement) {
		t.Errorf("expected status.phase %q, got %q", state.PhaseNoImprovement, hr.Status.Phase)
	}
}

func TestHeroRunStatusPublisher_FinalFlushOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-shutdown"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "shutdown-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sandboxNameForRun(runID),
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		Build()

	harness, _, _ := setupTestHarness(t, nil)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:                 store,
		Harness:               harness,
		KubeClient:            fakeClient,
		HeroRunStatusInterval: 10 * time.Minute, // Long interval so ticker does not tick during test
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Mark run dirty
	orch.statusPublisher.MarkDirty(repo.Key(), runID)

	runDone := make(chan error, 1)
	go func() {
		runDone <- orch.statusPublisher.Run(ctx)
	}()

	// Cancel context to simulate process shutdown
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Run to exit after context cancellation")
	}

	// Verify that the final flush executed during shutdown wrote status
	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(t.Context(), client.ObjectKey{Namespace: "ao-sandboxes", Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun after shutdown flush: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseWorking) {
		t.Errorf("expected status.phase %q, got %q", state.PhaseWorking, hr.Status.Phase)
	}
}
