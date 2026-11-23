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
	"os"
	"path/filepath"
	"testing"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestOrchestrator_DeletingRunJSONDoesNotAffectRun(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	store := state.NewFileStore(tempDir)

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

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "ok\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	fixedTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		Clock:      func() time.Time { return fixedTime },
		KubeClient: fakeClient,
	})

	scheduler, err := NewScheduler(SchedulerOptions{
		Store:        store,
		Orchestrator: orch,
		Clock:        func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name: "optimize-latency",
		},
		BaseCommit: "commit-base123",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	run, err := scheduler.StartRun(ctx, repo, "optimize-latency", StartOptions{Reason: "test"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	runID := run.ID
	runJSONPath := filepath.Join(tempDir, "repos", repo.Host, repo.Owner, repo.Name, "runs", runID, "run.json")
	if _, err := os.Stat(runJSONPath); err != nil {
		t.Fatalf("expected run.json to exist after StartRun, got: %v", err)
	}

	// 1. Delete run.json before the first reconcile (Pending -> Preparing)
	if err := os.Remove(runJSONPath); err != nil {
		t.Fatalf("os.Remove(run.json): %v", err)
	}
	if _, err := os.Stat(runJSONPath); !os.IsNotExist(err) {
		t.Fatalf("expected run.json to be deleted, got err: %v", err)
	}

	// Reconcile Pending run when run.json is missing on disk
	req := runRequest(orch, repo.Key(), runID)
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("orch.Reconcile Pending with deleted run.json failed: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// Verify HeroRun state in kube-apiserver advanced to Preparing
	hrKey := client.ObjectKey{Namespace: harness.Namespace(), Name: sandboxNameForRun(runID)}
	var hr herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, hrKey, &hr); err != nil {
		t.Fatalf("getting HeroRun from fakeClient: %v", err)
	}
	if hr.Status.Phase != string(state.PhasePreparing) {
		t.Errorf("expected HeroRun phase %q, got %q", state.PhasePreparing, hr.Status.Phase)
	}

	// 2. Delete run.json again before Preparing -> Working transition
	if err := os.Remove(runJSONPath); err != nil {
		t.Fatalf("os.Remove(run.json): %v", err)
	}
	if _, err := os.Stat(runJSONPath); !os.IsNotExist(err) {
		t.Fatalf("expected run.json to be deleted, got err: %v", err)
	}

	// Mark sandbox Ready so Preparing can transition to Working
	sb, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sandboxNameForRun(runID), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "PodReady",
			LastTransitionTime: metav1.NewTime(fixedTime),
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).UpdateStatus(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox status: %v", err)
	}

	// Reconcile Preparing -> Working when run.json is missing on disk
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile Preparing with deleted run.json failed: %v", err)
	}

	// Verify HeroRun state in kube-apiserver advanced to Working
	if err := fakeClient.Get(ctx, hrKey, &hr); err != nil {
		t.Fatalf("getting HeroRun from fakeClient: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseWorking) {
		t.Errorf("expected HeroRun phase %q, got %q", state.PhaseWorking, hr.Status.Phase)
	}

	// 3. Delete run.json again in the middle of Working phase
	if err := os.Remove(runJSONPath); err != nil {
		t.Fatalf("os.Remove(run.json): %v", err)
	}
	if _, err := os.Stat(runJSONPath); !os.IsNotExist(err) {
		t.Fatalf("expected run.json to be deleted, got err: %v", err)
	}

	// Complete the run to terminal phase
	workingRun, err := HeroRunToRun(&hr)
	if err != nil {
		t.Fatalf("HeroRunToRun: %v", err)
	}
	if err := orch.finishRun(ctx, repo, workingRun, &hr, runEnd{
		Phase:   state.PhaseNoImprovement,
		Kind:    "finished",
		Message: "no improvement found",
	}); err != nil {
		t.Fatalf("finishRun: %v", err)
	}

	// Reconcile terminal run with run.json deleted
	if err := os.Remove(runJSONPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("os.Remove(run.json): %v", err)
	}
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile terminal run with deleted run.json failed: %v", err)
	}

	// Verify final HeroRun state in kube-apiserver
	if err := fakeClient.Get(ctx, hrKey, &hr); err != nil {
		t.Fatalf("getting HeroRun from fakeClient: %v", err)
	}
	if hr.Status.Phase != string(state.PhaseNoImprovement) {
		t.Errorf("expected HeroRun final phase %q, got %q", state.PhaseNoImprovement, hr.Status.Phase)
	}
}

func TestScheduler_DeletingRunJSONDoesNotAffectScheduling(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	store := state.NewFileStore(tempDir)

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

	harness, _, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	fixedTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		Clock:      func() time.Time { return fixedTime },
		KubeClient: fakeClient,
	})

	scheduler, err := NewScheduler(SchedulerOptions{
		Store:        store,
		Orchestrator: orch,
		Clock:        func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "optimize-latency",
			Body:     "Optimize latency",
			Hash:     "hash-1",
			Schedule: "on-change",
		},
		Hash:       "hash-1",
		BaseCommit: "commit-base1",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	// Tick creates first run
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	hrList, err := scheduler.listRuns(ctx, repo)
	if err != nil || len(hrList) != 1 {
		t.Fatalf("expected 1 run scheduled, got %d (err: %v)", len(hrList), err)
	}

	runID := hrList[0].ID
	runJSONPath := filepath.Join(tempDir, "repos", repo.Host, repo.Owner, repo.Name, "runs", runID, "run.json")

	// Delete run.json before the scheduler pass
	if err := os.Remove(runJSONPath); err != nil {
		t.Fatalf("os.Remove(run.json): %v", err)
	}
	if _, err := os.Stat(runJSONPath); !os.IsNotExist(err) {
		t.Fatalf("expected run.json to be deleted")
	}

	// Update goal with changed hash; since run 1 is active in HeroRun, scheduler must not start another run
	goalRec.Goal.Hash = "hash-2"
	goalRec.Hash = "hash-2"
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal hash-2: %v", err)
	}

	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick with deleted run.json: %v", err)
	}
	runsAfter, err := scheduler.listRuns(ctx, repo)
	if err != nil || len(runsAfter) != 1 {
		t.Fatalf("expected scheduler to block new run based on HeroRun despite missing run.json, got %d runs", len(runsAfter))
	}

	// Transition run 1 to terminal phase in HeroRun
	hrList[0].SetPhase(state.PhaseSucceeded, fixedTime)
	var termHR herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: sandboxNameForRun(runID)}, &termHR); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if err := orch.saveHeroRun(ctx, &termHR, hrList[0]); err != nil {
		t.Fatalf("saveHeroRun: %v", err)
	}

	// Delete run.json again before next tick
	if err := os.Remove(runJSONPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("os.Remove(run.json): %v", err)
	}

	// Tick after run 1 is terminal; scheduler should schedule run 2
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick after terminal: %v", err)
	}
	runsFinal, err := scheduler.listRuns(ctx, repo)
	if err != nil || len(runsFinal) != 2 {
		t.Fatalf("expected 2 runs after terminal run, got %d", len(runsFinal))
	}
}

func TestOrchestrator_Resume_DeletingRunJSONDoesNotAffectResume(t *testing.T) {
	ctx := t.Context()
	tempDir := t.TempDir()
	store := state.NewFileStore(tempDir)

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

	harness, _, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261030T120000Z-active"
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxNameForRun(runID),
			Namespace: harness.Namespace(),
			Labels: map[string]string{
				LabelRun: runID,
			},
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "commit-base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhaseWorking),
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).
		WithObjects(hr).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	// Verify run.json does NOT exist on disk
	runJSONPath := filepath.Join(tempDir, "repos", repo.Host, repo.Owner, repo.Name, "runs", runID, "run.json")
	if _, err := os.Stat(runJSONPath); !os.IsNotExist(err) {
		t.Fatalf("expected run.json to not exist on disk")
	}

	// Resume without run.json on disk
	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// Verify run was enqueued and index was recorded from HeroRun
	orch.runIndexMu.RLock()
	entry, found := orch.runIndex[sandboxNameForRun(runID)]
	orch.runIndexMu.RUnlock()
	if !found {
		t.Fatalf("expected run %s to be indexed during resume despite missing run.json", runID)
	}
	if entry.runID != runID || entry.repo != repo.Key() {
		t.Errorf("unexpected runIndex entry: %+v", entry)
	}
}

func TestOrchestrator_HeroRunWithEmptyPhaseReconcilesAsPending(t *testing.T) {
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

	harness, agentsCS, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261030T120000Z-emptyphase"
	hrName := sandboxNameForRun(runID)

	// Simulate crash between Create and initial Status.Update: Phase is empty ("")
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       types.UID("test-uid-" + runID),
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "commit-base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: "", // empty phase
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).
		WithObjects(hr).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      hrName,
		},
	}

	// Reconcile must treat empty phase as Pending, prepare the run, and advance to Preparing
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("orch.Reconcile with empty phase failed: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	var updatedHR herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &updatedHR); err != nil {
		t.Fatalf("getting updated HeroRun: %v", err)
	}
	if updatedHR.Status.Phase != string(state.PhasePreparing) {
		t.Errorf("expected HeroRun phase %q, got %q", state.PhasePreparing, updatedHR.Status.Phase)
	}

	// Verify Sandbox was created
	sb, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, hrName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if len(sb.OwnerReferences) != 1 {
		t.Errorf("expected 1 owner reference on sandbox, got %d", len(sb.OwnerReferences))
	}
}

func TestOrchestrator_DeletedHeroRunEndsRun(t *testing.T) {
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

	runID := "20261030T120000Z-deleted"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhaseWorking,
		BaseCommit: "commit-base123",
	}
	// Run exists on disk in run.json
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	// fakeClient does NOT have the HeroRun (it was deleted or never created)
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      sandboxNameForRun(runID),
		},
	}
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("orch.Reconcile failed unexpectedly: %v", err)
	}
	if (res != reconcile.Result{}) {
		t.Errorf("expected empty reconcile.Result for deleted run, got: %+v", res)
	}

	// Verify HeroRun was NOT recreated in kube-apiserver from run.json
	var hr herorunv1alpha1.HeroRun
	hrKey := client.ObjectKey{Namespace: harness.Namespace(), Name: sandboxNameForRun(runID)}
	if err := fakeClient.Get(ctx, hrKey, &hr); err == nil {
		t.Fatalf("expected HeroRun to NOT be recreated after being deleted, but found: %+v", hr)
	}
}

func TestOrchestrator_ConcurrentHeroRunStatusUpdateCausesConflict(t *testing.T) {
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

	harness, _, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261030T120000Z-conflict"
	hrName := sandboxNameForRun(runID)

	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       types.UID("test-uid-" + runID),
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "commit-base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePreparing),
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).
		WithObjects(hr).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	// 1. Reconciler loads hr
	var loadedHR herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &loadedHR); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}

	// 2. A concurrent process updates hr in the apiserver
	var concurrentHR herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &concurrentHR); err != nil {
		t.Fatalf("getting concurrent HeroRun: %v", err)
	}
	concurrentHR.Status.Summary = "concurrent update"
	if err := fakeClient.Status().Update(ctx, &concurrentHR); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	// 3. Reconciler attempts to save using loadedHR; must fail with conflict because loadedHR has old ResourceVersion
	run, err := HeroRunToRun(&loadedHR)
	if err != nil {
		t.Fatalf("HeroRunToRun: %v", err)
	}
	run.SetPhase(state.PhaseWorking, time.Now().UTC())

	saveErr := orch.saveHeroRun(ctx, &loadedHR, run)
	if saveErr == nil {
		t.Fatal("expected saveHeroRun to fail with conflict error on stale HeroRun, got nil")
	}
	if !k8serrors.IsConflict(saveErr) {
		t.Errorf("expected conflict error, got: %v", saveErr)
	}
}

func TestOrchestrator_PrepareRun_SaveConflictRetriesWithSameSandbox(t *testing.T) {
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

	harness, agentsCS, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261030T120000Z-retrysb"
	hrName := sandboxNameForRun(runID)

	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       types.UID("test-uid-" + runID),
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "commit-base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	failNextSave := true
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}).
		WithObjects(hr).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if failNextSave {
					failNextSave = false
					return k8serrors.NewConflict(schema.GroupResource{Group: herorunv1alpha1.GroupVersion.Group, Resource: "heroruns"}, hrName, errors.New("simulated status conflict"))
				}
				return client.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      hrName,
		},
	}

	// 1. First reconcile pass: creates Sandbox, but save status fails with conflict
	_, err = orch.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected first reconcile pass to fail when save status conflicts, got nil")
	}

	// Verify Sandbox was created during the first attempt
	sb1, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, hrName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox after first attempt: %v", err)
	}

	// 2. Second reconcile pass (retry): must reuse existing Sandbox and advance to Preparing
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("expected retry reconcile pass to succeed with reused sandbox, got: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// Verify HeroRun status updated to Preparing
	var updatedHR herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &updatedHR); err != nil {
		t.Fatalf("getting updated HeroRun: %v", err)
	}
	if updatedHR.Status.Phase != string(state.PhasePreparing) {
		t.Errorf("expected HeroRun phase %q, got %q", state.PhasePreparing, updatedHR.Status.Phase)
	}

	// Verify the same sandbox was reused
	sb2, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, hrName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox after retry: %v", err)
	}
	if sb1.UID != sb2.UID {
		t.Errorf("expected same sandbox to be reused, got UID %s vs %s", sb1.UID, sb2.UID)
	}
}
