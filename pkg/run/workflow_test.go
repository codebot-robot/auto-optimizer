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
	"strings"
	"testing"
	"time"

	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestRunWorkflow_Pending_Success(t *testing.T) {
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

	runID := "20261123T120000Z-wf01"
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
			BaseCommit: "base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hr).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	run := &state.Run{
		ID:    runID,
		Repo:  repo.Key(),
		Phase: state.PhasePending,
	}

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run failed: %v", err)
	}
	if res.done {
		t.Errorf("expected res.done to be false")
	}
	if res.requeueAfter <= 0 {
		t.Errorf("expected res.requeueAfter > 0, got %v", res.requeueAfter)
	}

	if wf.run.Phase != state.PhasePreparing {
		t.Errorf("expected run phase %s, got %s", state.PhasePreparing, wf.run.Phase)
	}
	if wf.hr.Status.Phase != string(state.PhasePending) {
		t.Errorf("expected HeroRun status to remain unchanged until caller saves, got %s", wf.hr.Status.Phase)
	}
	if wf.run.SandboxName != hrName {
		t.Errorf("expected SandboxName %s, got %s", hrName, wf.run.SandboxName)
	}
	if wf.run.ReadyDeadline == nil {
		t.Fatalf("expected ReadyDeadline to be set")
	}

	// Verify Sandbox was created with owner reference to HeroRun
	sb, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, hrName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox %s: %v", hrName, err)
	}
	if len(sb.OwnerReferences) != 1 || sb.OwnerReferences[0].UID != hr.UID {
		t.Errorf("unexpected sandbox owner references: %+v", sb.OwnerReferences)
	}
}

func TestRunWorkflow_Pending_WithWorkspaceClaim(t *testing.T) {
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

	runID := "20261123T120000Z-wfclaim"
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
			BaseCommit: "base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hr).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:                 store,
		Harness:               harness,
		KubeClient:            fakeClient,
		WorkspaceStorageClass: "standard",
		WorkspaceSize:         "15Gi",
	})

	run := &state.Run{
		ID:    runID,
		Repo:  repo.Key(),
		Phase: state.PhasePending,
	}

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run failed: %v", err)
	}
	if res.done {
		t.Errorf("expected res.done to be false")
	}
	if res.requeueAfter <= 0 {
		t.Errorf("expected res.requeueAfter > 0, got %v", res.requeueAfter)
	}
	if wf.run.WorkspaceClaimName != hrName {
		t.Errorf("expected WorkspaceClaimName %s, got %s", hrName, wf.run.WorkspaceClaimName)
	}

	var claim corev1.PersistentVolumeClaim
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &claim); err != nil {
		t.Fatalf("getting PVC %s: %v", hrName, err)
	}
	if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != "standard" {
		t.Errorf("unexpected StorageClassName: %v", claim.Spec.StorageClassName)
	}
	if len(claim.OwnerReferences) != 1 || claim.OwnerReferences[0].UID != hr.UID {
		t.Errorf("unexpected PVC owner references: %+v", claim.OwnerReferences)
	}
}

func TestRunWorkflow_Pending_ReuseExistingSandbox(t *testing.T) {
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

	runID := "20261123T120000Z-wfreuse"
	hrName := sandboxNameForRun(runID)
	hrUID := types.UID("test-uid-" + runID)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       hrUID,
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	isController := true
	existingSB := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       types.UID("existing-sandbox-uid"),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: herorunv1alpha1.GroupVersion.String(),
					Kind:       "HeroRun",
					Name:       hrName,
					UID:        hrUID,
					Controller: &isController,
				},
			},
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, existingSB, metav1.CreateOptions{}); err != nil {
		t.Fatalf("pre-creating sandbox: %v", err)
	}

	fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hr).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	run := &state.Run{
		ID:    runID,
		Repo:  repo.Key(),
		Phase: state.PhasePending,
	}

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run failed: %v", err)
	}
	if res.done {
		t.Errorf("expected res.done to be false")
	}
	if res.requeueAfter <= 0 {
		t.Errorf("expected res.requeueAfter > 0, got %v", res.requeueAfter)
	}
	if wf.run.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, wf.run.Phase)
	}

	sb, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, hrName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if sb.UID != existingSB.UID {
		t.Errorf("expected existing sandbox UID %s, got %s", existingSB.UID, sb.UID)
	}
}

func TestRunWorkflow_Pending_SandboxAlreadyExists_DifferentOwnerFails(t *testing.T) {
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

	runID := "20261123T120000Z-wfdiffowner"
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
			BaseCommit: "base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	isController := true
	existingSB := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       types.UID("existing-sandbox-uid"),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: herorunv1alpha1.GroupVersion.String(),
					Kind:       "HeroRun",
					Name:       hrName,
					UID:        "different-owner-uid",
					Controller: &isController,
				},
			},
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, existingSB, metav1.CreateOptions{}); err != nil {
		t.Fatalf("pre-creating sandbox: %v", err)
	}

	fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hr).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	run := &state.Run{
		ID:    runID,
		Repo:  repo.Key(),
		Phase: state.PhasePending,
	}

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !res.done {
		t.Errorf("expected res.done to be true on terminal failure")
	}
	if res.requeueAfter != 0 {
		t.Errorf("expected zero requeueAfter on terminal failure, got %v", res.requeueAfter)
	}
	if wf.run.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, wf.run.Phase)
	}
	if wf.run.Error == "" {
		t.Errorf("expected error message on run")
	}
}

func TestReconcileWorkflow_CanceledContextDoesNotPersistFailure(t *testing.T) {
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

	runID := "20261123T120000Z-wfcancel"
	hrName := sandboxNameForRun(runID)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			UID:       "", // triggers workspace claim owner error
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hr).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:                 store,
		Harness:               harness,
		KubeClient:            fakeClient,
		WorkspaceStorageClass: "standard",
	})

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	run := &state.Run{
		ID:    runID,
		Repo:  repo.Key(),
		Phase: state.PhasePending,
	}
	res, err := orch.reconcileWorkflow(cancelCtx, newRunWorkflow(orch, repo, run, hr))
	if err != nil {
		t.Fatalf("expected nil error on canceled context, got %v", err)
	}
	if (res != reconcile.Result{}) {
		t.Errorf("expected empty reconcile.Result, got %+v", res)
	}

	var gotHR herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &gotHR); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if gotHR.Status.Phase != string(state.PhasePending) {
		t.Errorf("expected HeroRun phase to remain %s on canceled context, got %s", state.PhasePending, gotHR.Status.Phase)
	}
}

func TestRunWorkflow_UnsupportedPhase(t *testing.T) {
	ctx := t.Context()
	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}

	runID := "20261123T120000Z-wfunsupported"
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: sandboxNameForRun(runID),
		},
	}
	run := &state.Run{
		ID:    runID,
		Repo:  repo.Key(),
		Phase: state.PhaseSucceeded,
	}

	wf := newRunWorkflow(nil, repo, run, hr)
	_, err := wf.Run(ctx)
	if err == nil {
		t.Fatalf("expected error for unsupported phase, got nil")
	}
}

func TestRunWorkflow_Preparing_NotReadyRequeues(t *testing.T) {
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

	runID := "20261124T120000Z-wfprepwait"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(10 * time.Second)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhasePreparing)},
	}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: newTestFakeClientBuilder(scheme).WithObjects(hr).Build(),
		Clock:      func() time.Time { return now },
	})

	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		Phase:         state.PhasePreparing,
		SandboxName:   sbName,
		ReadyDeadline: &deadline,
	}
	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if res.done {
		t.Errorf("expected waiting result, got %+v", res)
	}
	if res.requeueAfter != 10*time.Second {
		t.Errorf("requeueAfter = %v, want 10s", res.requeueAfter)
	}
	if wf.run.Phase != state.PhasePreparing {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhasePreparing)
	}
	if wf.hr.Status.Phase != string(state.PhasePreparing) {
		t.Errorf("expected HeroRun status unchanged until caller saves, got %s", wf.hr.Status.Phase)
	}
}

func TestRunWorkflow_Preparing_ReadyDeadlinePassedFails(t *testing.T) {
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

	runID := "20261124T120000Z-wfprepexp"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 5, 0, 0, time.UTC)
	deadline := now.Add(-time.Second)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhasePreparing)},
	}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: newTestFakeClientBuilder(scheme).WithObjects(hr).Build(),
		Clock:      func() time.Time { return now },
	})

	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		Phase:         state.PhasePreparing,
		SandboxName:   sbName,
		ReadyDeadline: &deadline,
	}
	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if !res.done {
		t.Fatalf("expected res.done to be true")
	}
	if wf.run.Phase != state.PhaseFailed {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseFailed)
	}
	if wf.run.ErrorKind != "failed" {
		t.Errorf("errorKind = %q, want %q", wf.run.ErrorKind, "failed")
	}
	if wf.run.ReadyDeadline != nil {
		t.Errorf("expected ReadyDeadline to be cleared, got %v", wf.run.ReadyDeadline)
	}
	if !strings.Contains(wf.run.Error, "sandbox not ready before deadline") {
		t.Errorf("unexpected error: %q", wf.run.Error)
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected sandbox to remain until caller saves terminal state, got err: %v", err)
	}
}

func TestRunWorkflow_Preparing_ReadyAdvancesToWorking(t *testing.T) {
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

	procSrv := &mockProcessServer{}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261124T120000Z-wfprepready"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}
	if _, err := coreCS.CoreV1().Pods(harness.Namespace()).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbName,
			Namespace: harness.Namespace(),
			UID:       types.UID("pod-uid-wfprep"),
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating pod: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Minute)
	binder := &fakePortalBinder{}
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhasePreparing)},
	}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		KubeClient:    newTestFakeClientBuilder(scheme).WithObjects(hr).Build(),
		Clock:         func() time.Time { return now },
		GitAuthor:     "auto-optimizer <ao@example.com>",
		PortalBinder:  binder,
		PolicyFactory: portal.HostListPolicyFactory([]string{"github.com"}),
	})

	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		BaseCommit:    "abc123",
		Phase:         state.PhasePreparing,
		SandboxName:   sbName,
		ReadyDeadline: &deadline,
	}
	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if res.done || res.requeueAfter != 0 {
		t.Errorf("expected immediate requeue result, got %+v", res)
	}
	if wf.run.Phase != state.PhaseWorking {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseWorking)
	}
	if wf.run.ReadyDeadline != nil {
		t.Errorf("expected ReadyDeadline to be cleared, got %v", wf.run.ReadyDeadline)
	}
	if wf.hr.Status.Phase != string(state.PhasePreparing) {
		t.Errorf("expected HeroRun status unchanged until caller saves, got %s", wf.hr.Status.Phase)
	}
}

func TestRunWorkflow_Preparing_PortalBindRetryAndDeadline(t *testing.T) {
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

	harness, agentsCS, coreCS := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261124T120000Z-wfportal"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			}},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}
	if _, err := coreCS.CoreV1().Pods(harness.Namespace()).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbName,
			Namespace: harness.Namespace(),
			UID:       types.UID("pod-uid-wfportal"),
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating pod: %v", err)
	}

	currentTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	deadline := currentTime.Add(30 * time.Second)
	binder := &fakePortalBinder{
		bindFunc: func(context.Context, types.UID, *portalsv1.EgressPolicy) error {
			return errors.New("portal lagging")
		},
	}
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhasePreparing)},
	}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		KubeClient:    newTestFakeClientBuilder(scheme).WithObjects(hr).Build(),
		Clock:         func() time.Time { return currentTime },
		PortalBinder:  binder,
		PolicyFactory: portal.HostListPolicyFactory(nil),
	})

	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		Phase:         state.PhasePreparing,
		SandboxName:   sbName,
		ReadyDeadline: &deadline,
	}

	// Before deadline: requeues at portalBindRetryInterval.
	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run before deadline: %v", err)
	}
	if res.done || res.requeueAfter != portalBindRetryInterval {
		t.Errorf("expected retry after %v, got %+v", portalBindRetryInterval, res)
	}
	if wf.run.Phase != state.PhasePreparing {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhasePreparing)
	}

	// After deadline: fails with ErrorKind portal_bind_failed and leaves deletion to caller.
	currentTime = deadline.Add(time.Second)
	res, err = wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run after deadline: %v", err)
	}
	if !res.done {
		t.Fatalf("expected res.done to be true")
	}
	if wf.run.Phase != state.PhaseFailed {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseFailed)
	}
	if wf.run.ErrorKind != "portal_bind_failed" {
		t.Errorf("errorKind = %q, want %q", wf.run.ErrorKind, "portal_bind_failed")
	}
	if wf.run.ReadyDeadline != nil {
		t.Errorf("expected ReadyDeadline to be cleared, got %v", wf.run.ReadyDeadline)
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected sandbox to remain until caller saves terminal state, got err: %v", err)
	}
}

func TestReconcileWorkflow_TerminalFailure_SavesBeforeDeletingSandbox(t *testing.T) {
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

	runID := "20261124T120000Z-wfsaveorder"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 5, 0, 0, time.UTC)
	deadline := now.Add(-time.Second)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Spec:       herorunv1alpha1.HeroRunSpec{RunID: runID, Repository: repo.URL},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhasePreparing)},
	}

	// Build a fake client without the HeroRun so Status().Update fails.
	failingClient := newTestFakeClientBuilder(scheme).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: failingClient,
		Clock:      func() time.Time { return now },
	})

	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		Phase:         state.PhasePreparing,
		SandboxName:   sbName,
		ReadyDeadline: &deadline,
	}
	if _, err := orch.reconcileWorkflow(ctx, newRunWorkflow(orch, repo, run, hr)); err == nil {
		t.Fatalf("expected error when saving terminal HeroRun fails")
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); err != nil {
		t.Fatalf("sandbox must not be deleted when saving terminal state fails: %v", err)
	}

	// Once the HeroRun exists and saving succeeds, the sandbox is deleted.
	if err := failingClient.Create(ctx, hr); err != nil {
		t.Fatalf("Create HeroRun: %v", err)
	}
	runRetry := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		Phase:         state.PhasePreparing,
		SandboxName:   sbName,
		ReadyDeadline: &deadline,
	}
	if _, err := orch.reconcileWorkflow(ctx, newRunWorkflow(orch, repo, runRetry, hr)); err != nil {
		t.Fatalf("reconcileWorkflow retry: %v", err)
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted after terminal state saved, got err: %v", err)
	}
}
