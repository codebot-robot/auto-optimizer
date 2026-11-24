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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
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
