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
	"github.com/go-git/go-git/v5/plumbing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/session"
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

func TestRunWorkflow_Selecting_Success(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	remoteURL, bare, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}
	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	const verifyCmd = "go test ./..."
	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			"sh -c cd '/workspace/repo' && " + verifyCmd: func(stream processv1.ProcessService_StartServer) error {
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: []byte("ok\n")}})
				return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: 0}}})
			},
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261124T120000Z-wfselok"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 10, 0, 0, time.UTC)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Spec:       herorunv1alpha1.HeroRunSpec{RunID: runID, Repository: repo.URL},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhaseSelecting)},
	}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: newTestFakeClientBuilder(scheme).WithObjects(hr).Build(),
		Clock:      func() time.Time { return now },
		ForgeFor:   func(string) (forge.Forge, error) { return forge.NewGitRemote(), nil },
	})

	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Goal:        &goals.Goal{Name: "export-goal", Verify: verifyCmd},
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sbName,
	}
	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if res.done || res.requeueAfter != 0 {
		t.Errorf("expected immediate requeue result, got %+v", res)
	}
	if wf.run.Phase != state.PhaseOpeningPR {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseOpeningPR)
	}
	wantBranch := "ao/export-goal/" + runID
	if wf.run.Branch != wantBranch {
		t.Errorf("branch = %q, want %q", wf.run.Branch, wantBranch)
	}
	if wf.hr.Status.Phase != string(state.PhaseSelecting) {
		t.Errorf("expected HeroRun status unchanged until caller saves, got %s", wf.hr.Status.Phase)
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected sandbox to remain until caller saves transition, got err: %v", err)
	}

	ref, err := bare.Reference(plumbing.ReferenceName("refs/heads/"+wantBranch), true)
	if err != nil {
		t.Fatalf("bare.Reference: %v", err)
	}
	if ref.Hash().String() != tipSHA {
		t.Errorf("remote branch ref = %s, want %s", ref.Hash().String(), tipSHA)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var kinds []string
	for _, e := range events {
		kinds = append(kinds, e.Kind)
	}
	wantKinds := []string{"verify_started", "verify_passed", "commits_exported", "branch_pushed"}
	if strings.Join(kinds, ",") != strings.Join(wantKinds, ",") {
		t.Errorf("event kinds = %v, want %v", kinds, wantKinds)
	}
}

func TestRunWorkflow_Selecting_RefusalsAndDirtyTree(t *testing.T) {
	const baseSHA = "1234567890abcdef1234567890abcdef12345678"
	tests := []struct {
		name        string
		statusOut   string
		diffOut     string
		wantPhase   state.RunPhase
		wantKind    string
		wantMessage string
	}{
		{
			name:        "no changes against base",
			diffOut:     "",
			wantPhase:   state.PhaseNoImprovement,
			wantKind:    "no_changes",
			wantMessage: "no changes against base commit",
		},
		{
			name:        "edits under .goals/",
			diffOut:     ".goals/perf.md\x00main.go\x00",
			wantPhase:   state.PhaseNoImprovement,
			wantKind:    "refused",
			wantMessage: "changes under .goals/",
		},
		{
			name:        "dirty working tree",
			statusOut:   "? untracked.txt\x00",
			wantPhase:   state.PhaseFailed,
			wantKind:    "failed",
			wantMessage: session.ErrUncommittedChanges.Error(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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

			procSrv := &scriptedExportServer{
				commands: map[string]string{
					"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": tc.statusOut,
					"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       tc.diffOut,
				},
			}
			harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)
			runID := "20261124T120000Z-wfrefuse"
			sbName := sandboxNameForRun(runID)
			if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
				ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
			}, metav1.CreateOptions{}); err != nil {
				t.Fatalf("creating sandbox: %v", err)
			}

			now := time.Date(2026, 11, 24, 12, 10, 0, 0, time.UTC)
			orch := newTestOrchestrator(t, OrchestratorOptions{
				Store:   store,
				Harness: harness,
				Clock:   func() time.Time { return now },
			})
			run := &state.Run{
				ID:          runID,
				Repo:        repo.Key(),
				GoalName:    "test-goal",
				Phase:       state.PhaseSelecting,
				BaseCommit:  baseSHA,
				SandboxName: sbName,
			}
			putTestRun(t, ctx, orch, run)

			var hr herorunv1alpha1.HeroRun
			if err := orch.kubeClient.Get(ctx, client.ObjectKey{Namespace: orch.namespace(), Name: sbName}, &hr); err != nil {
				t.Fatalf("get HeroRun: %v", err)
			}

			// Direct Run check: updates run in memory and leaves sandbox intact.
			wf := newRunWorkflow(orch, repo, run, &hr)
			res, err := wf.Run(ctx)
			if err != nil {
				t.Fatalf("wf.Run: %v", err)
			}
			if !res.done {
				t.Fatalf("expected res.done to be true")
			}
			if wf.run.Phase != tc.wantPhase {
				t.Errorf("phase = %s, want %s", wf.run.Phase, tc.wantPhase)
			}
			if tc.wantPhase == state.PhaseNoImprovement {
				if wf.run.Error != "" || wf.run.ErrorKind != "" {
					t.Errorf("expected empty Error/ErrorKind for NoImprovement, got %q/%q", wf.run.Error, wf.run.ErrorKind)
				}
				if wf.end == nil || wf.end.Kind != tc.wantKind || wf.end.Message != tc.wantMessage {
					t.Errorf("wf.end = %+v, want kind=%q msg=%q", wf.end, tc.wantKind, tc.wantMessage)
				}
			} else {
				if wf.run.ErrorKind != tc.wantKind || wf.run.Error != tc.wantMessage {
					t.Errorf("ErrorKind/Error = %q/%q, want %q/%q", wf.run.ErrorKind, wf.run.Error, tc.wantKind, tc.wantMessage)
				}
			}
			if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); err != nil {
				t.Fatalf("expected sandbox intact before reconcileWorkflow saves: %v", err)
			}

			// Through Orchestrator.Reconcile: saves terminal phase, appends event, deletes sandbox.
			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
				t.Fatalf("orch.Reconcile: %v", err)
			}
			gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if gotRun.Phase != tc.wantPhase {
				t.Errorf("saved phase = %s, want %s", gotRun.Phase, tc.wantPhase)
			}
			events, err := store.ReadEvents(ctx, repo.Key(), runID)
			if err != nil {
				t.Fatalf("ReadEvents: %v", err)
			}
			found := false
			for _, e := range events {
				if e.Phase == tc.wantPhase && e.Kind == tc.wantKind && strings.Contains(e.Message, tc.wantMessage) {
					found = true
				}
			}
			if !found {
				t.Errorf("expected event phase=%s kind=%s msg containing %q, got %+v", tc.wantPhase, tc.wantKind, tc.wantMessage, events)
			}
			if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
				t.Errorf("expected sandbox deleted after reconcile, got err: %v", err)
			}
		})
	}
}

func TestRunWorkflow_Selecting_VerifyOutcomes(t *testing.T) {
	const verifyCmd = "make test"
	expectedShell := "sh -c cd '/workspace/repo' && " + verifyCmd

	tests := []struct {
		name          string
		procSrv       *scriptedExportServer
		verifyTimeout time.Duration
		wantPhase     state.RunPhase
		wantKind      string
		wantSubstr    string
		wantExitCode  int
	}{
		{
			name: "non-zero exit",
			procSrv: &scriptedExportServer{
				startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
					expectedShell: func(stream processv1.ProcessService_StartServer) error {
						_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
						_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stderr{Stderr: []byte("FAIL\n")}})
						return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: 2}}})
					},
				},
			},
			verifyTimeout: time.Minute,
			wantPhase:     state.PhaseNoImprovement,
			wantKind:      "verify_failed",
			wantSubstr:    "exit code 2",
			wantExitCode:  2,
		},
		{
			name: "timeout",
			procSrv: &scriptedExportServer{
				startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
					expectedShell: func(stream processv1.ProcessService_StartServer) error {
						<-stream.Context().Done()
						return status.Error(codes.DeadlineExceeded, "deadline exceeded")
					},
				},
			},
			verifyTimeout: 30 * time.Millisecond,
			wantPhase:     state.PhaseNoImprovement,
			wantKind:      "verify_failed",
			wantSubstr:    "timed out",
		},
		{
			name: "execution error",
			procSrv: &scriptedExportServer{
				startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
					expectedShell: func(stream processv1.ProcessService_StartServer) error {
						return status.Error(codes.Unavailable, "connection lost")
					},
				},
			},
			verifyTimeout: time.Minute,
			wantPhase:     state.PhaseFailed,
			wantKind:      "verify_error",
			wantSubstr:    "connection lost",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := state.NewFileStore(t.TempDir())
			repo := &state.Repo{
				URL:   "https://github.com/gke-labs/test-repo",
				Host:  "github.com",
				Owner: "gke-labs",
				Name:  "test-repo",
			}
			if err := store.PutRepo(ctx, repo); err != nil {
				t.Fatalf("PutRepo: %v", err)
			}
			harness, _, _ := setupTestHarnessWithLLM(t, tc.procSrv, nil)
			orch := newTestOrchestrator(t, OrchestratorOptions{
				Store:         store,
				Harness:       harness,
				VerifyTimeout: tc.verifyTimeout,
				Clock:         func() time.Time { return time.Date(2026, 11, 24, 12, 10, 0, 0, time.UTC) },
			})

			run := &state.Run{
				ID:          "20261124T120000Z-wfver",
				Repo:        repo.Key(),
				GoalName:    "test-goal",
				Goal:        &goals.Goal{Name: "test-goal", Verify: verifyCmd},
				Phase:       state.PhaseSelecting,
				SandboxName: "run-20261124t120000z-wfver",
			}
			hr := &herorunv1alpha1.HeroRun{
				ObjectMeta: metav1.ObjectMeta{Name: run.SandboxName, Namespace: harness.Namespace()},
				Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhaseSelecting)},
			}
			wf := newRunWorkflow(orch, repo, run, hr)
			res, err := wf.Run(ctx)
			if err != nil {
				t.Fatalf("wf.Run: %v", err)
			}
			if !res.done {
				t.Fatalf("expected res.done to be true")
			}
			if wf.run.Phase != tc.wantPhase {
				t.Errorf("phase = %s, want %s", wf.run.Phase, tc.wantPhase)
			}
			if tc.wantPhase == state.PhaseNoImprovement {
				if wf.end == nil || wf.end.Kind != tc.wantKind || !strings.Contains(wf.end.Message, tc.wantSubstr) {
					t.Errorf("wf.end = %+v, want kind=%q msg containing %q", wf.end, tc.wantKind, tc.wantSubstr)
				}
				if tc.wantExitCode != 0 {
					if wf.end.Data["exitCode"] != tc.wantExitCode {
						t.Errorf("exitCode = %v, want %d", wf.end.Data["exitCode"], tc.wantExitCode)
					}
				}
			} else if wf.run.ErrorKind != tc.wantKind || !strings.Contains(wf.run.Error, tc.wantSubstr) {
				t.Errorf("ErrorKind/Error = %q/%q, want kind=%q msg containing %q", wf.run.ErrorKind, wf.run.Error, tc.wantKind, tc.wantSubstr)
			}
		})
	}
}

func TestRunWorkflow_Selecting_EmptySandboxAndPushFailures(t *testing.T) {
	ctx := t.Context()

	t.Run("empty sandbox name", func(t *testing.T) {
		store := state.NewFileStore(t.TempDir())
		repo := &state.Repo{URL: "https://github.com/gke-labs/test-repo", Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
		harness, _, _ := setupTestHarness(t, nil)
		orch := newTestOrchestrator(t, OrchestratorOptions{Store: store, Harness: harness})
		run := &state.Run{ID: "20261124T120000Z-nosb", Repo: repo.Key(), Phase: state.PhaseSelecting}
		wf := newRunWorkflow(orch, repo, run, &herorunv1alpha1.HeroRun{})
		res, err := wf.Run(ctx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if !res.done || wf.run.Phase != state.PhaseFailed || wf.run.ErrorKind != "failed" {
			t.Errorf("got done=%v phase=%s errorKind=%q, want done=true Failed/failed", res.done, wf.run.Phase, wf.run.ErrorKind)
		}
	})

	t.Run("nil forge", func(t *testing.T) {
		store := state.NewFileStore(t.TempDir())
		remoteURL, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
		repoKey, err := state.ParseRepoURL(remoteURL)
		if err != nil {
			t.Fatalf("ParseRepoURL: %v", err)
		}
		repo := &state.Repo{URL: remoteURL, Host: repoKey.Host, Owner: repoKey.Owner, Name: repoKey.Name}
		if err := store.PutRepo(ctx, repo); err != nil {
			t.Fatalf("PutRepo: %v", err)
		}
		procSrv := &scriptedExportServer{
			commands: map[string]string{
				"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
				"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
				"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
				"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
			},
			pack: packBytes,
		}
		harness, _, _ := setupTestHarnessWithLLM(t, procSrv, nil)
		orch := newTestOrchestrator(t, OrchestratorOptions{Store: store, Harness: harness})
		run := &state.Run{
			ID:          "20261124T120000Z-nilforge",
			Repo:        repo.Key(),
			GoalName:    "export-goal",
			Phase:       state.PhaseSelecting,
			BaseCommit:  baseSHA,
			SandboxName: "run-20261124t120000z-nilforge",
		}
		wf := newRunWorkflow(orch, repo, run, &herorunv1alpha1.HeroRun{})
		res, err := wf.Run(ctx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if !res.done || wf.run.Phase != state.PhaseFailed || wf.run.ErrorKind != "push_failed" {
			t.Errorf("got done=%v phase=%s errorKind=%q, want done=true Failed/push_failed", res.done, wf.run.Phase, wf.run.ErrorKind)
		}
	})
}

func TestReconcileWorkflow_Selecting_SavesOpeningPRBeforeDeletingSandbox(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	remoteURL, bare, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}
	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	runID := "20261124T120000Z-wfselsave"
	sbName := sandboxNameForRun(runID)
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	now := time.Date(2026, 11, 24, 12, 10, 0, 0, time.UTC)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: harness.Namespace()},
		Spec:       herorunv1alpha1.HeroRunSpec{RunID: runID, Repository: repo.URL, Goal: "export-goal", BaseCommit: baseSHA},
		Status:     herorunv1alpha1.HeroRunStatus{Phase: string(state.PhaseSelecting), SandboxName: sbName},
	}

	// Fake client does not have HeroRun yet, so putRun fails after the branch is pushed.
	failingClient := newTestFakeClientBuilder(scheme).Build()
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: failingClient,
		Clock:      func() time.Time { return now },
		ForgeFor:   func(string) (forge.Forge, error) { return forge.NewGitRemote(), nil },
	})

	makeRun := func() *state.Run {
		return &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "export-goal",
			Phase:       state.PhaseSelecting,
			BaseCommit:  baseSHA,
			SandboxName: sbName,
		}
	}

	if _, err := orch.reconcileWorkflow(ctx, newRunWorkflow(orch, repo, makeRun(), hr)); err == nil {
		t.Fatalf("expected error when saving OpeningPR HeroRun fails")
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); err != nil {
		t.Fatalf("sandbox must not be deleted when saving OpeningPR fails: %v", err)
	}

	// Once HeroRun exists, retry pushes idempotently, saves OpeningPR, and deletes sandbox.
	if err := failingClient.Create(ctx, hr); err != nil {
		t.Fatalf("Create HeroRun: %v", err)
	}
	if _, err := orch.reconcileWorkflow(ctx, newRunWorkflow(orch, repo, makeRun(), hr)); err != nil {
		t.Fatalf("reconcileWorkflow retry: %v", err)
	}
	gotRun, err := GetRun(ctx, failingClient, harness.Namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseOpeningPR {
		t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseOpeningPR)
	}
	wantBranch := "ao/export-goal/" + runID
	if gotRun.Branch != wantBranch {
		t.Errorf("branch = %q, want %q", gotRun.Branch, wantBranch)
	}
	ref, err := bare.Reference(plumbing.ReferenceName("refs/heads/"+wantBranch), true)
	if err != nil {
		t.Fatalf("bare.Reference: %v", err)
	}
	if ref.Hash().String() != tipSHA {
		t.Errorf("remote branch ref = %s, want %s", ref.Hash().String(), tipSHA)
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Get(ctx, sbName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted after OpeningPR saved, got err: %v", err)
	}
}

func newOpeningPRWorkflowFixture(t *testing.T, forgeFor func(string) (forge.Forge, error)) (*Orchestrator, state.Store, *state.Repo, *state.Run, *herorunv1alpha1.HeroRun) {
	t.Helper()
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

	runID := "20261124T120000Z-wfopenpr"
	hrName := sandboxNameForRun(runID)
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-speed",
			BaseCommit: "1189a319f389802669884696409ba28db20216b0",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhaseOpeningPR),
		},
	}

	now := time.Date(2026, 11, 24, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: newTestFakeClientBuilder(scheme).WithObjects(hr).Build(),
		PublicURL:  "https://ao.example.com",
		Clock:      func() time.Time { return now },
		ForgeFor:   forgeFor,
	})

	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-speed",
		Phase:      state.PhaseOpeningPR,
		BaseCommit: "1189a319f389802669884696409ba28db20216b0",
		Branch:     "ao/optimize-speed/" + runID,
		PRTitle:    "Speed up queries",
		PRBody:     "Replaced linear scan with hash map lookup.",
	}
	return orch, store, repo, run, hr
}

func TestRunWorkflow_OpeningPR_OpensPullRequest(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, store, repo, run, hr := newOpeningPRWorkflowFixture(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})
	run.Branch = "" // verify default branch name is computed and recorded

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if !res.done || res.requeueAfter != 0 {
		t.Fatalf("expected done=true requeueAfter=0, got %+v", res)
	}

	wantBranch := "ao/optimize-speed/" + run.ID
	if wf.run.Branch != wantBranch {
		t.Errorf("branch = %q, want %q", wf.run.Branch, wantBranch)
	}
	if wf.run.Phase != state.PhaseSucceeded {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseSucceeded)
	}
	if wf.hr.Status.Phase != string(state.PhaseOpeningPR) {
		t.Errorf("expected HeroRun status unchanged until caller saves, got %s", wf.hr.Status.Phase)
	}

	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected 1 pull request, got %d", len(prs))
	}
	pr := prs[0]
	if pr.Head != wantBranch || pr.Base != "main" || pr.Title != run.PRTitle {
		t.Errorf("unexpected PR: %+v", pr)
	}
	if wf.run.PRURL != pr.URL {
		t.Errorf("PRURL = %q, want %q", wf.run.PRURL, pr.URL)
	}
	for _, substr := range []string{run.GoalName, "1189a31", wantBranch, run.ID, "https://ao.example.com/runs/" + run.ID, "What the agent says", run.PRBody} {
		if !strings.Contains(pr.Body, substr) {
			t.Errorf("expected PR body to contain %q, got:\n%s", substr, pr.Body)
		}
	}
	if wf.end == nil || wf.end.Kind != "pull_request_opened" || wf.end.Message != pr.URL {
		t.Errorf("unexpected wf.end: %+v", wf.end)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected no events persisted before reconcileWorkflow saves, got %+v", events)
	}
}

func TestRunWorkflow_OpeningPR_ReusesExistingOpenPullRequest(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, _, repo, run, hr := newOpeningPRWorkflowFixture(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})
	fakeForge.SetPullRequests(repo.URL, []forge.PullRequest{
		{
			Number: 1,
			Title:  "Closed PR",
			Head:   run.Branch,
			Base:   "main",
			URL:    repo.URL + "/pull/1",
			State:  "closed",
		},
		{
			Number: 2,
			Title:  "Existing open PR",
			Head:   run.Branch,
			Base:   "main",
			URL:    repo.URL + "/pull/2",
			State:  "open",
		},
	})

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if !res.done {
		t.Fatalf("expected res.done to be true")
	}
	if wf.run.Phase != state.PhaseSucceeded {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseSucceeded)
	}
	wantURL := repo.URL + "/pull/2"
	if wf.run.PRURL != wantURL {
		t.Errorf("PRURL = %q, want %q", wf.run.PRURL, wantURL)
	}
	if len(fakeForge.PullRequests(repo.URL)) != 2 {
		t.Errorf("expected no new pull request opened, got %d", len(fakeForge.PullRequests(repo.URL)))
	}
	if wf.end == nil || wf.end.Kind != "pull_request_reused" || wf.end.Message != wantURL {
		t.Errorf("unexpected wf.end: %+v", wf.end)
	}
}

func TestRunWorkflow_OpeningPR_ForgeWithoutChangeRequests(t *testing.T) {
	ctx := t.Context()
	orch, _, repo, run, hr := newOpeningPRWorkflowFixture(t, func(string) (forge.Forge, error) {
		return forge.NewGitRemote(), nil
	})

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if !res.done {
		t.Fatalf("expected res.done to be true")
	}
	if wf.run.Phase != state.PhaseSucceeded {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseSucceeded)
	}
	if wf.run.PRURL != "" {
		t.Errorf("PRURL = %q, want empty", wf.run.PRURL)
	}
	if wf.end == nil || wf.end.Kind != "pull_request_not_opened" || wf.end.Message != "forge cannot open pull requests" {
		t.Errorf("unexpected wf.end: %+v", wf.end)
	}
}

func TestRunWorkflow_OpeningPR_DefaultBaseAndTitle(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, _, repo, run, hr := newOpeningPRWorkflowFixture(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})
	repo.DefaultBranch = ""
	run.PRTitle = ""

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if !res.done || wf.run.Phase != state.PhaseSucceeded {
		t.Fatalf("expected Succeeded terminal result, got res=%+v phase=%s", res, wf.run.Phase)
	}
	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected 1 PR, got %d", len(prs))
	}
	if prs[0].Base != "main" {
		t.Errorf("base = %q, want main", prs[0].Base)
	}
	if prs[0].Title != "Optimize optimize-speed" {
		t.Errorf("title = %q, want %q", prs[0].Title, "Optimize optimize-speed")
	}
}

func TestRunWorkflow_OpeningPR_Failures(t *testing.T) {
	cases := []struct {
		name        string
		setupForge  func() func(string) (forge.Forge, error)
		wantErrPart string
	}{
		{
			name:        "nil forgeFor",
			setupForge:  func() func(string) (forge.Forge, error) { return nil },
			wantErrPart: "no forge configured for repository",
		},
		{
			name: "forgeFor returns nil",
			setupForge: func() func(string) (forge.Forge, error) {
				return func(string) (forge.Forge, error) { return nil, nil }
			},
			wantErrPart: "no forge configured for repository",
		},
		{
			name: "forgeFor returns error",
			setupForge: func() func(string) (forge.Forge, error) {
				return func(string) (forge.Forge, error) { return nil, errors.New("bad forge config") }
			},
			wantErrPart: "selecting forge for",
		},
		{
			name: "list pull requests fails",
			setupForge: func() func(string) (forge.Forge, error) {
				f := forge.NewFake()
				f.SetListPullRequestsError(errors.New("list timeout"))
				return func(string) (forge.Forge, error) { return f, nil }
			},
			wantErrPart: "listing pull requests: list timeout",
		},
		{
			name: "open pull request fails",
			setupForge: func() func(string) (forge.Forge, error) {
				f := forge.NewFake()
				f.SetOpenPullRequestError(errors.New("API rate limit exceeded"))
				return func(string) (forge.Forge, error) { return f, nil }
			},
			wantErrPart: "API rate limit exceeded",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			orch, _, repo, run, hr := newOpeningPRWorkflowFixture(t, tc.setupForge())

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
			if wf.run.ErrorKind != "pull_request_failed" {
				t.Errorf("errorKind = %q, want %q", wf.run.ErrorKind, "pull_request_failed")
			}
			if !strings.Contains(wf.run.Error, tc.wantErrPart) {
				t.Errorf("error = %q, want substring %q", wf.run.Error, tc.wantErrPart)
			}
			if wf.hr.Status.Phase != string(state.PhaseOpeningPR) {
				t.Errorf("expected HeroRun status unchanged until caller saves, got %s", wf.hr.Status.Phase)
			}
		})
	}
}

func TestRunWorkflow_OpeningPR_CanceledContext(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, _, repo, run, hr := newOpeningPRWorkflowFixture(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	wf := newRunWorkflow(orch, repo, run, hr)
	res, err := wf.Run(cancelCtx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if res != (workflowResult{}) {
		t.Errorf("expected zero workflowResult on canceled context, got %+v", res)
	}
	if wf.run.Phase != state.PhaseOpeningPR {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseOpeningPR)
	}
	if len(fakeForge.PullRequests(repo.URL)) != 0 {
		t.Errorf("expected no pull requests opened on canceled context, got %d", len(fakeForge.PullRequests(repo.URL)))
	}
}

func TestReconcileWorkflow_OpeningPR_SavesBeforeRecordingEvent(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, store, repo, run, hr := newOpeningPRWorkflowFixture(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	// Build a fake client without the HeroRun so Status().Update fails.
	failingClient := newTestFakeClientBuilder(scheme).Build()
	orch.kubeClient = failingClient

	if _, err := orch.reconcileWorkflow(ctx, newRunWorkflow(orch, repo, run, hr)); err == nil {
		t.Fatalf("expected error when saving terminal HeroRun fails")
	}
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no events recorded when saving HeroRun fails, got %+v", events)
	}

	// Once the HeroRun exists and saving succeeds, retry reuses the open PR and saves/records.
	hrRetry := hr.DeepCopy()
	hrRetry.ResourceVersion = ""
	hrRetry.Status = herorunv1alpha1.HeroRunStatus{Phase: string(state.PhaseOpeningPR)}
	if err := failingClient.Create(ctx, hrRetry); err != nil {
		t.Fatalf("Create HeroRun: %v", err)
	}
	runRetry := &state.Run{
		ID:         run.ID,
		Repo:       repo.Key(),
		GoalName:   run.GoalName,
		Phase:      state.PhaseOpeningPR,
		BaseCommit: run.BaseCommit,
		Branch:     run.Branch,
		PRTitle:    run.PRTitle,
		PRBody:     run.PRBody,
	}
	if _, err := orch.reconcileWorkflow(ctx, newRunWorkflow(orch, repo, runRetry, hrRetry)); err != nil {
		t.Fatalf("reconcileWorkflow retry: %v", err)
	}

	gotRun, err := GetRun(ctx, failingClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected 1 pull request, got %d", len(prs))
	}
	if gotRun.Phase != state.PhaseSucceeded || gotRun.PRURL != prs[0].URL {
		t.Errorf("got phase=%s PRURL=%q, want Succeeded/%q", gotRun.Phase, gotRun.PRURL, prs[0].URL)
	}
	events, err = store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 || events[0].Kind != "pull_request_reused" || events[0].Message != prs[0].URL {
		t.Errorf("expected pull_request_reused event for %s, got %+v", prs[0].URL, events)
	}
}
