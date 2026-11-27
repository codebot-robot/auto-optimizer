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
	wantBranch := "auto-optimizer/export-goal/" + runID
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
		if e.Kind == "verify_passed" {
			m, ok := e.Data.(map[string]any)
			if !ok || m["exitCode"] != float64(0) || m["tail"] != "ok\n" {
				t.Errorf("verify_passed data = %#v, want exitCode=0 tail=%q", e.Data, "ok\n")
			}
		}
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
		wantTail      string
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
			wantPhase:     state.PhaseVerifyFailed,
			wantKind:      "verify_failed",
			wantSubstr:    "exit code 2",
			wantExitCode:  2,
			wantTail:      "FAIL\n",
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
			wantPhase:     state.PhaseVerifyFailed,
			wantKind:      "verify_failed",
			wantSubstr:    "timed out",
		},
		{
			name: "timeout with output",
			procSrv: &scriptedExportServer{
				startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
					expectedShell: func(stream processv1.ProcessService_StartServer) error {
						_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
						_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: []byte("running slow test...\n")}})
						<-stream.Context().Done()
						return status.Error(codes.DeadlineExceeded, "deadline exceeded")
					},
				},
			},
			verifyTimeout: 100 * time.Millisecond,
			wantPhase:     state.PhaseVerifyFailed,
			wantKind:      "verify_failed",
			wantSubstr:    "timed out",
			wantTail:      "running slow test...\n",
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
			if tc.wantPhase == state.PhaseVerifyFailed {
				if wf.run.Error != "" || wf.run.ErrorKind != "" {
					t.Errorf("expected empty Error/ErrorKind for VerifyFailed, got %q/%q", wf.run.Error, wf.run.ErrorKind)
				}
				if wf.end == nil || wf.end.Kind != tc.wantKind || !strings.Contains(wf.end.Message, tc.wantSubstr) {
					t.Errorf("wf.end = %+v, want kind=%q msg containing %q", wf.end, tc.wantKind, tc.wantSubstr)
				}
				if tc.wantExitCode != 0 {
					if wf.end.Data["exitCode"] != tc.wantExitCode {
						t.Errorf("exitCode = %v, want %d", wf.end.Data["exitCode"], tc.wantExitCode)
					}
				} else if _, hasExit := wf.end.Data["exitCode"]; hasExit {
					t.Errorf("unexpected exitCode in data: %#v", wf.end.Data)
				}
				if tc.wantTail != "" {
					if wf.end.Data["tail"] != tc.wantTail {
						t.Errorf("tail = %q, want %q", wf.end.Data["tail"], tc.wantTail)
					}
				} else if tc.wantExitCode == 0 && wf.end.Data != nil {
					t.Errorf("data = %#v, want nil", wf.end.Data)
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
	wantBranch := "auto-optimizer/export-goal/" + runID
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
		Branch:     "auto-optimizer/optimize-speed/" + runID,
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

	wantBranch := "auto-optimizer/optimize-speed/" + run.ID
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

type workingWorkflowFixture struct {
	orch       *Orchestrator
	store      state.Store
	kubeClient client.Client
	harness    *session.Harness
	repo       *state.Repo
	run        *state.Run
	hr         *herorunv1alpha1.HeroRun
	now        time.Time
}

func newWorkingWorkflowFixture(t *testing.T, runID string, now time.Time, deadline *time.Time, opts ...func(*OrchestratorOptions, *state.Run)) *workingWorkflowFixture {
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

	harness, agentsCS, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	hrName := sandboxNameForRun(runID)
	run := &state.Run{
		ID:                 runID,
		Repo:               repo.Key(),
		GoalName:           "optimize-latency",
		Phase:              state.PhaseWorking,
		PhaseTimestamps:    map[state.RunPhase]time.Time{state.PhasePending: now.Add(-15 * time.Minute), state.PhaseWorking: now.Add(-10 * time.Minute)},
		BaseCommit:         "abcdef1234567890",
		Branch:             "auto-optimizer/optimize-latency/" + runID,
		SandboxName:        hrName,
		WorkspaceClaimName: hrName,
		Deadline:           deadline,
		Goal: &goals.Goal{
			Name: "optimize-latency",
			Body: "Reduce p99 request latency by optimizing hot path allocations.",
		},
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes(harness.Namespace()).Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: hrName, Namespace: harness.Namespace()},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	orchOpts := OrchestratorOptions{
		Store:           store,
		Harness:         harness,
		Clock:           func() time.Time { return now },
		MaxTurns:        25,
		MaxSessionDepth: 4,
	}
	for _, fn := range opts {
		fn(&orchOpts, run)
	}

	hr := HeroRunFromRun(run)
	hr.Namespace = harness.Namespace()
	hr.UID = types.UID("hr-uid-" + runID)
	hr.Spec.Repository = repo.URL
	fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hr).Build()
	if orchOpts.KubeClient == nil {
		orchOpts.KubeClient = fakeClient
	}

	orch := newTestOrchestrator(t, orchOpts)
	return &workingWorkflowFixture{
		orch:       orch,
		store:      store,
		kubeClient: orch.kubeClient,
		harness:    harness,
		repo:       repo,
		run:        run,
		hr:         hr,
		now:        now,
	}
}

func createTestSession(t *testing.T, ctx context.Context, c client.Client, sess *herorunv1alpha1.Session) {
	t.Helper()
	st := sess.Status
	if err := c.Create(ctx, sess); err != nil {
		t.Fatalf("creating Session %s: %v", sess.Name, err)
	}
	if st.Phase != "" || st.Outcome != "" || st.Error != "" || st.PRTitle != "" || st.PRBody != "" || st.Summary != "" {
		sess.Status = st
		if err := c.Status().Update(ctx, sess); err != nil {
			t.Fatalf("updating Session %s status: %v", sess.Name, err)
		}
	}
}

func TestRunWorkflow_Working_CreatesRootSession(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(30 * time.Minute)
	f := newWorkingWorkflowFixture(t, "20261126T120000Z-wfworkcreate", now, &deadline, func(o *OrchestratorOptions, _ *state.Run) {
		opts := o.Harness.RunOptions()
		opts.SystemPrompt = "Base toolset system instructions."
		o.Harness = session.NewHarness(o.Harness.LLMClient(), o.Harness.SandboxClient(), nil, opts)
	})

	wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if res.done || !res.wait || res.requeueAfter != 30*time.Minute {
		t.Fatalf("got res=%+v, want {done:false wait:true requeueAfter:30m}", res)
	}

	wantSessID := generateSessionID(f.run.ID)
	if wf.run.Phase != state.PhaseWorking {
		t.Errorf("phase = %s, want %s", wf.run.Phase, state.PhaseWorking)
	}
	if wf.run.SessionID != wantSessID {
		t.Errorf("SessionID = %q, want %q", wf.run.SessionID, wantSessID)
	}

	var sess herorunv1alpha1.Session
	if err := f.kubeClient.Get(ctx, client.ObjectKey{Namespace: f.harness.Namespace(), Name: wantSessID}, &sess); err != nil {
		t.Fatalf("getting created Session: %v", err)
	}
	if sess.Labels[LabelRun] != f.run.ID {
		t.Errorf("label %s = %q, want %q", LabelRun, sess.Labels[LabelRun], f.run.ID)
	}
	if len(sess.OwnerReferences) != 1 {
		t.Fatalf("OwnerReferences = %+v, want 1", sess.OwnerReferences)
	}
	ref := sess.OwnerReferences[0]
	if ref.Kind != "HeroRun" || ref.Name != f.hr.Name || ref.UID != f.hr.UID || ref.Controller == nil || !*ref.Controller {
		t.Errorf("unexpected OwnerReference: %+v", ref)
	}
	if sess.Spec.RunID != f.run.ID || sess.Spec.HeroRunName != f.hr.Name {
		t.Errorf("RunID/HeroRunName = %q/%q, want %q/%q", sess.Spec.RunID, sess.Spec.HeroRunName, f.run.ID, f.hr.Name)
	}
	if sess.Spec.Repository != f.repo.URL || sess.Spec.Branch != f.run.BranchName() {
		t.Errorf("Repository/Branch = %q/%q, want %q/%q", sess.Spec.Repository, sess.Spec.Branch, f.repo.URL, f.run.BranchName())
	}
	if !strings.Contains(sess.Spec.SystemPrompt, "Base toolset system instructions.") ||
		!strings.Contains(sess.Spec.SystemPrompt, f.run.Goal.Body) ||
		!strings.Contains(sess.Spec.SystemPrompt, f.repo.URL) ||
		!strings.Contains(sess.Spec.SystemPrompt, f.run.BranchName()) {
		t.Errorf("SystemPrompt missing expected sections: %q", sess.Spec.SystemPrompt)
	}
	if sess.Spec.InitialPrompt != "Please work on the goal." {
		t.Errorf("InitialPrompt = %q, want %q", sess.Spec.InitialPrompt, "Please work on the goal.")
	}
	if sess.Spec.SandboxName != f.run.SandboxName || sess.Spec.WorkspaceClaimName != f.run.WorkspaceClaimName {
		t.Errorf("SandboxName/WorkspaceClaimName = %q/%q, want %q/%q", sess.Spec.SandboxName, sess.Spec.WorkspaceClaimName, f.run.SandboxName, f.run.WorkspaceClaimName)
	}
	if sess.Spec.MaxTurns != 25 || sess.Spec.MaxDepth != 4 {
		t.Errorf("MaxTurns/MaxDepth = %d/%d, want 25/4", sess.Spec.MaxTurns, sess.Spec.MaxDepth)
	}
	if sess.Spec.Deadline == nil || !sess.Spec.Deadline.Time.Equal(deadline) {
		t.Errorf("Deadline = %v, want %v", sess.Spec.Deadline, deadline)
	}
	if !sess.Spec.AllowPullRequest {
		t.Errorf("expected AllowPullRequest = true")
	}

	events, err := f.store.ReadEvents(ctx, f.repo.Key(), f.run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) != 1 || events[0].Phase != state.PhaseWorking || events[0].Kind != "session_started" {
		t.Fatalf("unexpected events: %+v", events)
	}
	data, ok := events[0].Data.(map[string]any)
	if !ok || data["session_id"] != wantSessID {
		t.Errorf("session_started event data = %+v, want session_id=%q", events[0].Data, wantSessID)
	}
}

func TestRunWorkflow_Working_CreatesRootSession_GoalFallbackFromStore(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	f := newWorkingWorkflowFixture(t, "20261126T120000Z-wfworkgoal", now, nil, func(_ *OrchestratorOptions, r *state.Run) {
		r.Goal = nil
	})
	if err := f.store.PutGoal(ctx, f.repo.Key(), &state.GoalRecord{
		Goal: &goals.Goal{Name: f.run.GoalName, Body: "Goal body loaded from store."},
	}); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if !res.wait || res.done || res.requeueAfter != 0 {
		t.Fatalf("got res=%+v, want {done:false wait:true requeueAfter:0}", res)
	}

	var sess herorunv1alpha1.Session
	if err := f.kubeClient.Get(ctx, client.ObjectKey{Namespace: f.harness.Namespace(), Name: generateSessionID(f.run.ID)}, &sess); err != nil {
		t.Fatalf("getting Session: %v", err)
	}
	if !strings.Contains(sess.Spec.SystemPrompt, "Goal body loaded from store.") {
		t.Errorf("expected goal body from store in SystemPrompt, got: %q", sess.Spec.SystemPrompt)
	}
	if sess.Spec.Deadline != nil {
		t.Errorf("expected nil Deadline when run has no deadline, got %v", sess.Spec.Deadline)
	}
}

func TestRunWorkflow_Working_WaitsWhileSessionNonTerminal(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(15 * time.Minute)

	cases := []struct {
		name        string
		phase       herorunv1alpha1.SessionPhase
		deadline    *time.Time
		wantRequeue time.Duration
		clearSessID bool
	}{
		{name: "pending with deadline", phase: herorunv1alpha1.SessionPhasePending, deadline: &deadline, wantRequeue: 15 * time.Minute, clearSessID: true},
		{name: "running with deadline", phase: herorunv1alpha1.SessionPhaseRunning, deadline: &deadline, wantRequeue: 15 * time.Minute},
		{name: "waiting without deadline", phase: herorunv1alpha1.SessionPhaseWaiting, deadline: nil, wantRequeue: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			runID := "20261126T120000Z-wfwait"
			f := newWorkingWorkflowFixture(t, runID, now, tc.deadline)
			sessID := generateSessionID(runID)
			if !tc.clearSessID {
				f.run.SessionID = sessID
			}
			createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
				ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
				Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
				Status:     herorunv1alpha1.SessionStatus{Phase: tc.phase},
			})

			res, err := f.orch.reconcileWorkflow(ctx, newRunWorkflow(f.orch, f.repo, f.run, f.hr))
			if err != nil {
				t.Fatalf("reconcileWorkflow: %v", err)
			}
			if res.RequeueAfter != tc.wantRequeue {
				t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, tc.wantRequeue)
			}
			if len(f.orch.notifyCh) != 0 {
				t.Errorf("expected no o.Notify call while waiting on non-terminal Session, got %d", len(f.orch.notifyCh))
			}

			gotRun, err := GetRun(ctx, f.kubeClient, f.harness.Namespace(), runID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if gotRun.Phase != state.PhaseWorking || gotRun.SessionID != sessID {
				t.Errorf("got phase=%s sessionID=%q, want Working/%q", gotRun.Phase, gotRun.SessionID, sessID)
			}
			events, err := f.store.ReadEvents(ctx, f.repo.Key(), runID)
			if err != nil {
				t.Fatalf("ReadEvents: %v", err)
			}
			if len(events) != 0 {
				t.Errorf("expected no events when reusing existing non-terminal Session, got %+v", events)
			}
		})
	}
}

func TestRunWorkflow_Working_TerminalOutcomes(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	deadline := now.Add(20 * time.Minute)

	t.Run("PullRequest transitions to Selecting and releases local sandbox handle", func(t *testing.T) {
		ctx := t.Context()
		runID := "20261126T120000Z-wfpr"
		f := newWorkingWorkflowFixture(t, runID, now, &deadline)
		sessID := generateSessionID(runID)
		f.run.SessionID = sessID
		createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
			Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
			Status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseSucceeded,
				Outcome: herorunv1alpha1.SessionOutcomePullRequest,
				PRTitle: "Reduce allocations in router",
				PRBody:  "Eliminates per-request slice copy.",
			},
		})
		// Track the sandbox handle locally so we can verify releaseSandbox clears it.
		f.harness.GetOrAttachSandbox(f.run.SandboxName)
		if f.harness.SandboxClient().TrackedSandboxesCount() != 1 {
			t.Fatalf("expected 1 tracked sandbox before reconcile")
		}

		res, err := f.orch.reconcileWorkflow(ctx, newRunWorkflow(f.orch, f.repo, f.run, f.hr))
		if err != nil {
			t.Fatalf("reconcileWorkflow: %v", err)
		}
		if res != (reconcile.Result{}) {
			t.Errorf("expected empty reconcile.Result, got %+v", res)
		}
		if len(f.orch.notifyCh) != 1 {
			t.Errorf("expected o.Notify to trigger Selecting immediately, got %d notifications", len(f.orch.notifyCh))
		}
		if f.harness.SandboxClient().TrackedSandboxesCount() != 0 {
			t.Errorf("expected releaseSandbox to clear local handle, got %d tracked", f.harness.SandboxClient().TrackedSandboxesCount())
		}
		if _, err := f.harness.SandboxClient().GetSandboxResource(ctx, f.harness.Namespace(), f.run.SandboxName); err != nil {
			t.Errorf("expected Kubernetes Sandbox resource to remain for Selecting, got err: %v", err)
		}

		gotRun, err := GetRun(ctx, f.kubeClient, f.harness.Namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseSelecting || gotRun.PRTitle != "Reduce allocations in router" || gotRun.PRBody != "Eliminates per-request slice copy." {
			t.Errorf("got run %+v, want Selecting with PRTitle/PRBody", gotRun)
		}

		events, err := f.store.ReadEvents(ctx, f.repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		var sawPRReq, sawPhase bool
		for _, ev := range events {
			if ev.Phase == state.PhaseSelecting && ev.Kind == "pull_request_requested" {
				sawPRReq = true
			}
			if ev.Phase == state.PhaseSelecting && ev.Kind == "phase" {
				sawPhase = true
			}
		}
		if !sawPRReq || !sawPhase {
			t.Errorf("expected pull_request_requested and phase events, got %+v", events)
		}
	})

	terminalCases := []struct {
		name          string
		status        herorunv1alpha1.SessionStatus
		wantPhase     state.RunPhase
		wantEventKind string
		wantSummary   string
		wantErrorKind string
		wantErrorPart string
	}{
		{
			name: "Finished transitions to NoImprovement with Summary",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseSucceeded,
				Outcome: herorunv1alpha1.SessionOutcomeFinished,
				Summary: "Already optimal; no safe optimization found.",
			},
			wantPhase:     state.PhaseNoImprovement,
			wantEventKind: "finished",
			wantSummary:   "Already optimal; no safe optimization found.",
		},
		{
			name: "Completed transitions to NoImprovement",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseSucceeded,
				Outcome: herorunv1alpha1.SessionOutcomeCompleted,
			},
			wantPhase:     state.PhaseNoImprovement,
			wantEventKind: "no_improvement",
		},
		{
			name: "MaxTurns transitions to NoImprovement",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseSucceeded,
				Outcome: herorunv1alpha1.SessionOutcomeMaxTurns,
			},
			wantPhase:     state.PhaseNoImprovement,
			wantEventKind: "no_improvement",
		},
		{
			name: "DeadlineExceeded transitions to Failed with deadline_exceeded",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseFailed,
				Outcome: herorunv1alpha1.SessionOutcomeDeadlineExceeded,
				Error:   "deadline exceeded",
			},
			wantPhase:     state.PhaseFailed,
			wantEventKind: "deadline_exceeded",
			wantErrorKind: "deadline_exceeded",
			wantErrorPart: "deadline exceeded",
		},
		{
			name: "Failed transitions to Failed with session error",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseFailed,
				Outcome: herorunv1alpha1.SessionOutcomeFailed,
				Error:   "LLM API 503",
			},
			wantPhase:     state.PhaseFailed,
			wantEventKind: "failed",
			wantErrorKind: "failed",
			wantErrorPart: "session error: LLM API 503",
		},
		{
			name: "Failed with empty error uses fallback message",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseFailed,
				Outcome: herorunv1alpha1.SessionOutcomeFailed,
			},
			wantPhase:     state.PhaseFailed,
			wantEventKind: "failed",
			wantErrorKind: "failed",
			wantErrorPart: "session failed",
		},
		{
			name: "Canceled before deadline transitions to Failed",
			status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseCanceled,
				Outcome: herorunv1alpha1.SessionOutcomeCanceled,
				Error:   "canceled externally",
			},
			wantPhase:     state.PhaseFailed,
			wantEventKind: "failed",
			wantErrorKind: "failed",
			wantErrorPart: "session canceled: canceled externally",
		},
	}

	for _, tc := range terminalCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			runID := "20261126T120000Z-wfterm"
			f := newWorkingWorkflowFixture(t, runID, now, &deadline)
			sessID := generateSessionID(runID)
			f.run.SessionID = sessID
			createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
				ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
				Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
				Status:     tc.status,
			})

			res, err := f.orch.reconcileWorkflow(ctx, newRunWorkflow(f.orch, f.repo, f.run, f.hr))
			if err != nil {
				t.Fatalf("reconcileWorkflow: %v", err)
			}
			if res != (reconcile.Result{}) {
				t.Errorf("expected empty reconcile.Result, got %+v", res)
			}
			if len(f.orch.notifyCh) != 0 {
				t.Errorf("expected no o.Notify on terminal run, got %d", len(f.orch.notifyCh))
			}

			gotRun, err := GetRun(ctx, f.kubeClient, f.harness.Namespace(), runID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if gotRun.Phase != tc.wantPhase {
				t.Errorf("phase = %s, want %s", gotRun.Phase, tc.wantPhase)
			}
			if tc.wantSummary != "" && gotRun.Summary != tc.wantSummary {
				t.Errorf("Summary = %q, want %q", gotRun.Summary, tc.wantSummary)
			}
			if tc.wantErrorKind != "" && gotRun.ErrorKind != tc.wantErrorKind {
				t.Errorf("ErrorKind = %q, want %q", gotRun.ErrorKind, tc.wantErrorKind)
			}
			if tc.wantErrorPart != "" && !strings.Contains(gotRun.Error, tc.wantErrorPart) {
				t.Errorf("Error = %q, want substring %q", gotRun.Error, tc.wantErrorPart)
			}

			if _, err := f.harness.SandboxClient().GetSandboxResource(ctx, f.harness.Namespace(), f.run.SandboxName); !k8serrors.IsNotFound(err) {
				t.Errorf("expected sandbox deleted after terminal run, got err: %v", err)
			}

			events, err := f.store.ReadEvents(ctx, f.repo.Key(), runID)
			if err != nil {
				t.Fatalf("ReadEvents: %v", err)
			}
			if len(events) != 1 || events[0].Phase != tc.wantPhase || events[0].Kind != tc.wantEventKind {
				t.Fatalf("events = %+v, want single %s/%s event", events, tc.wantPhase, tc.wantEventKind)
			}
			if tc.wantEventKind == "deadline_exceeded" {
				data, ok := events[0].Data.(map[string]any)
				if !ok || data["deadline"] != deadline.Format(time.RFC3339) || data["elapsed"] == "" {
					t.Errorf("expected deadline and elapsed in event data, got %+v", events[0].Data)
				}
			}
		})
	}
}

func TestRunWorkflow_Working_TerminalOutcomeConsumedBeforeDeadline(t *testing.T) {
	ctx := t.Context()
	deadline := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	afterDeadline := deadline.Add(5 * time.Minute)
	runID := "20261126T120000Z-wftermfirst"

	f := newWorkingWorkflowFixture(t, runID, afterDeadline, &deadline)
	sessID := generateSessionID(runID)
	f.run.SessionID = sessID
	createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
		Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
		Status: herorunv1alpha1.SessionStatus{
			Phase:   herorunv1alpha1.SessionPhaseSucceeded,
			Outcome: herorunv1alpha1.SessionOutcomePullRequest,
			PRTitle: "Last-second optimization",
			PRBody:  "Completed right before deadline.",
		},
	})

	wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
	res, err := wf.Run(ctx)
	if err != nil {
		t.Fatalf("wf.Run: %v", err)
	}
	if res != (workflowResult{}) {
		t.Fatalf("got res=%+v, want immediate requeue for Selecting", res)
	}
	if wf.run.Phase != state.PhaseSelecting || wf.run.PRTitle != "Last-second optimization" {
		t.Errorf("got phase=%s PRTitle=%q, want Selecting/Last-second optimization", wf.run.Phase, wf.run.PRTitle)
	}

	// Verify Session status was not overwritten to Canceled.
	var gotSess herorunv1alpha1.Session
	if err := f.kubeClient.Get(ctx, client.ObjectKey{Namespace: f.harness.Namespace(), Name: sessID}, &gotSess); err != nil {
		t.Fatalf("Get Session: %v", err)
	}
	if gotSess.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || gotSess.Status.Outcome != herorunv1alpha1.SessionOutcomePullRequest {
		t.Errorf("Session status unexpectedly mutated: %+v", gotSess.Status)
	}
}

func TestRunWorkflow_Working_DeadlineHandling(t *testing.T) {
	deadline := time.Date(2026, 11, 26, 12, 30, 0, 0, time.UTC)

	t.Run("cancels non-terminal root Session and fails run", func(t *testing.T) {
		ctx := t.Context()
		now := deadline.Add(time.Minute)
		runID := "20261126T120000Z-wfdlcancel"
		f := newWorkingWorkflowFixture(t, runID, now, &deadline)
		sessID := generateSessionID(runID)
		f.run.SessionID = sessID
		waitUntil := metav1.NewTime(deadline.Add(5 * time.Minute))
		createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
			Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
			Status: herorunv1alpha1.SessionStatus{
				Phase:        herorunv1alpha1.SessionPhaseWaiting,
				WaitingUntil: &waitUntil,
			},
		})

		if _, err := f.orch.reconcileWorkflow(ctx, newRunWorkflow(f.orch, f.repo, f.run, f.hr)); err != nil {
			t.Fatalf("reconcileWorkflow: %v", err)
		}

		var gotSess herorunv1alpha1.Session
		if err := f.kubeClient.Get(ctx, client.ObjectKey{Namespace: f.harness.Namespace(), Name: sessID}, &gotSess); err != nil {
			t.Fatalf("Get Session: %v", err)
		}
		if gotSess.Status.Phase != herorunv1alpha1.SessionPhaseCanceled || gotSess.Status.Outcome != herorunv1alpha1.SessionOutcomeCanceled {
			t.Errorf("Session status = %s/%s, want Canceled/Canceled", gotSess.Status.Phase, gotSess.Status.Outcome)
		}
		if gotSess.Status.WaitingUntil != nil || gotSess.Status.FinishedAt == nil || gotSess.Status.ObservedAt == nil {
			t.Errorf("unexpected Session timestamps/wait: %+v", gotSess.Status)
		}

		gotRun, err := GetRun(ctx, f.kubeClient, f.harness.Namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed || gotRun.ErrorKind != "deadline_exceeded" {
			t.Errorf("got phase=%s errorKind=%q, want Failed/deadline_exceeded", gotRun.Phase, gotRun.ErrorKind)
		}
		if _, err := f.harness.SandboxClient().GetSandboxResource(ctx, f.harness.Namespace(), f.run.SandboxName); !k8serrors.IsNotFound(err) {
			t.Errorf("expected sandbox deleted on deadline exceeded, got err: %v", err)
		}
		events, err := f.store.ReadEvents(ctx, f.repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		if len(events) != 1 || events[0].Kind != "deadline_exceeded" {
			t.Fatalf("expected deadline_exceeded event, got %+v", events)
		}
	})

	t.Run("fails without creating Session when deadline already reached", func(t *testing.T) {
		ctx := t.Context()
		runID := "20261126T120000Z-wfdlnosess"
		f := newWorkingWorkflowFixture(t, runID, deadline, &deadline)

		wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
		res, err := wf.Run(ctx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if !res.done || wf.run.Phase != state.PhaseFailed || wf.run.ErrorKind != "deadline_exceeded" {
			t.Fatalf("got res=%+v phase=%s errorKind=%q, want done Failed/deadline_exceeded", res, wf.run.Phase, wf.run.ErrorKind)
		}
		var list herorunv1alpha1.SessionList
		if err := f.kubeClient.List(ctx, &list); err != nil {
			t.Fatalf("List Sessions: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected no Session created after deadline, got %d", len(list.Items))
		}
	})

	t.Run("retries cleanly as deadline_exceeded if Session was already canceled after deadline", func(t *testing.T) {
		ctx := t.Context()
		now := deadline.Add(2 * time.Minute)
		runID := "20261126T120000Z-wfdlretry"
		f := newWorkingWorkflowFixture(t, runID, now, &deadline)
		sessID := generateSessionID(runID)
		f.run.SessionID = sessID
		createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
			Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
			Status: herorunv1alpha1.SessionStatus{
				Phase:   herorunv1alpha1.SessionPhaseCanceled,
				Outcome: herorunv1alpha1.SessionOutcomeCanceled,
				Error:   "run deadline exceeded",
			},
		})

		wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
		res, err := wf.Run(ctx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if !res.done || wf.run.Phase != state.PhaseFailed || wf.run.ErrorKind != "deadline_exceeded" {
			t.Errorf("got res=%+v phase=%s errorKind=%q, want done Failed/deadline_exceeded", res, wf.run.Phase, wf.run.ErrorKind)
		}
	})
}

func TestRunWorkflow_Working_EdgeCases(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)

	t.Run("empty SandboxName fails without creating Session", func(t *testing.T) {
		ctx := t.Context()
		f := newWorkingWorkflowFixture(t, "20261126T120000Z-wfnosb", now, nil, func(_ *OrchestratorOptions, r *state.Run) {
			r.SandboxName = ""
		})
		wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
		res, err := wf.Run(ctx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if !res.done || wf.run.Phase != state.PhaseFailed || !strings.Contains(wf.run.Error, "sandbox name is empty") {
			t.Errorf("got res=%+v phase=%s err=%q", res, wf.run.Phase, wf.run.Error)
		}
		var list herorunv1alpha1.SessionList
		if err := f.kubeClient.List(ctx, &list); err != nil {
			t.Fatalf("List Sessions: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 Sessions, got %d", len(list.Items))
		}
	})

	t.Run("session belonging to another run fails", func(t *testing.T) {
		ctx := t.Context()
		runID := "20261126T120000Z-wfother"
		f := newWorkingWorkflowFixture(t, runID, now, nil)
		sessID := generateSessionID(runID)
		createTestSession(t, ctx, f.kubeClient, &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{Name: sessID, Namespace: f.harness.Namespace()},
			Spec:       herorunv1alpha1.SessionSpec{RunID: "different-run"},
			Status:     herorunv1alpha1.SessionStatus{Phase: herorunv1alpha1.SessionPhaseRunning},
		})
		wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
		res, err := wf.Run(ctx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if !res.done || wf.run.Phase != state.PhaseFailed || !strings.Contains(wf.run.Error, "belongs to run different-run") {
			t.Errorf("got res=%+v phase=%s err=%q", res, wf.run.Phase, wf.run.Error)
		}
	})

	t.Run("canceled context returns without creating Session", func(t *testing.T) {
		ctx := t.Context()
		f := newWorkingWorkflowFixture(t, "20261126T120000Z-wfctx", now, nil)
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		wf := newRunWorkflow(f.orch, f.repo, f.run, f.hr)
		res, err := wf.Run(cancelCtx)
		if err != nil {
			t.Fatalf("wf.Run: %v", err)
		}
		if res != (workflowResult{}) || wf.run.SessionID != "" {
			t.Errorf("got res=%+v sessionID=%q, want zero", res, wf.run.SessionID)
		}
		var list herorunv1alpha1.SessionList
		if err := f.kubeClient.List(ctx, &list); err != nil {
			t.Fatalf("List Sessions: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 Sessions on canceled context, got %d", len(list.Items))
		}
	})
}
