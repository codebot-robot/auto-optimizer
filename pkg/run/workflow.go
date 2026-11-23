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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// workflowResult instructs the caller how and when to invoke Run again.
type workflowResult struct {
	// done reports that the workflow has reached a terminal phase and should not be called again.
	done bool
	// requeueAfter tells the caller to invoke Run again after a duration.
	requeueAfter time.Duration
}

// runWorkflow drives a run through its reconciliation loop.
type runWorkflow struct {
	orch *Orchestrator
	repo *state.Repo
	run  *state.Run
	hr   *herorunv1alpha1.HeroRun
}

// newRunWorkflow creates a runWorkflow for the given orchestrator, repo, run, and HeroRun.
func newRunWorkflow(orch *Orchestrator, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun) *runWorkflow {
	return &runWorkflow{
		orch: orch,
		repo: repo,
		run:  run,
		hr:   hr,
	}
}

// Run executes one unit of work for the run, advancing its state in memory,
// and returns a workflowResult instructing the caller how and when to invoke Run again.
func (w *runWorkflow) Run(ctx context.Context) (workflowResult, error) {
	switch w.run.Phase {
	case state.PhasePending:
		return w.runPending(ctx)
	default:
		return workflowResult{}, fmt.Errorf("unsupported workflow phase: %s", w.run.Phase)
	}
}

// runPending executes the Pending phase: assigns a sandbox name, sets the ready deadline,
// ensures the workspace claim exists (if configured), creates the Sandbox (or reuses it
// if already created by this run), and advances the phase to Preparing.
func (w *runWorkflow) runPending(ctx context.Context) (workflowResult, error) {
	log := scoped.Log(ctx)
	now := w.orch.clock().UTC()

	// 1. Assign sandbox name if not already assigned
	sandboxName := w.run.SandboxName
	if sandboxName == "" {
		sandboxName = sandboxNameForRun(w.run.ID)
		w.run.SandboxName = sandboxName
	}

	// 2. Set ready deadline
	readyDeadline := now.Add(w.orch.readyTimeout)
	w.run.ReadyDeadline = &readyDeadline

	log.Info("preparing run: creating sandbox", "repo", w.repo.URL, "run", w.run.ID, "sandbox", sandboxName)

	// 3. Build sandbox options (including HeroRun owner reference) and workspace claim if configured
	sbOpts := w.buildCreateSandboxOptions()

	if w.orch.workspaceStorageClass != "" {
		if len(sbOpts.OwnerReferences) == 0 {
			return w.failRun(now, "cannot create workspace claim: HeroRun owner is required"), nil
		}
		claimName := sandboxNameForRun(w.run.ID)
		if err := w.ensureWorkspaceClaim(ctx, claimName, sbOpts.OwnerReferences); err != nil {
			return w.failRun(now, fmt.Sprintf("creating workspace claim: %v", err)), nil
		}
		w.run.WorkspaceClaimName = claimName
		sbOpts.WorkspaceClaimName = claimName
	}

	// 4. Create sandbox with a label naming the run, annotation naming the repository, owner reference, and shutdown time.
	// On AlreadyExists, read the existing Sandbox and reuse it if its owner is this HeroRun.
	if _, err := w.orch.harness.CreateSandboxWithOptions(ctx, sbOpts); err != nil {
		reused := false
		if k8serrors.IsAlreadyExists(err) {
			existing, getErr := w.orch.harness.SandboxClient().AgentsClient().AgentsV1beta1().Sandboxes(w.orch.namespace()).Get(ctx, sandboxName, metav1.GetOptions{})
			if getErr != nil {
				return workflowResult{}, fmt.Errorf("getting existing sandbox %s: %w", sandboxName, getErr)
			}
			for _, ref := range existing.OwnerReferences {
				if ref.Kind == "HeroRun" && ref.UID == w.hr.UID {
					reused = true
					w.orch.harness.GetOrAttachSandbox(sandboxName)
					break
				}
			}
		}
		if !reused {
			return w.failRun(now, fmt.Sprintf("creating sandbox: %v", err)), nil
		}
	}

	// 5. Advance phase to Preparing
	w.run.SetPhase(state.PhasePreparing, now)

	requeueAfter := min(readySafetyRequeueInterval, w.orch.readyTimeout)
	if requeueAfter <= 0 {
		requeueAfter = time.Millisecond
	}
	return workflowResult{requeueAfter: requeueAfter}, nil
}

// buildCreateSandboxOptions builds the session.CreateSandboxOptions for the run,
// including labels, annotations, HeroRun owner reference, shutdown time, and workspace claim.
func (w *runWorkflow) buildCreateSandboxOptions() session.CreateSandboxOptions {
	sandboxName := w.run.SandboxName
	if sandboxName == "" {
		sandboxName = sandboxNameForRun(w.run.ID)
	}

	ownerRefs := heroRunOwnerReferences(w.hr)
	labels := map[string]string{
		LabelRun: w.run.ID,
	}
	annotations := map[string]string{
		AnnotationRepo: w.repo.Key().String(),
	}
	var sbShutdown time.Time
	if w.run.Deadline != nil {
		sbShutdown = w.run.Deadline.Add(SandboxShutdownGrace)
	}

	return session.CreateSandboxOptions{
		Name:               sandboxName,
		Labels:             labels,
		Annotations:        annotations,
		OwnerReferences:    ownerRefs,
		ShutdownTime:       sbShutdown,
		WorkspaceClaimName: w.run.WorkspaceClaimName,
		PortalIntercept:    w.orch.portalBinder != nil,
		ExtendLifecycle:    false,
	}
}

// ensureWorkspaceClaim creates a PersistentVolumeClaim for the run's workspace if it does not already exist.
func (w *runWorkflow) ensureWorkspaceClaim(ctx context.Context, name string, ownerRefs []metav1.OwnerReference) error {
	storageQty, err := resource.ParseQuantity(w.orch.workspaceSize)
	if err != nil {
		return fmt.Errorf("parsing workspace size %q: %w", w.orch.workspaceSize, err)
	}

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       w.orch.namespace(),
			OwnerReferences: ownerRefs,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOncePod,
			},
			StorageClassName: &w.orch.workspaceStorageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageQty,
				},
			},
		},
	}

	if err := w.orch.kubeClient.Create(ctx, claim); err != nil && !k8serrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating workspace claim %s: %w", name, err)
	}
	return nil
}

func (w *runWorkflow) failRun(now time.Time, errMsg string) workflowResult {
	w.run.ReadyDeadline = nil
	w.run.SetPhase(state.PhaseFailed, now)
	w.run.Error = errMsg
	return workflowResult{done: true}
}
