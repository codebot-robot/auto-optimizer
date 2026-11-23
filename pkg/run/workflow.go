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

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// Result instructs the caller how and when to invoke Run again.
// It directly mirrors controller-runtime's reconcile.Result.
type Result struct {
	// Requeue tells the controller to requeue the reconciliation request immediately.
	Requeue bool
	// RequeueAfter tells the controller to requeue the reconciliation request after a duration.
	RequeueAfter time.Duration
}

// IsZero returns true if the Result is empty.
func (r Result) IsZero() bool {
	return !r.Requeue && r.RequeueAfter == 0
}

// RunWorkflow is the concrete runner that drives a run through its reconciliation loop.
type RunWorkflow struct {
	orch    *Orchestrator
	Repo    *state.Repo
	run     *state.Run
	HeroRun *herorunv1alpha1.HeroRun
}

// NewRunWorkflow creates a new RunWorkflow for the given orchestrator, repo, run, and HeroRun.
func NewRunWorkflow(orch *Orchestrator, repo *state.Repo, run *state.Run, hr *herorunv1alpha1.HeroRun) *RunWorkflow {
	return &RunWorkflow{
		orch:    orch,
		Repo:    repo,
		run:     run,
		HeroRun: hr,
	}
}

// Run executes one unit of work for the run, advancing its state in memory,
// and returns a Result instructing the caller how and when to invoke Run again.
func (w *RunWorkflow) Run(ctx context.Context) (Result, error) {
	switch w.run.Phase {
	case state.PhasePending:
		return w.runPending(ctx)
	default:
		return Result{}, fmt.Errorf("unsupported workflow phase: %s", w.run.Phase)
	}
}

// runPending executes the Pending phase: assigns a sandbox name, sets the ready deadline,
// ensures the workspace claim exists (if configured), creates the Sandbox (or reuses it
// if already created by this run), and advances the phase to Preparing.
func (w *RunWorkflow) runPending(ctx context.Context) (Result, error) {
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

	log.Info("preparing run: creating sandbox", "repo", w.Repo.URL, "run", w.run.ID, "sandbox", sandboxName)

	// 3. Build sandbox options (including HeroRun owner reference) and workspace claim if configured
	sbOpts := w.buildCreateSandboxOptions(ctx)

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
				return Result{}, fmt.Errorf("getting existing sandbox %s: %w", sandboxName, getErr)
			}
			for _, ref := range existing.OwnerReferences {
				if ref.Kind == "HeroRun" && ref.UID == w.HeroRun.UID {
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
	w.HeroRun.Status = HeroRunFromRun(w.run).Status

	requeueAfter := readySafetyRequeueInterval
	if w.run.ReadyDeadline != nil {
		rem := w.run.ReadyDeadline.Sub(now)
		if rem < requeueAfter {
			requeueAfter = rem
		}
	}
	if requeueAfter <= 0 {
		requeueAfter = time.Millisecond
	}
	return Result{RequeueAfter: requeueAfter}, nil
}

func (w *RunWorkflow) buildCreateSandboxOptions(ctx context.Context) session.CreateSandboxOptions {
	return w.orch.buildCreateSandboxOptions(ctx, w.Repo, w.run, w.HeroRun)
}

func (w *RunWorkflow) ensureWorkspaceClaim(ctx context.Context, name string, ownerRefs []metav1.OwnerReference) error {
	return w.orch.ensureWorkspaceClaim(ctx, name, ownerRefs)
}

func (w *RunWorkflow) failRun(now time.Time, errMsg string) Result {
	w.run.ReadyDeadline = nil
	w.run.SetPhase(state.PhaseFailed, now)
	w.run.Error = errMsg
	w.HeroRun.Status = HeroRunFromRun(w.run).Status
	return Result{}
}
