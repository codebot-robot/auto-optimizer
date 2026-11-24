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
	"fmt"
	"os"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// workflowResult instructs the caller how and when to invoke Run again.
type workflowResult struct {
	// done reports that the workflow has reached a terminal phase and should not be called again.
	done bool
	// requeueAfter tells the caller to invoke Run again after a duration; zero means invoke again immediately.
	requeueAfter time.Duration
}

// runWorkflow drives a run through its reconciliation loop.
type runWorkflow struct {
	orch *Orchestrator
	repo *state.Repo
	run  *state.Run
	hr   *herorunv1alpha1.HeroRun
	end  *runEnd
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
	case state.PhasePreparing:
		return w.runPreparing(ctx)
	case state.PhaseSelecting:
		return w.runSelecting(ctx)
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

	// 1. Determine sandbox name
	sandboxName := w.run.SandboxName
	if sandboxName == "" {
		sandboxName = sandboxNameForRun(w.run.ID)
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

	// 5. Record sandbox ownership and advance phase to Preparing
	w.run.SandboxName = sandboxName
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

// runPreparing checks whether the run's Sandbox is ready, prepares it when ready
// (binding portal egress policy, ensuring the repository clone, and configuring
// git identity), and advances the run to Working. When the Sandbox is not ready
// yet or portal binding lags before the ready deadline, runPreparing returns a
// requeue duration without polling.
func (w *runWorkflow) runPreparing(ctx context.Context) (workflowResult, error) {
	log := scoped.Log(ctx)
	now := w.orch.clock().UTC()

	sbObj, err := w.orch.harness.SandboxClient().GetSandboxResource(ctx, w.orch.namespace(), w.run.SandboxName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return w.failRun(now, fmt.Sprintf("sandbox %s was deleted before becoming ready", w.run.SandboxName)), nil
		}
		return workflowResult{}, fmt.Errorf("getting sandbox %s: %w", w.run.SandboxName, err)
	}

	if failMsg, failed := session.IsSandboxFailed(sbObj); failed {
		return w.failRun(now, fmt.Sprintf("sandbox %s failed: %s", w.run.SandboxName, failMsg)), nil
	}

	sb := w.orch.harness.GetOrAttachSandbox(w.run.SandboxName)
	if pullErr := sb.CheckPodImagePullStatus(ctx); pullErr != nil {
		return w.failRun(now, fmt.Sprintf("sandbox %s pod image pull failed: %v", w.run.SandboxName, pullErr)), nil
	}

	ready := false
	for _, cond := range sbObj.Status.Conditions {
		if cond.Type == string(sandboxv1beta1.SandboxConditionReady) && cond.Status == metav1.ConditionTrue {
			ready = true
			break
		}
	}

	if !ready {
		if w.run.ReadyDeadline != nil && !now.Before(*w.run.ReadyDeadline) {
			return w.failRun(now, "sandbox not ready before deadline"), nil
		}
		return workflowResult{
			requeueAfter: requeueUntilDeadline(readySafetyRequeueInterval, now, w.run.ReadyDeadline),
		}, nil
	}

	if err := w.orch.store.AppendEvent(ctx, w.repo.Key(), w.run.ID, state.Event{
		Time:    w.orch.clock().UTC(),
		Phase:   w.run.Phase,
		Kind:    "sandbox_ready",
		Message: fmt.Sprintf("Sandbox %s is ready", w.run.SandboxName),
		Data:    map[string]any{"sandbox": w.run.SandboxName},
	}); err != nil {
		log.Error(err, "failed to append sandbox_ready event", "run", w.run.ID)
	}

	podIP := ""
	if len(sbObj.Status.PodIPs) > 0 {
		podIP = sbObj.Status.PodIPs[0]
	}
	sb.SetPodInfo(sbObj.Name, podIP)

	if err := w.orch.prepareSandbox(ctx, w.repo, w.run, sb); err != nil {
		failTime := w.orch.clock().UTC()
		var pErr *portalBindError
		if errors.As(err, &pErr) {
			log.Error(pErr.err, "failed to bind portal egress policy", "run", w.run.ID, "sandbox", w.run.SandboxName)
			if w.run.ReadyDeadline != nil && !now.Before(*w.run.ReadyDeadline) {
				return w.failRunWithKind(failTime, "portal_bind_failed", pErr.err.Error()), nil
			}
			return workflowResult{
				requeueAfter: requeueUntilDeadline(portalBindRetryInterval, now, w.run.ReadyDeadline),
			}, nil
		}

		log.Error(err, "sandbox preparation failed", "run", w.run.ID, "sandbox", w.run.SandboxName)
		return w.failRun(failTime, err.Error()), nil
	}

	workTime := w.orch.clock().UTC()
	w.run.SetPhase(state.PhaseWorking, workTime)
	w.run.ReadyDeadline = nil
	log.Info("run successfully prepared and entered Working phase", "run", w.run.ID, "sandbox", w.run.SandboxName)
	return workflowResult{}, nil
}

// runSelecting executes the Selecting phase: runs the goal's verify command if configured,
// exports candidate commits as a packfile while enforcing refusal checks, pushes the branch
// to the forge, and advances the run to OpeningPR. Sandbox release is deferred to the caller
// after saving the state transition.
func (w *runWorkflow) runSelecting(ctx context.Context) (workflowResult, error) {
	log := scoped.Log(ctx)

	if w.run.SandboxName == "" {
		return w.failRun(w.orch.clock().UTC(), "sandbox name is empty for selecting run"), nil
	}
	sb := w.orch.harness.GetOrAttachSandbox(w.run.SandboxName)

	if w.run.Goal != nil && w.run.Goal.Verify != "" {
		if res, stop := w.runVerify(ctx, sb); stop {
			return res, nil
		}
	}

	destDir, err := os.MkdirTemp("", "ao-export-*")
	if err != nil {
		return w.failRun(w.orch.clock().UTC(), fmt.Sprintf("creating temp dir for export: %v", err)), nil
	}
	defer func() {
		if rmErr := os.RemoveAll(destDir); rmErr != nil {
			log.V(2).Info("failed to remove temp export dir", "dir", destDir, "error", rmErr)
		}
	}()

	exportRes, err := sb.ExportCommits(ctx, session.ExportOptions{
		RepoDir: "/workspace/repo",
		Base:    w.run.BaseCommit,
		DestDir: destDir,
	})
	if err != nil {
		now := w.orch.clock().UTC()
		switch {
		case errors.Is(err, session.ErrNoChanges):
			return w.noImprovement(now, "no_changes", "no changes against base commit", nil), nil
		case errors.Is(err, session.ErrGoalsChanged):
			return w.noImprovement(now, "refused", "changes under .goals/", nil), nil
		case errors.Is(err, session.ErrUncommittedChanges):
			return w.failRun(now, err.Error()), nil
		default:
			return w.failRun(now, fmt.Sprintf("export commits: %v", err)), nil
		}
	}

	log.V(2).Info("commits exported successfully", "run", w.run.ID, "tip", exportRes.Tip)
	w.appendEvent(ctx, w.orch.clock().UTC(), state.PhaseSelecting, "commits_exported", fmt.Sprintf("Exported commits with tip %s", exportRes.Tip), map[string]any{"tip": exportRes.Tip})

	var f forge.Forge
	if w.orch.forgeFor != nil {
		f, err = w.orch.forgeFor(w.repo.URL)
		if err != nil {
			return w.failRunWithKind(w.orch.clock().UTC(), "push_failed", fmt.Sprintf("selecting forge for %s: %v", w.repo.URL, err)), nil
		}
	}
	if f == nil {
		return w.failRunWithKind(w.orch.clock().UTC(), "push_failed", fmt.Sprintf("no forge configured for repository %s", w.repo.URL)), nil
	}

	packFile, err := os.Open(exportRes.PackfilePath)
	if err != nil {
		return w.failRunWithKind(w.orch.clock().UTC(), "push_failed", fmt.Sprintf("opening packfile: %v", err)), nil
	}
	defer func() {
		if closeErr := packFile.Close(); closeErr != nil {
			log.V(2).Info("failed to close packfile", "error", closeErr)
		}
	}()

	var creds forge.PushCredentialer
	if c, ok := f.(forge.PushCredentialer); ok {
		creds = c
	}

	branchName := w.run.BranchName()
	if pushErr := forge.PushPackfile(ctx, forge.PushPackfileOptions{
		RemoteURL:   w.repo.URL,
		Branch:      branchName,
		Tip:         plumbing.NewHash(exportRes.Tip),
		Base:        plumbing.NewHash(w.run.BaseCommit),
		Packfile:    packFile,
		Credentials: creds,
	}); pushErr != nil {
		now := w.orch.clock().UTC()
		if errors.Is(pushErr, forge.ErrBranchMoved) {
			return w.failRunWithKind(now, "branch_moved", "remote branch is not at base commit"), nil
		}
		return w.failRunWithKind(now, "push_failed", fmt.Sprintf("push packfile: %v", pushErr)), nil
	}

	w.run.Branch = branchName
	pushTime := w.orch.clock().UTC()
	w.appendEvent(ctx, pushTime, state.PhaseSelecting, "branch_pushed", fmt.Sprintf("Pushed branch %s with tip %s", branchName, exportRes.Tip), map[string]any{
		"branch": branchName,
		"tip":    exportRes.Tip,
	})

	w.run.SetPhase(state.PhaseOpeningPR, pushTime)
	return workflowResult{}, nil
}

func (w *runWorkflow) runVerify(ctx context.Context, sb *session.Sandbox) (workflowResult, bool) {
	w.appendEvent(ctx, w.orch.clock().UTC(), state.PhaseSelecting, "verify_started", fmt.Sprintf("Running verify command: %s", w.run.Goal.Verify), map[string]any{"command": w.run.Goal.Verify})

	verifyCtx, verifyCancel := context.WithTimeout(ctx, w.orch.verifyTimeout)
	res, err := sb.RunVerify(verifyCtx, "/workspace/repo", w.run.Goal.Verify, 50)
	timedOut := errors.Is(err, context.DeadlineExceeded) || verifyCtx.Err() == context.DeadlineExceeded || status.Code(err) == codes.DeadlineExceeded
	verifyCancel()

	if ctx.Err() != nil {
		return workflowResult{}, true
	}
	if err != nil {
		now := w.orch.clock().UTC()
		if timedOut {
			return w.noImprovement(now, "verify_failed", fmt.Sprintf("verify command timed out after %s", w.orch.verifyTimeout), nil), true
		}
		return w.failRunWithKind(now, "verify_error", fmt.Sprintf("running verify command: %v", err)), true
	}
	if res.ExitCode != 0 {
		msg := fmt.Sprintf("verify command failed with exit code %d", res.ExitCode)
		return w.noImprovement(w.orch.clock().UTC(), "verify_failed", msg, map[string]any{
			"exitCode": res.ExitCode,
			"tail":     res.Tail,
		}), true
	}

	w.appendEvent(ctx, w.orch.clock().UTC(), state.PhaseSelecting, "verify_passed", "Verify command passed", nil)
	return workflowResult{}, false
}

func (w *runWorkflow) appendEvent(ctx context.Context, at time.Time, phase state.RunPhase, kind, msg string, data any) {
	if err := w.orch.store.AppendEvent(ctx, w.repo.Key(), w.run.ID, state.Event{
		Time:    at,
		Phase:   phase,
		Kind:    kind,
		Message: msg,
		Data:    data,
	}); err != nil {
		scoped.Log(ctx).Error(err, "failed to append event", "kind", kind, "run", w.run.ID)
	}
}

func (w *runWorkflow) noImprovement(now time.Time, kind, msg string, data map[string]any) workflowResult {
	w.run.ReadyDeadline = nil
	w.run.SetPhase(state.PhaseNoImprovement, now)
	w.end = &runEnd{
		Phase:   state.PhaseNoImprovement,
		Kind:    kind,
		Message: msg,
		Data:    data,
	}
	return workflowResult{done: true}
}

func (w *runWorkflow) failRun(now time.Time, errMsg string) workflowResult {
	return w.failRunWithKind(now, "failed", errMsg)
}

func (w *runWorkflow) failRunWithKind(now time.Time, kind, errMsg string) workflowResult {
	w.run.ReadyDeadline = nil
	w.run.SetPhase(state.PhaseFailed, now)
	w.run.Error = errMsg
	w.run.ErrorKind = kind
	return workflowResult{done: true}
}

func requeueUntilDeadline(interval time.Duration, now time.Time, deadline *time.Time) time.Duration {
	requeueAfter := interval
	if deadline != nil {
		if rem := deadline.Sub(now); rem < requeueAfter {
			requeueAfter = rem
		}
	}
	if requeueAfter <= 0 {
		return time.Millisecond
	}
	return requeueAfter
}
