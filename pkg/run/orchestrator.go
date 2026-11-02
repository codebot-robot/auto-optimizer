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
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets/ao"
)

// Request identifies a run to be reconciled.
type Request struct {
	Repo  state.RepoKey
	RunID string
}

// String returns a human-readable identifier for the request.
func (r Request) String() string {
	return fmt.Sprintf("%s/%s", r.Repo, r.RunID)
}

// OrchestratorOptions configures the run Orchestrator.
type OrchestratorOptions struct {
	Store             state.Store
	SessionStore      session.Store
	Harness           *session.Harness
	Clock             func() time.Time
	MaxTurns          int
	MaxConcurrentRuns int
}

// Orchestrator reconciles runs in phase Pending and Working using a controller-runtime controller.
type Orchestrator struct {
	store             state.Store
	sessionStore      session.Store
	harness           *session.Harness
	clock             func() time.Time
	maxTurns          int
	maxConcurrentRuns int

	notifyCh chan event.TypedGenericEvent[Request]

	activeRunsMu sync.Mutex
	activeRuns   map[string]bool
}

var _ reconcile.TypedReconciler[Request] = (*Orchestrator)(nil)
var _ manager.Runnable = (*Orchestrator)(nil)

// NewManager builds a controller-runtime manager from rest.Config with metrics and health
// endpoints disabled and leader election disabled.
func NewManager(restConfig *rest.Config) (manager.Manager, error) {
	return ctrl.NewManager(restConfig, ctrl.Options{
		Logger: klog.NewKlogr(),
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
}

// NewOrchestrator creates a new run Orchestrator.
func NewOrchestrator(opts OrchestratorOptions) (*Orchestrator, error) {
	if opts.Store == nil {
		return nil, errors.New("state store is required")
	}
	if opts.Harness == nil {
		return nil, errors.New("harness is required")
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}

	maxTurns := opts.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 30
	}

	maxConcurrentRuns := opts.MaxConcurrentRuns
	if maxConcurrentRuns <= 0 {
		maxConcurrentRuns = 4
	}

	return &Orchestrator{
		store:             opts.Store,
		sessionStore:      opts.SessionStore,
		harness:           opts.Harness,
		clock:             clock,
		maxTurns:          maxTurns,
		maxConcurrentRuns: maxConcurrentRuns,
		notifyCh:          make(chan event.TypedGenericEvent[Request], 1024),
		activeRuns:        make(map[string]bool),
	}, nil
}

// Notify enqueues a run for reconciliation.
// If o is nil, Notify is a no-op.
func (o *Orchestrator) Notify(repo state.RepoKey, runID string) {
	if o == nil {
		return
	}
	req := Request{Repo: repo, RunID: runID}
	select {
	case o.notifyCh <- event.TypedGenericEvent[Request]{Object: req}:
	default:
		go func() {
			o.notifyCh <- event.TypedGenericEvent[Request]{Object: req}
		}()
	}
}

// SetupWithManager registers the orchestrator with the controller-runtime manager.
func (o *Orchestrator) SetupWithManager(mgr manager.Manager) error {
	// Skip name validation so unit tests can build multiple controllers named "run" in one process.
	skipNameValidation := true
	c, err := controller.NewTyped[Request]("run", mgr, controller.TypedOptions[Request]{
		Reconciler:              o,
		MaxConcurrentReconciles: o.maxConcurrentRuns,
		SkipNameValidation:      &skipNameValidation,
	})
	if err != nil {
		return fmt.Errorf("creating run controller: %w", err)
	}

	src := source.TypedChannel[Request, Request](
		o.notifyCh,
		handler.TypedEnqueueRequestsFromMapFunc(func(_ context.Context, req Request) []Request {
			return []Request{req}
		}),
	)
	if err := c.Watch(src); err != nil {
		return fmt.Errorf("watching notification channel: %w", err)
	}

	return mgr.Add(o)
}

// Start implements manager.Runnable. On manager startup it calls Resume,
// which marks interrupted Preparing runs as Failed and enqueues all non-terminal runs.
// It then blocks until ctx is cancelled.
func (o *Orchestrator) Start(ctx context.Context) error {
	if err := o.Resume(ctx); err != nil {
		scoped.Log(ctx).Error(err, "failed to resume interrupted runs on startup")
	}
	<-ctx.Done()
	return nil
}

// Resume inspects existing runs across all repositories on startup.
// Any run found in Preparing is marked Failed with an explanatory event,
// because sandboxes and clones are not yet durable across restarts.
// Every non-terminal run is enqueued for reconciliation.
func (o *Orchestrator) Resume(ctx context.Context) error {
	log := scoped.Log(ctx)
	repos, err := o.store.ListRepos(ctx)
	if err != nil {
		return fmt.Errorf("listing repositories for resume: %w", err)
	}

	now := o.clock().UTC()
	for _, repo := range repos {
		runs, err := o.store.ListRuns(ctx, repo.Key())
		if err != nil {
			log.Error(err, "failed to list runs during resume", "repo", repo.URL)
			continue
		}

		for _, r := range runs {
			if r.Phase == state.PhasePreparing {
				log.Info("marking interrupted Preparing run as Failed", "repo", repo.URL, "run", r.ID)
				r.SetPhase(state.PhaseFailed, now)
				r.Error = "server restarted while run was in Preparing phase"
				if err := o.store.PutRun(ctx, r); err != nil {
					log.Error(err, "failed to update interrupted run", "run", r.ID)
				}

				event := state.Event{
					Time:    now,
					Phase:   state.PhaseFailed,
					Kind:    "restart",
					Message: "Run failed: server restarted while preparing sandbox",
					Error:   r.Error,
				}
				if err := o.store.AppendEvent(ctx, repo.Key(), r.ID, event); err != nil {
					log.Error(err, "failed to append restart failure event", "run", r.ID)
				}

				if r.SandboxName != "" && o.harness != nil && o.harness.SandboxClient() != nil {
					if delErr := o.harness.SandboxClient().DeleteSandboxByName(ctx, o.harness.Namespace(), r.SandboxName); delErr != nil {
						log.V(2).Info("error deleting orphaned sandbox on restart", "sandbox", r.SandboxName, "error", delErr)
					}
				}
				continue
			}

			if !r.Phase.IsTerminal() {
				o.Notify(repo.Key(), r.ID)
			}
		}
	}
	return nil
}

// Reconcile loads the run and the repository, dispatches on the phase to
// prepareRun or workRun unchanged, and returns.
func (o *Orchestrator) Reconcile(ctx context.Context, req Request) (reconcile.Result, error) {
	log := scoped.Log(ctx)

	run, err := o.store.GetRun(ctx, req.Repo, req.RunID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting run %s: %w", req.RunID, err)
	}

	repo, err := o.store.GetRepo(ctx, req.Repo)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("getting repo %s: %w", req.Repo, err)
	}

	switch run.Phase {
	case state.PhasePending:
		if err := o.prepareRun(ctx, repo, run); err != nil {
			log.Error(err, "preparing run failed", "repo", repo.URL, "run", run.ID)
			// prepareRun transitions to PhaseFailed on error; do not requeue terminal runs.
			return reconcile.Result{}, nil
		}
	case state.PhaseWorking:
		if o.sessionStore != nil && o.harness != nil && o.harness.LLMClient() != nil {
			if err := o.workRun(ctx, repo, run); err != nil {
				log.Error(err, "working run failed", "repo", repo.URL, "run", run.ID)
				// workRun transitions to PhaseFailed on error; do not requeue terminal runs.
				return reconcile.Result{}, nil
			}
		}
	}

	return reconcile.Result{}, nil
}

func (o *Orchestrator) prepareRun(ctx context.Context, repo *state.Repo, run *state.Run) error {
	log := scoped.Log(ctx)
	now := o.clock().UTC()

	// 1. Assign sandbox name if not already assigned
	sandboxName := run.SandboxName
	if sandboxName == "" {
		sandboxName = fmt.Sprintf("run-%s", strings.ToLower(run.ID))
		run.SandboxName = sandboxName
	}

	// 2. Transition to Preparing
	run.SetPhase(state.PhasePreparing, now)
	if err := o.store.PutRun(ctx, run); err != nil {
		return fmt.Errorf("updating run to Preparing: %w", err)
	}

	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    now,
		Phase:   state.PhasePreparing,
		Kind:    "phase",
		Message: fmt.Sprintf("Run %s entered Preparing phase", run.ID),
	}); err != nil {
		log.Error(err, "failed to append Preparing phase event", "run", run.ID)
	}

	log.Info("preparing run: creating sandbox", "repo", repo.URL, "run", run.ID, "sandbox", sandboxName)

	// 3. Create sandbox and wait for it to be ready
	sb, err := o.harness.StartSandbox(ctx, sandboxName)
	if err != nil {
		failTime := o.clock().UTC()
		run.SetPhase(state.PhaseFailed, failTime)
		run.Error = fmt.Sprintf("creating sandbox: %v", err)
		if putErr := o.store.PutRun(ctx, run); putErr != nil {
			log.Error(putErr, "failed to update run to Failed", "run", run.ID)
		}
		if evErr := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    failTime,
			Phase:   state.PhaseFailed,
			Kind:    "failed",
			Message: fmt.Sprintf("Failed to create sandbox: %v", err),
			Error:   run.Error,
		}); evErr != nil {
			log.Error(evErr, "failed to append sandbox creation failure event", "run", run.ID)
		}
		return fmt.Errorf("creating sandbox: %w", err)
	}

	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    o.clock().UTC(),
		Phase:   state.PhasePreparing,
		Kind:    "sandbox_ready",
		Message: fmt.Sprintf("Sandbox %s is ready", sandboxName),
		Data:    map[string]any{"sandbox": sandboxName},
	}); err != nil {
		log.Error(err, "failed to append sandbox_ready event", "run", run.ID)
	}

	// 4. Run git clone -- <url> /workspace/repo, then git -C /workspace/repo checkout <base commit>.
	// Avoid building a shell command from strings so external values (repo URL and commit SHA)
	// cannot inject shell syntax. Each command is executed directly and recorded as its own event.
	commands := []struct {
		desc string
		args []string
	}{
		{
			desc: fmt.Sprintf("git clone -- %s /workspace/repo", repo.URL),
			args: []string{"git", "clone", "--", repo.URL, "/workspace/repo"},
		},
		{
			desc: fmt.Sprintf("git -C /workspace/repo checkout %s", run.BaseCommit),
			args: []string{"git", "-C", "/workspace/repo", "checkout", run.BaseCommit},
		},
	}

	for _, cmd := range commands {
		log.Info("executing command inside sandbox", "run", run.ID, "cmd", cmd.desc)

		res, execErr := sb.ExecCommand(ctx, tools.ExecCommandOptions{
			Command: cmd.args,
		})

		cmdTime := o.clock().UTC()
		var cmdFailed bool
		var cmdErrMsg string

		if execErr != nil {
			cmdFailed = true
			cmdErrMsg = fmt.Sprintf("command error: %v", execErr)
		} else if res != nil && res.ExitCode != 0 {
			cmdFailed = true
			stderr := strings.TrimSpace(res.Stderr)
			if stderr != "" {
				cmdErrMsg = fmt.Sprintf("command failed with exit code %d: %s", res.ExitCode, stderr)
			} else {
				cmdErrMsg = fmt.Sprintf("command failed with exit code %d", res.ExitCode)
			}
		} else if res == nil {
			cmdFailed = true
			cmdErrMsg = "command returned nil result"
		}

		if cmdFailed {
			log.Error(errors.New(cmdErrMsg), "command failed; deleting sandbox", "run", run.ID, "sandbox", sandboxName)

			var exitCode int
			var stdout, stderr string
			if res != nil {
				exitCode = res.ExitCode
				stdout = res.Stdout
				stderr = res.Stderr
			}

			// Record command failure event
			if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
				Time:    cmdTime,
				Phase:   state.PhasePreparing,
				Kind:    "command",
				Message: cmdErrMsg,
				Error:   cmdErrMsg,
				Data: map[string]any{
					"command":   cmd.desc,
					"args":      cmd.args,
					"exit_code": exitCode,
					"stdout":    stdout,
					"stderr":    stderr,
				},
			}); err != nil {
				log.Error(err, "failed to append command failure event", "run", run.ID)
			}

			// Delete sandbox
			if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
				log.Error(delErr, "failed to delete sandbox after command failure", "sandbox", sandboxName)
			}

			// Transition run to Failed
			run.SetPhase(state.PhaseFailed, cmdTime)
			run.Error = cmdErrMsg
			if err := o.store.PutRun(ctx, run); err != nil {
				return fmt.Errorf("updating run to Failed: %w", err)
			}

			if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
				Time:    cmdTime,
				Phase:   state.PhaseFailed,
				Kind:    "phase",
				Message: fmt.Sprintf("Run %s failed: %s", run.ID, cmdErrMsg),
				Error:   cmdErrMsg,
			}); err != nil {
				log.Error(err, "failed to append Failed phase event", "run", run.ID)
			}

			return errors.New(cmdErrMsg)
		}

		// Command succeeded! Record command result event
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    cmdTime,
			Phase:   state.PhasePreparing,
			Kind:    "command",
			Message: fmt.Sprintf("Executed %s", cmd.desc),
			Data: map[string]any{
				"command":   cmd.desc,
				"args":      cmd.args,
				"exit_code": 0,
				"stdout":    res.Stdout,
				"stderr":    res.Stderr,
			},
		}); err != nil {
			log.Error(err, "failed to append command success event", "run", run.ID)
		}
	}

	// 5. Transition to Working phase (sandbox left alive).
	// Note: While the run is in Working phase awaiting the session (issue #96),
	// nothing currently extends the sandbox's lifetime, so shutdownTime (ShutdownPolicy: Delete)
	// will delete it after the inactivity window (SandboxInactivityTimeout, default 5m) if
	// the session does not start immediately. In issue #96, the session begins immediately
	// upon entering Working or periodically extends the sandbox's TTL.
	workTime := o.clock().UTC()
	run.SetPhase(state.PhaseWorking, workTime)
	if err := o.store.PutRun(ctx, run); err != nil {
		return fmt.Errorf("updating run to Working: %w", err)
	}

	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
		Time:    workTime,
		Phase:   state.PhaseWorking,
		Kind:    "phase",
		Message: fmt.Sprintf("Run %s entered Working phase", run.ID),
	}); err != nil {
		log.Error(err, "failed to append Working phase event", "run", run.ID)
	}

	log.Info("run successfully prepared and entered Working phase", "run", run.ID, "sandbox", sandboxName)

	o.Notify(repo.Key(), run.ID)
	return nil
}

func (o *Orchestrator) setRunActive(runID string, active bool) bool {
	o.activeRunsMu.Lock()
	defer o.activeRunsMu.Unlock()
	if active {
		if o.activeRuns[runID] {
			return false
		}
		o.activeRuns[runID] = true
		return true
	}
	delete(o.activeRuns, runID)
	return true
}

func (o *Orchestrator) failRunWithEvent(ctx context.Context, repo *state.Repo, run *state.Run, errMsg string) error {
	if ctx.Err() != nil {
		return nil
	}

	failTime := o.clock().UTC()
	run.SetPhase(state.PhaseFailed, failTime)
	run.Error = errMsg
	if err := o.store.PutRun(ctx, run); err != nil {
		scoped.Log(ctx).Error(err, "failed to update run to Failed", "run", run.ID)
	}

	event := state.Event{
		Time:    failTime,
		Phase:   state.PhaseFailed,
		Kind:    "failed",
		Message: fmt.Sprintf("Run failed: %s", errMsg),
		Error:   errMsg,
	}
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, event); err != nil {
		scoped.Log(ctx).Error(err, "failed to append failure event", "run", run.ID)
	}

	return errors.New(errMsg)
}

func (o *Orchestrator) finishRunNoImprovement(ctx context.Context, repo *state.Repo, run *state.Run, sb *session.Sandbox, kind, message string) error {
	endTime := o.clock().UTC()
	run.SetPhase(state.PhaseNoImprovement, endTime)
	if err := o.store.PutRun(ctx, run); err != nil {
		scoped.Log(ctx).Error(err, "failed to update run to NoImprovement", "run", run.ID)
	}

	event := state.Event{
		Time:    endTime,
		Phase:   state.PhaseNoImprovement,
		Kind:    kind,
		Message: message,
	}
	if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, event); err != nil {
		scoped.Log(ctx).Error(err, "failed to append event", "kind", kind, "run", run.ID)
	}

	if delErr := o.harness.DeleteSandbox(context.WithoutCancel(ctx), sb); delErr != nil {
		scoped.Log(ctx).V(2).Info("error deleting sandbox after session", "sandbox", run.SandboxName, "error", delErr)
	}
	return nil
}

func (o *Orchestrator) workRun(ctx context.Context, repo *state.Repo, run *state.Run) error {
	log := scoped.Log(ctx)

	if ctx.Err() != nil {
		return nil
	}

	if !o.setRunActive(run.ID, true) {
		log.V(2).Info("run is already actively executing a session", "run", run.ID)
		return nil
	}
	defer o.setRunActive(run.ID, false)

	sandboxName := run.SandboxName
	if sandboxName == "" {
		return o.failRunWithEvent(ctx, repo, run, "sandbox name is empty for working run")
	}

	sb := o.harness.GetOrAttachSandbox(sandboxName)
	if sb == nil {
		return o.failRunWithEvent(ctx, repo, run, "failed to get sandbox handle")
	}

	// Verify sandbox exists in Kubernetes and has not expired.
	// If expired or deleted, fail the run with a clear event.
	if err := sb.CheckAliveAndExtendLifecycle(ctx, o.clock().UTC(), session.SandboxInactivityTimeout); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("sandbox %s expired or deleted: %v", sandboxName, err))
	}

	// Assign session ID if not already assigned and persist in run.json
	sessionID := run.SessionID
	if sessionID == "" {
		sessionID = generateSessionID(run.ID)
		run.SessionID = sessionID
		if err := o.store.PutRun(ctx, run); err != nil {
			return fmt.Errorf("updating run with session ID: %w", err)
		}
		if err := o.store.AppendEvent(ctx, repo.Key(), run.ID, state.Event{
			Time:    o.clock().UTC(),
			Phase:   state.PhaseWorking,
			Kind:    "session_started",
			Message: fmt.Sprintf("Started session %s", sessionID),
			Data:    map[string]any{"session_id": sessionID},
		}); err != nil {
			log.Error(err, "failed to append session_started event", "run", run.ID)
		}
	}

	goalBody := ""
	if run.Goal != nil {
		goalBody = run.Goal.Body
	} else if run.GoalName != "" {
		goalRec, err := o.store.GetGoal(ctx, repo.Key(), run.GoalName)
		if err == nil && goalRec != nil && goalRec.Goal != nil {
			goalBody = goalRec.Goal.Body
		}
	}

	sess, err := o.harness.LoadSession(ctx, o.sessionStore, sessionID)
	if err != nil {
		if errors.Is(err, session.ErrNotFound) {
			sess, err = o.harness.NewSession(ctx, o.sessionStore, sessionID, session.Metadata{
				CreatedAt: o.clock().UTC(),
			})
			if err != nil {
				return fmt.Errorf("creating session %s: %w", sessionID, err)
			}
		} else {
			return fmt.Errorf("loading session %s: %w", sessionID, err)
		}
	}

	// Bind sandbox to session
	sess.SetActiveSandbox(sb)
	sess.FailOnSandboxExpiry = true

	// Seed system prompt if new session
	if len(sess.Messages()) == 0 {
		sysPrompt := buildSystemPrompt(o.harness.RunOptions().SystemPrompt, repo, run, goalBody)
		if err := sess.AddMessages(ctx, llm.Message{
			Role:    "system",
			Content: &sysPrompt,
		}); err != nil {
			return fmt.Errorf("seeding system prompt: %w", err)
		}
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var heartbeatErr error
	var heartbeatMu sync.Mutex

	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)

	go func() {
		ticker := time.NewTicker(session.SandboxInactivityTimeout / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				if err := sb.CheckAliveAndExtendLifecycle(sessionCtx, o.clock().UTC(), session.SandboxInactivityTimeout); err != nil {
					heartbeatMu.Lock()
					heartbeatErr = err
					heartbeatMu.Unlock()
					cancel()
					return
				}
			}
		}
	}()

	events := &orchestratorTurnEvents{}
	finishTool := ao.NewFinishTool()
	harness := o.harness.WithMaxTurns(o.maxTurns).WithExtraTools(finishTool)
	turnErr := harness.RunTurn(sessionCtx, sess, "Please work on the goal.", events)

	if errors.Is(turnErr, session.ErrSessionFinished) {
		run.Summary = finishTool.Summary
		return o.finishRunNoImprovement(ctx, repo, run, sb, "finished", finishTool.Summary)
	}

	if turnErr == nil || errors.Is(turnErr, session.ErrMaxTurns) {
		return o.finishRunNoImprovement(ctx, repo, run, sb, "no_improvement", "session ended without a pull request")
	}

	if ctx.Err() != nil {
		return nil
	}

	heartbeatMu.Lock()
	hbErr := heartbeatErr
	heartbeatMu.Unlock()
	if errors.Is(turnErr, session.ErrSandboxExpired) || errors.Is(hbErr, session.ErrSandboxExpired) {
		return o.failRunWithEvent(ctx, repo, run, "sandbox expired or was deleted during session")
	}

	return o.failRunWithEvent(ctx, repo, run, fmt.Sprintf("session error: %v", turnErr))
}

func buildSystemPrompt(basePrompt string, repo *state.Repo, run *state.Run, goalBody string) string {
	var sb strings.Builder
	if basePrompt != "" {
		sb.WriteString(basePrompt)
		sb.WriteString("\n\n")
	}

	sb.WriteString("# Goal\n\n")
	sb.WriteString(strings.TrimSpace(goalBody))
	sb.WriteString("\n\n")

	sb.WriteString("# Repository Facts\n\n")
	if repo != nil {
		sb.WriteString(fmt.Sprintf("- URL: %s\n", repo.URL))
		if repo.DefaultBranch != "" {
			sb.WriteString(fmt.Sprintf("- Default Branch: %s\n", repo.DefaultBranch))
		}
	}
	if run != nil && run.BaseCommit != "" {
		sb.WriteString(fmt.Sprintf("- Base Commit: %s\n", run.BaseCommit))
	}
	sb.WriteString("- Working Directory: /workspace/repo\n\n")

	sb.WriteString("# Rules\n\n")
	sb.WriteString("- Never edit .goals/ or the metric script.\n")
	sb.WriteString("- Keep changes focused.\n")
	sb.WriteString("- Call the finish tool with a summary written for maintainers when you have nothing more to do.\n")

	return sb.String()
}

func generateSessionID(runID string) string {
	var sb strings.Builder
	sb.WriteString("run")
	for _, r := range strings.ToLower(runID) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		}
	}
	id := sb.String()
	if len(id) > 40 {
		id = id[:40]
	}
	return id
}

type orchestratorTurnEvents struct{}

func (e *orchestratorTurnEvents) AssistantMessage(_ context.Context, _ string) {}

func (e *orchestratorTurnEvents) ApproveToolCall(_ context.Context, _ llm.ToolCall) (bool, error) {
	return true, nil
}

func (e *orchestratorTurnEvents) ToolCallStarted(ctx context.Context, call llm.ToolCall) {
	scoped.Log(ctx).V(1).Info("tool call started", "tool", call.Function.Name)
}

func (e *orchestratorTurnEvents) ToolCallFinished(ctx context.Context, call llm.ToolCall, err error) {
	scoped.Log(ctx).V(1).Info("tool call finished", "tool", call.Function.Name, "error", err)
}

func (e *orchestratorTurnEvents) MessageAppended(_ context.Context, _ llm.Message) {}
