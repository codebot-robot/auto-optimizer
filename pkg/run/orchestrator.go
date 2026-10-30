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
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

// OrchestratorOptions configures the run Orchestrator.
type OrchestratorOptions struct {
	Store    state.Store
	Harness  *session.Harness
	Interval time.Duration
	Clock    func() time.Time
}

// Orchestrator picks runs in phase Pending and drives them through Preparing.
type Orchestrator struct {
	store    state.Store
	harness  *session.Harness
	interval time.Duration
	clock    func() time.Time
}

// NewOrchestrator creates a new run Orchestrator.
func NewOrchestrator(opts OrchestratorOptions) (*Orchestrator, error) {
	if opts.Store == nil {
		return nil, errors.New("state store is required")
	}
	if opts.Harness == nil {
		return nil, errors.New("harness is required")
	}

	interval := opts.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}

	return &Orchestrator{
		store:    opts.Store,
		harness:  opts.Harness,
		interval: interval,
		clock:    clock,
	}, nil
}

// Resume inspects existing runs across all repositories on startup.
// Any run found in Preparing is marked Failed with an explanatory event,
// because sandboxes and clones are not yet durable across restarts.
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
			if r.Phase != state.PhasePreparing {
				continue
			}

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
		}
	}
	return nil
}

// Run runs the orchestrator loop until the context is cancelled.
func (o *Orchestrator) Run(ctx context.Context) error {
	log := scoped.Log(ctx)
	log.Info("starting run orchestrator", "interval", o.interval)

	if err := o.Resume(ctx); err != nil {
		log.Error(err, "failed to resume interrupted runs on startup")
	}

	if err := o.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error(err, "initial orchestrator tick failed")
	}

	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := o.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Error(err, "orchestrator tick failed")
			}
		}
	}
}

// Tick performs one orchestration pass over all registered repositories.
func (o *Orchestrator) Tick(ctx context.Context) error {
	repos, err := o.store.ListRepos(ctx)
	if err != nil {
		return fmt.Errorf("listing repositories: %w", err)
	}

	var errs []error
	for _, repo := range repos {
		if err := o.tickRepo(ctx, repo); err != nil {
			errs = append(errs, fmt.Errorf("repo %s: %w", repo.URL, err))
		}
	}

	return errors.Join(errs...)
}

func (o *Orchestrator) tickRepo(ctx context.Context, repo *state.Repo) error {
	runs, err := o.store.ListRuns(ctx, repo.Key())
	if err != nil {
		return fmt.Errorf("listing runs: %w", err)
	}

	for _, run := range runs {
		if run.Phase != state.PhasePending {
			continue
		}

		if err := o.prepareRun(ctx, repo, run); err != nil {
			scoped.Log(ctx).Error(err, "preparing run failed", "repo", repo.URL, "run", run.ID)
		}
	}

	return nil
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
	return nil
}
