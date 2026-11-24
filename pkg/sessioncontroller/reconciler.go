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

// Package sessioncontroller implements the controller-runtime reconciler for Session resources.
package sessioncontroller

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets/ao"
)

const defaultMaxConcurrent = 4

// Options configures a Session Reconciler.
type Options struct {
	Client          client.Client
	SessionStore    session.Store
	HarnessFactory  func(toolset string) (*session.Harness, error)
	DefaultToolset  string
	DefaultMaxTurns int
	MaxConcurrent   int
	Clock           func() time.Time
	EstimateCost    func(model string, messages []llm.Message) float64
}

// Reconciler watches Session custom resources and drives each session's LLM loop.
type Reconciler struct {
	opts Options
}

// NewReconciler validates opts and constructs a Session Reconciler.
func NewReconciler(opts Options) (*Reconciler, error) {
	if opts.Client == nil || opts.SessionStore == nil || opts.HarnessFactory == nil {
		return nil, errors.New("client, session store, and harness factory are required")
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = defaultMaxConcurrent
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Reconciler{opts: opts}, nil
}

// SetupWithManager registers the Session reconciler with mgr.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&herorunv1alpha1.Session{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.opts.MaxConcurrent}).
		Complete(r)
}

// Reconcile drives a Session resource from Pending through Running to a terminal status.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var obj herorunv1alpha1.Session
	if err := r.opts.Client.Get(ctx, req.NamespacedName, &obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if obj.Status.Phase.IsTerminal() || ctx.Err() != nil {
		return reconcile.Result{}, nil
	}
	if obj.Spec.Deadline != nil && !r.opts.Clock().UTC().Before(obj.Spec.Deadline.Time) {
		return reconcile.Result{}, r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
			st.Phase, st.Outcome, st.Error = herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeDeadlineExceeded, "deadline exceeded"
			st.FinishedAt = &now
		})
	}
	if obj.Status.Phase != herorunv1alpha1.SessionPhaseRunning || obj.Status.StartedAt == nil {
		if err := r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
			st.Phase = herorunv1alpha1.SessionPhaseRunning
			if st.StartedAt == nil {
				st.StartedAt = &now
			}
		}); err != nil {
			return reconcile.Result{}, err
		}
	}

	applyTerminal := r.runSession(ctx, &obj)
	if ctx.Err() != nil {
		return reconcile.Result{}, nil
	}
	return reconcile.Result{}, r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
		applyTerminal(st)
		st.FinishedAt = &now
	})
}

func (r *Reconciler) runSession(ctx context.Context, obj *herorunv1alpha1.Session) func(*herorunv1alpha1.SessionStatus) {
	fail := func(err error) func(*herorunv1alpha1.SessionStatus) {
		return func(st *herorunv1alpha1.SessionStatus) { applyOutcome(st, err, nil, nil, false) }
	}
	toolset := cmp.Or(obj.Spec.Toolset, r.opts.DefaultToolset)
	harness, err := r.opts.HarnessFactory(toolset)
	if err != nil {
		return fail(err)
	}
	if obj.Spec.Model != "" {
		harness = harness.WithModel(obj.Spec.Model)
	}
	sess, err := harness.LoadSession(ctx, r.opts.SessionStore, obj.Name)
	if errors.Is(err, session.ErrNotFound) {
		sess, err = harness.NewSession(ctx, r.opts.SessionStore, obj.Name, session.Metadata{
			Ancestors: slices.Clone(obj.Spec.Ancestors),
			Parent:    obj.Spec.Parent,
			Toolset:   toolset,
			Repo:      obj.Spec.Repository,
			RunID:     obj.Spec.RunID,
			CreatedAt: r.opts.Clock().UTC(),
		})
	}
	if err != nil {
		return fail(err)
	}
	if obj.Spec.WorkspaceClaimName != "" {
		sess.SetCreateSandboxOptions(session.CreateSandboxOptions{
			Name:               cmp.Or(obj.Spec.SandboxName, obj.Name),
			WorkspaceClaimName: obj.Spec.WorkspaceClaimName,
		})
	}
	if obj.Spec.SandboxName != "" {
		if sb := harness.GetOrAttachSandbox(obj.Spec.SandboxName); sb != nil {
			sess.SetActiveSandbox(sb)
		}
	}
	if len(sess.Messages()) == 0 {
		if obj.Spec.SystemPrompt != "" {
			p := obj.Spec.SystemPrompt
			err = sess.AddMessages(ctx, llm.Message{Role: "system", Content: &p})
		} else {
			err = harness.EnsureSystemPrompt(ctx, sess)
		}
		if err != nil {
			return fail(err)
		}
	}

	finishTool := ao.NewFinishTool()
	extra := []tools.Tool{finishTool}
	var prTool *ao.OpenPullRequestTool
	if obj.Spec.AllowPullRequest {
		prTool = ao.NewOpenPullRequestTool(ao.OpenPullRequestExpectation{Repository: obj.Spec.Repository, Branch: obj.Spec.Branch})
		extra = append(extra, prTool)
	}
	harness = harness.WithExtraTools(extra...)
	if maxTurns := cmp.Or(obj.Spec.MaxTurns, r.opts.DefaultMaxTurns, harness.RunOptions().MaxTurns); maxTurns > 0 {
		budget := session.NewTurnBudget(maxTurns)
		for range countTurns(sess.Messages()) {
			budget.Take()
		}
		harness = harness.WithTurnBudget(budget)
	}

	turnCtx := ctx
	if obj.Spec.Deadline != nil {
		var cancel context.CancelFunc
		turnCtx, cancel = context.WithTimeout(ctx, obj.Spec.Deadline.Time.Sub(r.opts.Clock().UTC()))
		defer cancel()
	}

	model := harness.RunOptions().ModelName
	events := turnEvents(func(evCtx context.Context) {
		if ctx.Err() != nil {
			return
		}
		if err := r.updateStatus(ctx, obj, func(st *herorunv1alpha1.SessionStatus, _ metav1.Time) {
			r.recordMetrics(st, sess, model)
		}); err != nil {
			scoped.Log(evCtx).V(1).Info("failed to update Session progress status", "session", obj.Name, "error", err)
		}
	})
	if slices.ContainsFunc(sess.Messages(), func(m llm.Message) bool { return m.Role == "user" }) || obj.Spec.InitialPrompt == "" {
		err = harness.ContinueTurn(turnCtx, sess, events)
	} else {
		err = harness.RunTurn(turnCtx, sess, obj.Spec.InitialPrompt, events)
	}
	deadlineExceeded := err != nil && (errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(turnCtx.Err(), context.DeadlineExceeded) ||
		(obj.Spec.Deadline != nil && !r.opts.Clock().UTC().Before(obj.Spec.Deadline.Time)))
	return func(st *herorunv1alpha1.SessionStatus) {
		r.recordMetrics(st, sess, model)
		applyOutcome(st, err, prTool, finishTool, deadlineExceeded)
	}
}

func (r *Reconciler) recordMetrics(st *herorunv1alpha1.SessionStatus, sess *session.Session, model string) {
	msgs := sess.Messages()
	st.Turns, st.MessageCount = countTurns(msgs), len(msgs)
	if r.opts.EstimateCost != nil {
		st.CostUSD = r.opts.EstimateCost(model, msgs)
	}
}

func (r *Reconciler) updateStatus(ctx context.Context, obj *herorunv1alpha1.Session, mutate func(*herorunv1alpha1.SessionStatus, metav1.Time)) error {
	key := client.ObjectKeyFromObject(obj)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := r.opts.Client.Get(ctx, key, obj); err != nil {
			return err
		}
		if obj.Status.Phase.IsTerminal() {
			return nil
		}
		now := metav1.NewTime(r.opts.Clock().UTC())
		mutate(&obj.Status, now)
		obj.Status.ObservedAt = &now
		return r.opts.Client.Status().Update(ctx, obj)
	})
}

func applyOutcome(st *herorunv1alpha1.SessionStatus, turnErr error, prTool *ao.OpenPullRequestTool, finishTool *ao.FinishTool, deadlineExceeded bool) {
	switch {
	case errors.Is(turnErr, session.ErrPullRequestRequested):
		st.Phase, st.Outcome = herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomePullRequest
		if prTool != nil {
			st.PRTitle, st.PRBody = prTool.Title, prTool.Body
		}
	case errors.Is(turnErr, session.ErrSessionFinished):
		st.Phase, st.Outcome = herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomeFinished
		if finishTool != nil {
			st.Summary = finishTool.Summary
		}
	case turnErr == nil:
		st.Phase, st.Outcome = herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomeCompleted
	case errors.Is(turnErr, session.ErrMaxTurns):
		st.Phase, st.Outcome = herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomeMaxTurns
	case deadlineExceeded:
		st.Phase, st.Outcome, st.Error = herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeDeadlineExceeded, "deadline exceeded"
	default:
		st.Phase, st.Outcome, st.Error = herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeFailed, turnErr.Error()
	}
}

func countTurns(msgs []llm.Message) int {
	n := 0
	for _, m := range msgs {
		if m.Role == "assistant" {
			n++
		}
	}
	return n
}

type turnEvents func(context.Context)

func (turnEvents) AssistantMessage(context.Context, string)                    {}
func (turnEvents) ApproveToolCall(context.Context, llm.ToolCall) (bool, error) { return true, nil }
func (turnEvents) ToolCallStarted(context.Context, llm.ToolCall)               {}
func (turnEvents) ToolCallFinished(context.Context, llm.ToolCall, error)       {}
func (f turnEvents) MessageAppended(ctx context.Context, msg llm.Message) {
	if msg.Role == "assistant" {
		f(ctx)
	}
}
