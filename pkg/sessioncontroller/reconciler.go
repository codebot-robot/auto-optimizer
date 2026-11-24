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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets/ao"
)

const (
	defaultMaxConcurrent = 4
	cancelRequeueDelay   = time.Second
)

// Options configures a Session Reconciler.
type Options struct {
	Client          client.Client
	SessionStore    session.Store
	HarnessFactory  func(toolset string) (*session.Harness, error)
	SandboxOwnerFor func(ctx context.Context, obj *herorunv1alpha1.Session) (*session.SandboxOwner, error)
	DefaultToolset  string
	DefaultMaxTurns int
	MaxConcurrent   int
	Clock           func() time.Time
	EstimateCost    func(model string, messages []llm.Message) float64
}

// Reconciler watches Session custom resources and drives each session's LLM loop.
type Reconciler struct {
	opts Options

	runBudgetsMu sync.Mutex
	runBudgets   map[string]*session.TurnBudget
	msgSeq       atomic.Uint64
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
	return &Reconciler{
		opts:       opts,
		runBudgets: make(map[string]*session.TurnBudget),
	}, nil
}

// SetupWithManager registers the Session reconciler with mgr.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&herorunv1alpha1.Session{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&herorunv1alpha1.SessionMessage{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.opts.MaxConcurrent}).
		Complete(r)
}

// Reconcile drives a Session resource from Pending through Running to a terminal status.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var obj herorunv1alpha1.Session
	if err := r.opts.Client.Get(ctx, req.NamespacedName, &obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if obj.Status.Phase.IsTerminal() {
		r.releaseRunBudgetIfRoot(&obj)
		return reconcile.Result{}, nil
	}
	if ctx.Err() != nil {
		return reconcile.Result{}, nil
	}
	if r.deadlinePassed(&obj) {
		err := r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
			st.Phase, st.Outcome, st.Error = herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeDeadlineExceeded, "deadline exceeded"
			st.FinishedAt = &now
		})
		if err == nil {
			r.releaseRunBudgetIfRoot(&obj)
		}
		return reconcile.Result{}, err
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

	applyTerminal, canceled := r.runSession(ctx, &obj)
	if ctx.Err() != nil {
		return reconcile.Result{}, nil
	}
	if canceled {
		delay := cancelRequeueDelay
		if obj.Spec.Deadline != nil {
			if rem := obj.Spec.Deadline.Time.Sub(r.opts.Clock().UTC()); rem < delay {
				delay = max(rem, time.Millisecond)
			}
		}
		return reconcile.Result{RequeueAfter: delay}, nil
	}
	err := r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
		applyTerminal(st)
		st.FinishedAt = &now
	})
	if err == nil {
		r.releaseRunBudgetIfRoot(&obj)
	}
	return reconcile.Result{}, err
}

func (r *Reconciler) runSession(ctx context.Context, obj *herorunv1alpha1.Session) (func(*herorunv1alpha1.SessionStatus), bool) {
	fail := func(err error) (func(*herorunv1alpha1.SessionStatus), bool) {
		deadlineExceeded := errors.Is(err, context.DeadlineExceeded) || r.deadlinePassed(obj)
		if !deadlineExceeded && isCanceled(err) {
			scoped.Log(ctx).V(1).Info("session interrupted by cancellation; will resume", "session", obj.Name, "error", err)
			return nil, true
		}
		return func(st *herorunv1alpha1.SessionStatus) { applyOutcome(st, err, nil, nil, deadlineExceeded) }, false
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
			Repo:      normalizeRepoMetadata(ctx, obj.Spec.Repository),
			RunID:     obj.Spec.RunID,
			CreatedAt: r.opts.Clock().UTC(),
		})
	}
	if err != nil {
		return fail(err)
	}
	if err := r.attachSandbox(ctx, obj, harness, sess); err != nil {
		return fail(err)
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
	if obj.Spec.RunID != "" {
		extra = append(extra, ao.NewSendMessageTool(func(toolCtx context.Context, recipient, message string) error {
			return r.sendMessage(toolCtx, obj, recipient, message)
		}))
		harness = harness.WithMessageSource(func(msgCtx context.Context, s *session.Session) error {
			return r.deliverPendingMessages(msgCtx, obj, s)
		})
	}
	harness = harness.WithExtraTools(extra...)
	maxTurns := cmp.Or(obj.Spec.MaxTurns, r.opts.DefaultMaxTurns, harness.RunOptions().MaxTurns)
	budget, err := r.turnBudgetFor(ctx, obj, sess, maxTurns)
	if err != nil {
		return fail(err)
	}
	if budget != nil {
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
	firstTurn := true
	for {
		if firstTurn {
			firstTurn = false
			if slices.ContainsFunc(sess.Messages(), func(m llm.Message) bool { return m.Role == "user" }) || obj.Spec.InitialPrompt == "" {
				err = harness.ContinueTurn(turnCtx, sess, events)
			} else {
				err = harness.RunTurn(turnCtx, sess, obj.Spec.InitialPrompt, events)
			}
		} else {
			err = harness.ContinueTurn(turnCtx, sess, events)
		}
		if err != nil || obj.Spec.RunID == "" {
			break
		}
		pending, listErr := r.listPendingMessages(turnCtx, obj)
		if listErr != nil {
			err = listErr
			break
		}
		if len(pending) == 0 {
			break
		}
		if r.deadlinePassed(obj) {
			err = context.DeadlineExceeded
			break
		}
	}
	deadlineExceeded := err != nil && (errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(turnCtx.Err(), context.DeadlineExceeded) ||
		r.deadlinePassed(obj))
	if err != nil && !deadlineExceeded && isCanceled(err) {
		scoped.Log(ctx).V(1).Info("session turn interrupted by cancellation; will resume", "session", obj.Name, "error", err)
		return nil, true
	}
	return func(st *herorunv1alpha1.SessionStatus) {
		r.recordMetrics(st, sess, model)
		applyOutcome(st, err, prTool, finishTool, deadlineExceeded)
	}, false
}

func (r *Reconciler) deliverPendingMessages(ctx context.Context, obj *herorunv1alpha1.Session, sess *session.Session) error {
	pending, err := r.listPendingMessages(ctx, obj)
	if err != nil {
		return err
	}
	for i := range pending {
		text := formatSessionMessage(pending[i].Spec)
		if err := sess.AddMessages(ctx, llm.Message{Role: "user", Content: &text}); err != nil {
			return fmt.Errorf("adding pending message %s to session: %w", pending[i].Name, err)
		}
		if err := r.markMessageDelivered(ctx, &pending[i]); err != nil {
			return fmt.Errorf("marking session message %s delivered: %w", pending[i].Name, err)
		}
	}
	return nil
}

func (r *Reconciler) listPendingMessages(ctx context.Context, obj *herorunv1alpha1.Session) ([]herorunv1alpha1.SessionMessage, error) {
	var list herorunv1alpha1.SessionMessageList
	if err := r.opts.Client.List(ctx, &list,
		client.InNamespace(obj.Namespace),
		client.MatchingLabels{herorunv1alpha1.LabelSession: obj.Name},
	); err != nil {
		return nil, fmt.Errorf("listing session messages: %w", err)
	}
	var pending []herorunv1alpha1.SessionMessage
	for i := range list.Items {
		msg := list.Items[i]
		if msg.IsDelivered() || msg.DeletionTimestamp != nil {
			continue
		}
		if msg.Spec.SessionName != "" && msg.Spec.SessionName != obj.Name {
			continue
		}
		pending = append(pending, msg)
	}
	slices.SortFunc(pending, func(a, b herorunv1alpha1.SessionMessage) int {
		if c := a.CreationTimestamp.Time.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return pending, nil
}

func (r *Reconciler) markMessageDelivered(ctx context.Context, msg *herorunv1alpha1.SessionMessage) error {
	key := client.ObjectKeyFromObject(msg)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current herorunv1alpha1.SessionMessage
		if err := r.opts.Client.Get(ctx, key, &current); err != nil {
			if k8serrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if current.IsDelivered() {
			return nil
		}
		now := metav1.NewTime(r.opts.Clock().UTC())
		current.Status.DeliveredAt = &now
		return r.opts.Client.Status().Update(ctx, &current)
	})
}

func formatSessionMessage(spec herorunv1alpha1.SessionMessageSpec) string {
	if spec.From != "" {
		return fmt.Sprintf("Message from %s: %s", spec.From, spec.Text)
	}
	return spec.Text
}

func (r *Reconciler) sendMessage(ctx context.Context, sender *herorunv1alpha1.Session, recipient, message string) error {
	if sender.Spec.RunID == "" {
		return errors.New("cannot call send_message: session belongs to no run")
	}
	if recipient == sender.Name {
		return errors.New("cannot send message to self")
	}
	var target herorunv1alpha1.Session
	if err := r.opts.Client.Get(ctx, client.ObjectKey{Namespace: sender.Namespace, Name: recipient}, &target); err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("recipient session %q not found", recipient)
		}
		return fmt.Errorf("getting recipient session %q: %w", recipient, err)
	}
	if target.Spec.RunID != sender.Spec.RunID {
		return fmt.Errorf("cannot send message to session %q: not in the same run", recipient)
	}
	if target.Status.Phase.IsTerminal() || target.DeletionTimestamp != nil {
		return fmt.Errorf("recipient session %q is not running (phase %s)", recipient, target.Status.Phase)
	}

	name, err := r.newSessionMessageName(target.Name)
	if err != nil {
		return err
	}
	spec := herorunv1alpha1.SessionMessageSpec{
		SessionName: target.Name,
		RunID:       target.Spec.RunID,
		From:        sender.Name,
		Text:        message,
	}
	msg := &herorunv1alpha1.SessionMessage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: target.Namespace,
			Labels:    spec.Labels(),
		},
		Spec: spec,
	}
	if err := controllerutil.SetControllerReference(&target, msg, r.opts.Client.Scheme()); err != nil {
		return fmt.Errorf("setting owner reference on session message: %w", err)
	}
	if err := r.opts.Client.Create(ctx, msg); err != nil {
		return fmt.Errorf("creating session message: %w", err)
	}
	return nil
}

func (r *Reconciler) newSessionMessageName(targetName string) (string, error) {
	var suffix [2]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generating session message name: %w", err)
	}
	ts := max(r.opts.Clock().UTC().UnixNano(), 0)
	seq := r.msgSeq.Add(1)
	return fmt.Sprintf("%s-msg-%016x-%06x-%s", targetName, ts, seq, hex.EncodeToString(suffix[:])), nil
}

func (r *Reconciler) attachSandbox(ctx context.Context, obj *herorunv1alpha1.Session, harness *session.Harness, sess *session.Session) error {
	if obj.Spec.RunID != "" && r.opts.SandboxOwnerFor != nil {
		owner, err := r.opts.SandboxOwnerFor(ctx, obj)
		if err != nil {
			return err
		}
		if owner != nil {
			sess.SetSandboxOwner(owner)
			return nil
		}
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
	return nil
}

func (r *Reconciler) turnBudgetFor(ctx context.Context, obj *herorunv1alpha1.Session, sess *session.Session, maxTurns int) (*session.TurnBudget, error) {
	if obj.Spec.RunID == "" {
		return seedTurnBudget(maxTurns, countTurns(sess.Messages())), nil
	}

	r.runBudgetsMu.Lock()
	existing := r.runBudgets[obj.Spec.RunID]
	r.runBudgetsMu.Unlock()
	if existing != nil || maxTurns <= 0 {
		return existing, nil
	}

	used, err := r.countRunTurns(ctx, obj.Spec.RunID, sess)
	if err != nil {
		return nil, err
	}
	budget := seedTurnBudget(maxTurns, used)

	r.runBudgetsMu.Lock()
	defer r.runBudgetsMu.Unlock()
	if existing := r.runBudgets[obj.Spec.RunID]; existing != nil {
		return existing, nil
	}
	r.runBudgets[obj.Spec.RunID] = budget
	return budget, nil
}

func seedTurnBudget(maxTurns, used int) *session.TurnBudget {
	if maxTurns <= 0 {
		return nil
	}
	budget := session.NewTurnBudget(maxTurns)
	for range used {
		budget.Take()
	}
	return budget
}

func (r *Reconciler) countRunTurns(ctx context.Context, runID string, current *session.Session) (int, error) {
	summaries, err := session.ListRunSessions(ctx, r.opts.SessionStore, runID)
	if err != nil {
		return 0, err
	}
	total := countTurns(current.Messages())
	for _, sum := range summaries {
		if sum.Name == current.Name {
			continue
		}
		msgs, _, err := r.opts.SessionStore.LoadSession(ctx, sum.Name)
		if err != nil && !errors.Is(err, session.ErrNotFound) {
			return 0, err
		}
		total += countTurns(msgs)
	}
	return total, nil
}

func (r *Reconciler) releaseRunBudgetIfRoot(obj *herorunv1alpha1.Session) {
	if obj.Spec.RunID == "" || obj.Spec.Depth() != 0 {
		return
	}
	r.runBudgetsMu.Lock()
	delete(r.runBudgets, obj.Spec.RunID)
	r.runBudgetsMu.Unlock()
}

func (r *Reconciler) deadlinePassed(obj *herorunv1alpha1.Session) bool {
	return obj.Spec.Deadline != nil && !r.opts.Clock().UTC().Before(obj.Spec.Deadline.Time)
}

func normalizeRepoMetadata(ctx context.Context, repo string) string {
	if repo == "" {
		return ""
	}
	key, err := state.ParseRepoURL(repo)
	if err != nil {
		scoped.Log(ctx).V(1).Info("failed to normalize session repository metadata", "repository", repo, "error", err)
		return repo
	}
	return key.String()
}

func isCanceled(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	var se interface{ GRPCStatus() *status.Status }
	return errors.As(err, &se) && se.GRPCStatus().Code() == codes.Canceled
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
