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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
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
	defaultMaxDepth      = 3
	cancelRequeueDelay   = time.Second
)

// Options configures a Session Reconciler.
type Options struct {
	Client          client.Client
	SessionStore    session.Store
	StateStore      state.Store
	HarnessFactory  func(toolset string) (*session.Harness, error)
	SandboxOwnerFor func(ctx context.Context, obj *herorunv1alpha1.Session) (*session.SandboxOwner, error)
	DefaultToolset  string
	DefaultMaxTurns int
	DefaultMaxDepth int
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

	handlesMu sync.Mutex
	active    map[types.NamespacedName]*context.CancelFunc
	waiters   map[types.NamespacedName]chan struct{}
}

// NewReconciler validates opts and constructs a Session Reconciler.
func NewReconciler(opts Options) (*Reconciler, error) {
	if opts.Client == nil || opts.SessionStore == nil || opts.HarnessFactory == nil {
		return nil, errors.New("client, session store, and harness factory are required")
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = defaultMaxConcurrent
	}
	if opts.DefaultMaxDepth <= 0 {
		opts.DefaultMaxDepth = defaultMaxDepth
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Reconciler{
		opts:       opts,
		runBudgets: make(map[string]*session.TurnBudget),
		active:     make(map[types.NamespacedName]*context.CancelFunc),
		waiters:    make(map[types.NamespacedName]chan struct{}),
	}, nil
}

// SetupWithManager registers the Session reconciler with mgr.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&herorunv1alpha1.Session{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&herorunv1alpha1.Session{}, builder.WithPredicates(r.childSessionPredicate())).
		Owns(&herorunv1alpha1.SessionMessage{}, builder.WithPredicates(r.sessionMessagePredicate())).
		WithOptions(controller.Options{MaxConcurrentReconciles: r.opts.MaxConcurrent}).
		Complete(r)
}

func (r *Reconciler) childSessionPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			next, ok := e.ObjectNew.(*herorunv1alpha1.Session)
			if !ok || !next.Status.Phase.IsTerminal() {
				return false
			}
			if prev, ok := e.ObjectOld.(*herorunv1alpha1.Session); ok && prev.Status.Phase.IsTerminal() {
				return false
			}
			r.wakeParent(next)
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			if s, ok := e.Object.(*herorunv1alpha1.Session); ok && s.Spec.Parent != "" {
				r.wakeParent(s)
				return true
			}
			return false
		},
	}
}

func (r *Reconciler) sessionMessagePredicate() predicate.Predicate {
	gen := predicate.GenerationChangedPredicate{}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			if m, ok := e.Object.(*herorunv1alpha1.SessionMessage); ok && !m.IsDelivered() && m.Spec.SessionName != "" {
				r.wakeWaiter(types.NamespacedName{Namespace: m.Namespace, Name: m.Spec.SessionName})
			}
			return gen.Create(e)
		},
		UpdateFunc:  gen.Update,
		DeleteFunc:  gen.Delete,
		GenericFunc: gen.Generic,
	}
}

// Reconcile drives a Session resource from Pending through Running to a terminal status.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var obj herorunv1alpha1.Session
	if err := r.opts.Client.Get(ctx, req.NamespacedName, &obj); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if r.opts.StateStore != nil && obj.Spec.RunID != "" && obj.Spec.Repository != "" {
		repoKey, err := state.ParseRepoURL(obj.Spec.Repository)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("parsing repository %q: %w", obj.Spec.Repository, err)
		}
		if _, err := r.opts.StateStore.GetRepo(ctx, repoKey); err != nil {
			if errors.Is(err, state.ErrNotFound) {
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("getting repo %s: %w", repoKey, err)
		}
	}
	if obj.Status.Phase.IsTerminal() {
		return reconcile.Result{}, r.handleTerminal(ctx, &obj)
	}
	if ctx.Err() != nil {
		return reconcile.Result{}, nil
	}
	if obj.Spec.Parent != "" {
		var parent herorunv1alpha1.Session
		err := r.opts.Client.Get(ctx, client.ObjectKey{Namespace: obj.Namespace, Name: obj.Spec.Parent}, &parent)
		if err == nil && parent.Status.Phase.IsTerminal() {
			if err := r.markCanceled(ctx, &obj); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, r.handleTerminal(ctx, &obj)
		} else if err != nil && !k8serrors.IsNotFound(err) {
			return reconcile.Result{}, err
		}
	}
	if r.deadlinePassed(&obj) {
		err := r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
			st.Phase, st.Outcome, st.Error = herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeDeadlineExceeded, "deadline exceeded"
			st.WaitingUntil, st.FinishedAt = nil, &now
		})
		if err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, r.handleTerminal(ctx, &obj)
	}
	if obj.Status.Phase != herorunv1alpha1.SessionPhaseRunning || obj.Status.StartedAt == nil {
		if err := r.updateStatus(ctx, &obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
			st.Phase, st.WaitingUntil = herorunv1alpha1.SessionPhaseRunning, nil
			if st.StartedAt == nil {
				st.StartedAt = &now
			}
		}); err != nil {
			return reconcile.Result{}, err
		}
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	defer r.trackActive(req.NamespacedName, cancelRun)()

	applyTerminal, canceled := r.runSession(runCtx, &obj)
	if ctx.Err() != nil {
		return reconcile.Result{}, nil
	}
	if canceled {
		var latest herorunv1alpha1.Session
		if err := r.opts.Client.Get(ctx, req.NamespacedName, &latest); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
		if latest.Status.Phase.IsTerminal() {
			return reconcile.Result{}, r.handleTerminal(ctx, &latest)
		}
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
		st.WaitingUntil, st.FinishedAt = nil, &now
	})
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, r.handleTerminal(ctx, &obj)
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
		seenStopped := make(map[string]bool)
		extra = append(extra,
			ao.NewStartSessionTool(func(toolCtx context.Context, task string) (string, error) {
				return r.startChildSession(toolCtx, obj, task)
			}),
			ao.NewListSessionsTool(func(toolCtx context.Context) ([]ao.SessionInfo, error) {
				return r.listRunSessions(toolCtx, obj)
			}),
			ao.NewSendMessageTool(func(toolCtx context.Context, recipient, message string) error {
				return r.sendMessage(toolCtx, obj, recipient, message)
			}),
			ao.NewWaitTool(func(toolCtx context.Context, timeout time.Duration) (string, error) {
				return r.waitForEvent(toolCtx, obj, seenStopped, timeout)
			}),
		)
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
	r.wakeWaiter(types.NamespacedName{Namespace: target.Namespace, Name: target.Name})
	return nil
}

func (r *Reconciler) startChildSession(ctx context.Context, parent *herorunv1alpha1.Session, task string) (string, error) {
	if parent.Spec.RunID == "" {
		return "", errors.New("cannot call start_session: session belongs to no run")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	maxDepth := parent.Spec.MaxDepth
	if maxDepth <= 0 {
		maxDepth = r.opts.DefaultMaxDepth
	}
	if parent.Spec.Depth() >= maxDepth {
		return "", fmt.Errorf("depth limit of %d reached; cannot start child session", maxDepth)
	}
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generating child session name: %w", err)
	}
	childName := fmt.Sprintf("%sc%x", parent.Name[:min(len(parent.Name), 28)], suffix)

	ancestors := slices.Clone(parent.Spec.Ancestors)
	if len(ancestors) == 0 && parent.Spec.Parent != "" {
		ancestors = []string{parent.Spec.Parent}
	}
	childSpec := *parent.Spec.DeepCopy()
	childSpec.Parent, childSpec.Ancestors = parent.Name, append(ancestors, parent.Name)
	childSpec.InitialPrompt, childSpec.AllowPullRequest = task, false

	child := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName,
			Namespace: parent.Namespace,
			Labels:    map[string]string{herorunv1alpha1.LabelRun: parent.Spec.RunID},
		},
		Spec: childSpec,
	}
	if err := controllerutil.SetControllerReference(parent, child, r.opts.Client.Scheme()); err != nil {
		return "", fmt.Errorf("setting owner reference on child session: %w", err)
	}
	if err := r.opts.Client.Create(ctx, child); err != nil {
		return "", fmt.Errorf("creating child session: %w", err)
	}
	if r.opts.StateStore != nil && parent.Spec.Repository != "" {
		if repoKey, err := state.ParseRepoURL(parent.Spec.Repository); err != nil {
			scoped.Log(ctx).Error(err, "failed to parse repository for session_started event", "run", parent.Spec.RunID, "session", childName)
		} else if err := r.opts.StateStore.AppendEvent(ctx, repoKey, parent.Spec.RunID, state.Event{
			Time: r.opts.Clock().UTC(), Phase: state.PhaseWorking, Kind: "session_started",
			Message: fmt.Sprintf("Started session %s", childName),
			Data:    map[string]any{"session_id": childName, "parent": parent.Name},
		}); err != nil {
			scoped.Log(ctx).Error(err, "failed to append session_started event", "run", parent.Spec.RunID, "session", childName)
		}
	}
	return childName, nil
}

func (r *Reconciler) listRunSessions(ctx context.Context, obj *herorunv1alpha1.Session) ([]ao.SessionInfo, error) {
	if obj.Spec.RunID == "" {
		return nil, errors.New("cannot call list_sessions: session belongs to no run")
	}
	var list herorunv1alpha1.SessionList
	if err := r.opts.Client.List(ctx, &list, client.InNamespace(obj.Namespace)); err != nil {
		return nil, fmt.Errorf("listing sessions: %w", err)
	}
	var sessions []herorunv1alpha1.Session
	for i := range list.Items {
		if list.Items[i].Spec.RunID == obj.Spec.RunID {
			sessions = append(sessions, list.Items[i])
		}
	}
	slices.SortFunc(sessions, func(a, b herorunv1alpha1.Session) int {
		return cmp.Or(cmp.Compare(a.Spec.Depth(), b.Spec.Depth()), a.CreationTimestamp.Time.Compare(b.CreationTimestamp.Time), cmp.Compare(a.Name, b.Name))
	})
	result := make([]ao.SessionInfo, len(sessions))
	for i := range sessions {
		s := &sessions[i]
		st := "running"
		if s.Status.Phase.IsTerminal() || s.DeletionTimestamp != nil {
			st = "stopped"
		}
		result[i] = ao.SessionInfo{Name: s.Name, Parent: s.Spec.Parent, State: st}
	}
	return result, nil
}

func (r *Reconciler) waitForEvent(ctx context.Context, obj *herorunv1alpha1.Session, seenStopped map[string]bool, timeout time.Duration) (string, error) {
	if obj.Spec.RunID == "" {
		return "", errors.New("cannot call wait: session belongs to no run")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	waitDur, cappedByDeadline := timeout, false
	if obj.Spec.Deadline != nil {
		if rem := obj.Spec.Deadline.Time.Sub(r.opts.Clock().UTC()); rem <= 0 {
			return "Wait ended: run deadline passed.", nil
		} else if rem < waitDur {
			waitDur, cappedByDeadline = rem, true
		}
	}

	key := types.NamespacedName{Namespace: obj.Namespace, Name: obj.Name}
	timer := time.NewTimer(waitDur)
	defer timer.Stop()

	setPhase := func(phase herorunv1alpha1.SessionPhase, until *metav1.Time) {
		if err := r.updateStatus(ctx, obj, func(st *herorunv1alpha1.SessionStatus, _ metav1.Time) {
			st.Phase, st.WaitingUntil = phase, until
		}); err != nil {
			scoped.Log(ctx).V(1).Info("failed to update Session wait status", "session", obj.Name, "phase", phase, "error", err)
		}
	}
	waiting := false
	defer func() {
		if waiting && ctx.Err() == nil {
			setPhase(herorunv1alpha1.SessionPhaseRunning, nil)
		}
	}()

	for {
		ch := r.waitChan(key)
		msg, ready, err := r.checkWaitEvents(ctx, obj, seenStopped)
		if err != nil || ready {
			r.clearWaiter(key, ch)
			return msg, err
		}
		if !waiting {
			waiting = true
			until := metav1.NewTime(r.opts.Clock().UTC().Add(waitDur))
			setPhase(herorunv1alpha1.SessionPhaseWaiting, &until)
		}
		select {
		case <-ctx.Done():
			r.clearWaiter(key, ch)
			return "", ctx.Err()
		case <-ch:
		case <-timer.C:
			r.clearWaiter(key, ch)
			if cappedByDeadline {
				return "Wait ended: run deadline passed.", nil
			}
			return fmt.Sprintf("Timed out after %s with no new messages or stopped child sessions.", timeout), nil
		}
	}
}

func (r *Reconciler) checkWaitEvents(ctx context.Context, parent *herorunv1alpha1.Session, seen map[string]bool) (string, bool, error) {
	pending, err := r.listPendingMessages(ctx, parent)
	if err != nil {
		return "", false, fmt.Errorf("checking pending messages: %w", err)
	}
	var list herorunv1alpha1.SessionList
	if err := r.opts.Client.List(ctx, &list, client.InNamespace(parent.Namespace)); err != nil {
		return "", false, fmt.Errorf("checking child sessions: %w", err)
	}
	var stopped []string
	for i := range list.Items {
		c := &list.Items[i]
		if c.Spec.RunID == parent.Spec.RunID && c.Spec.Parent == parent.Name && c.Status.Phase.IsTerminal() && !seen[c.Name] {
			stopped = append(stopped, c.Name)
		}
	}
	if len(pending) == 0 && len(stopped) == 0 {
		return "", false, nil
	}
	slices.Sort(stopped)
	for _, name := range stopped {
		seen[name] = true
	}
	var parts []string
	if len(pending) > 0 {
		parts = append(parts, "A message arrived.")
	}
	if len(stopped) == 1 {
		parts = append(parts, fmt.Sprintf("Child session %s stopped.", stopped[0]))
	} else if len(stopped) > 1 {
		parts = append(parts, fmt.Sprintf("Child sessions stopped: %s.", strings.Join(stopped, ", ")))
	}
	return strings.Join(parts, " "), true, nil
}

func (r *Reconciler) handleTerminal(ctx context.Context, obj *herorunv1alpha1.Session) error {
	r.releaseRunBudgetIfRoot(obj)
	r.wakeParent(obj)
	return r.cancelChildren(ctx, obj)
}

func (r *Reconciler) markCanceled(ctx context.Context, obj *herorunv1alpha1.Session) error {
	return r.updateStatus(ctx, obj, func(st *herorunv1alpha1.SessionStatus, now metav1.Time) {
		st.Phase, st.Outcome, st.Error = herorunv1alpha1.SessionPhaseCanceled, herorunv1alpha1.SessionOutcomeCanceled, "parent session terminated"
		st.WaitingUntil, st.FinishedAt = nil, &now
	})
}

func (r *Reconciler) cancelChildren(ctx context.Context, parent *herorunv1alpha1.Session) error {
	var list herorunv1alpha1.SessionList
	if err := r.opts.Client.List(ctx, &list, client.InNamespace(parent.Namespace)); err != nil {
		return fmt.Errorf("listing child sessions: %w", err)
	}
	descendants := map[string]bool{parent.Name: true}
	for changed := true; changed; {
		changed = false
		for i := range list.Items {
			c := &list.Items[i]
			if !descendants[c.Name] && (descendants[c.Spec.Parent] || slices.ContainsFunc(c.Spec.Ancestors, func(a string) bool { return descendants[a] })) {
				descendants[c.Name], changed = true, true
			}
		}
	}
	var firstErr error
	for i := range list.Items {
		child := list.Items[i]
		if child.Name == parent.Name || !descendants[child.Name] || child.Status.Phase.IsTerminal() {
			continue
		}
		if err := r.markCanceled(ctx, &child); err != nil && !k8serrors.IsNotFound(err) {
			firstErr = cmp.Or(firstErr, err)
			continue
		}
		r.cancelActive(types.NamespacedName{Namespace: child.Namespace, Name: child.Name})
		if child.Spec.Parent != parent.Name {
			r.wakeParent(&child)
		}
	}
	return firstErr
}

func (r *Reconciler) trackActive(key types.NamespacedName, cancel context.CancelFunc) func() {
	h := &cancel
	r.handlesMu.Lock()
	r.active[key] = h
	r.handlesMu.Unlock()
	return func() {
		r.handlesMu.Lock()
		if r.active[key] == h {
			delete(r.active, key)
		}
		r.handlesMu.Unlock()
	}
}

func (r *Reconciler) cancelActive(key types.NamespacedName) {
	r.handlesMu.Lock()
	h := r.active[key]
	r.handlesMu.Unlock()
	if h != nil {
		(*h)()
	}
}

func (r *Reconciler) waitChan(key types.NamespacedName) <-chan struct{} {
	r.handlesMu.Lock()
	defer r.handlesMu.Unlock()
	if ch := r.waiters[key]; ch != nil {
		return ch
	}
	ch := make(chan struct{})
	r.waiters[key] = ch
	return ch
}

func (r *Reconciler) clearWaiter(key types.NamespacedName, ch <-chan struct{}) {
	r.handlesMu.Lock()
	if r.waiters[key] == ch {
		delete(r.waiters, key)
	}
	r.handlesMu.Unlock()
}

func (r *Reconciler) wakeParent(obj *herorunv1alpha1.Session) {
	if obj.Spec.Parent != "" {
		r.wakeWaiter(types.NamespacedName{Namespace: obj.Namespace, Name: obj.Spec.Parent})
	}
}

func (r *Reconciler) wakeWaiter(key types.NamespacedName) {
	r.handlesMu.Lock()
	if ch := r.waiters[key]; ch != nil {
		delete(r.waiters, key)
		close(ch)
	}
	r.handlesMu.Unlock()
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
