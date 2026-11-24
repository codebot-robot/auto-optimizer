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

package sessioncontroller

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// NotifyMessage signals that a pending message has arrived for sessionName,
// waking any in-process waiter and enqueueing reconciliation when a namespace is known.
func (r *Reconciler) NotifyMessage(sessionName string) {
	r.notifyMessage("", sessionName)
}

func (r *Reconciler) notifyMessage(namespace, sessionName string) {
	if r == nil || sessionName == "" {
		return
	}
	r.msgMu.Lock()
	if ch, ok := r.msgWaiters[sessionName]; ok {
		close(ch)
		delete(r.msgWaiters, sessionName)
	}
	ns := cmp.Or(namespace, r.sessionNamespaces[sessionName], r.opts.Namespace)
	r.msgMu.Unlock()

	if ns != "" && r.notifyCh != nil {
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: sessionName}}
		select {
		case r.notifyCh <- event.TypedGenericEvent[reconcile.Request]{Object: req}:
		default:
		}
	}
}

func (r *Reconciler) setSessionNamespace(sessionName, namespace string) {
	r.msgMu.Lock()
	if namespace == "" {
		delete(r.sessionNamespaces, sessionName)
	} else {
		r.sessionNamespaces[sessionName] = namespace
	}
	r.msgMu.Unlock()
}

func (r *Reconciler) sessionRepoKey(obj *herorunv1alpha1.Session) (state.RepoKey, bool, error) {
	if r.opts.StateStore == nil || obj == nil || obj.Spec.RunID == "" || obj.Spec.Repository == "" {
		return state.RepoKey{}, false, nil
	}
	key, err := state.ParseRepoURL(obj.Spec.Repository)
	if err != nil {
		return state.RepoKey{}, false, fmt.Errorf("parsing session repository %q: %w", obj.Spec.Repository, err)
	}
	return key, true, nil
}

func (r *Reconciler) deliverPendingMessages(ctx context.Context, repoKey state.RepoKey, runID string, sess *session.Session) error {
	r.msgMu.Lock()
	defer r.msgMu.Unlock()

	msgs, err := r.opts.StateStore.ReadPendingMessages(ctx, repoKey, runID, sess.Name)
	if err != nil || len(msgs) == 0 {
		return err
	}
	toAdd := make([]llm.Message, 0, len(msgs))
	for _, pm := range msgs {
		text := pm.Text
		if pm.From != "" {
			text = fmt.Sprintf("Message from %s: %s", pm.From, pm.Text)
		}
		toAdd = append(toAdd, llm.Message{Role: "user", Content: &text})
	}
	// Persist to the session transcript before clearing pending messages so
	// messages survive write failures and restarts.
	if err := sess.AddMessages(ctx, toAdd...); err != nil {
		return fmt.Errorf("adding pending messages to session: %w", err)
	}
	if err := r.opts.StateStore.ClearPendingMessages(ctx, repoKey, runID, sess.Name); err != nil {
		return fmt.Errorf("clearing pending messages: %w", err)
	}
	return nil
}

func (r *Reconciler) hasPendingMessages(ctx context.Context, repoKey state.RepoKey, runID, sessionID string) (bool, error) {
	r.msgMu.Lock()
	defer r.msgMu.Unlock()

	msgs, err := r.opts.StateStore.ReadPendingMessages(ctx, repoKey, runID, sessionID)
	return len(msgs) > 0, err
}

func (r *Reconciler) sendMessage(ctx context.Context, sender *herorunv1alpha1.Session, repoKey state.RepoKey, recipient, message string) error {
	if sender.Spec.RunID == "" {
		return errors.New("cannot call send_message: session belongs to no run")
	}
	if recipient == sender.Name {
		return errors.New("cannot send message to self")
	}
	var target herorunv1alpha1.Session
	if err := r.opts.Client.Get(ctx, types.NamespacedName{Namespace: sender.Namespace, Name: recipient}, &target); err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("recipient session %q not found", recipient)
		}
		return fmt.Errorf("getting recipient session %q: %w", recipient, err)
	}
	if target.Spec.RunID != sender.Spec.RunID {
		return fmt.Errorf("cannot send message to session %q: not in the same run", recipient)
	}
	if target.Status.Phase.IsTerminal() {
		return fmt.Errorf("recipient session %q is not running (phase %s)", recipient, target.Status.Phase)
	}

	r.msgMu.Lock()
	err := r.opts.StateStore.AppendPendingMessage(ctx, repoKey, sender.Spec.RunID, recipient, state.PendingMessage{
		From: sender.Name,
		Text: message,
	})
	r.msgMu.Unlock()
	if err != nil {
		return fmt.Errorf("appending pending message: %w", err)
	}
	r.notifyMessage(sender.Namespace, recipient)
	return nil
}
