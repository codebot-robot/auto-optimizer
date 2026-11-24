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
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

const (
	testRepoURL = "https://github.com/gke-labs/test"
	testRunID   = "20261124T120000Z-msg1"
)

func testRepoKey(t *testing.T) state.RepoKey {
	t.Helper()
	key, err := state.ParseRepoURL(testRepoURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}
	return key
}

func newMessagingTestEnv(t *testing.T, llmClient llm.ChatClient, initObjs []client.Object, mutateOpts ...func(*Options)) (*testEnv, state.Store) {
	t.Helper()
	stateStore := state.NewFileStore(t.TempDir())
	opts := append([]func(*Options){func(o *Options) {
		o.StateStore = stateStore
		o.Namespace = "ao-sandboxes"
	}}, mutateOpts...)
	return newTestEnv(t, llmClient, initObjs, opts...), stateStore
}

func registerWaiter(r *Reconciler, sessionName string) <-chan struct{} {
	r.msgMu.Lock()
	defer r.msgMu.Unlock()
	ch := make(chan struct{})
	r.msgWaiters[sessionName] = ch
	return ch
}

type hookSandbox struct {
	onExec func(ctx context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error)
}

func (h *hookSandbox) ExecCommand(ctx context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	if h.onExec != nil {
		return h.onExec(ctx, opts)
	}
	return &tools.ExecCommandResult{ExitCode: 0}, nil
}

type failingAppendStore struct {
	session.Store
	mu        sync.Mutex
	failMatch string
	err       error
}

func (f *failingAppendStore) AppendMessages(ctx context.Context, sessionID string, messages ...llm.Message) error {
	f.mu.Lock()
	match, failErr := f.failMatch, f.err
	f.mu.Unlock()
	if failErr != nil {
		for _, m := range messages {
			if match == "" || (m.Content != nil && strings.Contains(*m.Content, match)) {
				return failErr
			}
		}
	}
	return f.Store.AppendMessages(ctx, sessionID, messages...)
}

func TestReconcile_PendingMessages_DeliveredBeforeModelCall(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)

	var seenRequests [][]llm.Message
	var stateStore state.Store

	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			seenRequests = append(seenRequests, slices.Clone(req.Messages))
			return toolCallReply("call-1", "run_command", map[string]any{"command": "echo hi"})(ctx, req)
		},
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			seenRequests = append(seenRequests, slices.Clone(req.Messages))
			return textReply("Handled both messages.")(ctx, req)
		},
	}}

	sessObj := pendingSession("sesspremodel", herorunv1alpha1.SessionSpec{
		RunID:         testRunID,
		Repository:    testRepoURL,
		InitialPrompt: "Start working.",
	})

	var env *testEnv
	env, stateStore = newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj}, func(o *Options) {
		origFactory := o.HarnessFactory
		o.HarnessFactory = func(toolset string) (*session.Harness, error) {
			h, err := origFactory(toolset)
			if err != nil {
				return nil, err
			}
			h.SetSandboxOverride(&hookSandbox{
				onExec: func(execCtx context.Context, _ tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
					// Append two messages mid-turn (before model call 2) to verify FIFO order and formatting.
					if err := stateStore.AppendPendingMessage(execCtx, repoKey, testRunID, "sesspremodel", state.PendingMessage{
						From: "peer-1",
						Text: "check benchmarks",
					}); err != nil {
						return nil, err
					}
					if err := stateStore.AppendPendingMessage(execCtx, repoKey, testRunID, "sesspremodel", state.PendingMessage{
						Text: "also check memory",
					}); err != nil {
						return nil, err
					}
					return &tools.ExecCommandResult{Stdout: "ok\n", ExitCode: 0}, nil
				},
			})
			return h, nil
		}
	})

	// Pre-enqueue a message before the first model call.
	if err := stateStore.AppendPendingMessage(ctx, repoKey, testRunID, "sesspremodel", state.PendingMessage{
		Text: "early hint from user",
	}); err != nil {
		t.Fatalf("AppendPendingMessage: %v", err)
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesspremodel"}}
	if _, err := env.reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := env.getSession(t, "sesspremodel")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/Completed", got.Status.Phase, got.Status.Outcome)
	}
	if len(seenRequests) != 2 {
		t.Fatalf("LLM calls = %d, want 2", len(seenRequests))
	}

	// Call 1 sees: system, InitialPrompt, early hint.
	var call1User []string
	for _, m := range seenRequests[0] {
		if m.Role == "user" && m.Content != nil {
			call1User = append(call1User, *m.Content)
		}
	}
	if !slices.Equal(call1User, []string{"Start working.", "early hint from user"}) {
		t.Errorf("call 1 user messages = %v, want [Start working., early hint from user]", call1User)
	}

	// Call 2 sees the two mid-turn messages appended after the tool result in FIFO order.
	var call2User []string
	for _, m := range seenRequests[1] {
		if m.Role == "user" && m.Content != nil {
			call2User = append(call2User, *m.Content)
		}
	}
	wantCall2User := []string{
		"Start working.",
		"early hint from user",
		"Message from peer-1: check benchmarks",
		"also check memory",
	}
	if !slices.Equal(call2User, wantCall2User) {
		t.Errorf("call 2 user messages = %v, want %v", call2User, wantCall2User)
	}

	// Pending file must be cleared after successful delivery.
	pending, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, "sesspremodel")
	if err != nil {
		t.Fatalf("ReadPendingMessages: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("remaining pending messages = %+v, want 0", pending)
	}
}

func TestReconcile_MidTurnArrivalContinuesInsteadOfTerminating(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)

	turn1InFlight := make(chan struct{})
	releaseTurn1 := make(chan struct{})
	var call2Msgs []llm.Message

	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			close(turn1InFlight)
			<-releaseTurn1
			return textReply("Initial pass done.")(ctx, llm.ChatCompletionRequest{})
		},
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			call2Msgs = slices.Clone(req.Messages)
			return textReply("Handled follow-up message.")(ctx, req)
		},
	}}

	sessObj := pendingSession("sessmidturn", herorunv1alpha1.SessionSpec{
		RunID:         testRunID,
		Repository:    testRepoURL,
		InitialPrompt: "Initial task.",
	})
	env, stateStore := newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj})
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessmidturn"}}

	done := make(chan error, 1)
	go func() {
		_, err := env.reconciler.Reconcile(ctx, req)
		done <- err
	}()

	select {
	case <-turn1InFlight:
	case err := <-done:
		t.Fatalf("Reconcile exited early (err=%v)", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn 1 to start")
	}

	// Simulate an external caller (e.g. Orchestrator.PostMessage) arriving mid-turn.
	followUp := "Please also check the edge case."
	if err := stateStore.AppendPendingMessage(ctx, repoKey, testRunID, "sessmidturn", state.PendingMessage{
		Text: followUp,
	}); err != nil {
		t.Fatalf("AppendPendingMessage: %v", err)
	}
	env.reconciler.NotifyMessage("sessmidturn")

	close(releaseTurn1)

	if err := <-done; err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := env.getSession(t, "sessmidturn")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/Completed", got.Status.Phase, got.Status.Outcome)
	}
	if got.Status.Turns != 2 || fakeLLM.calls != 2 {
		t.Fatalf("Turns=%d LLM calls=%d, want 2 and 2", got.Status.Turns, fakeLLM.calls)
	}

	// Session must only transition to Succeeded once (after turn 2), never prematurely after turn 1.
	phases := env.recordedPhases()
	succeededCount := 0
	for _, p := range phases {
		if p == herorunv1alpha1.SessionPhaseSucceeded {
			succeededCount++
		}
	}
	if succeededCount != 1 || phases[len(phases)-1] != herorunv1alpha1.SessionPhaseSucceeded {
		t.Errorf("recorded phases = %v, want single terminal Succeeded at end", phases)
	}

	// Verify turn 2 received the follow-up message after turn 1's assistant response.
	if len(call2Msgs) != 4 || call2Msgs[3].Role != "user" || call2Msgs[3].Content == nil || *call2Msgs[3].Content != followUp {
		t.Fatalf("call 2 messages = %+v, want 4th message to be user %q", call2Msgs, followUp)
	}

	// Verify full transcript and cleared pending store.
	msgs, _, err := env.store.LoadSession(ctx, "sessmidturn")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("transcript len = %d, want 5 (system, user1, assistant1, user2, assistant2)", len(msgs))
	}
	pending, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, "sessmidturn")
	if err != nil || len(pending) != 0 {
		t.Errorf("pending after continuation = %+v (err=%v), want empty", pending, err)
	}
}

func TestReconcile_ContinuationStopsOnFinishAndMaxTurns(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)

	t.Run("finish tool ends session despite pending message", func(t *testing.T) {
		var stateStore state.Store
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(callCtx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				if err := stateStore.AppendPendingMessage(callCtx, repoKey, testRunID, "sessfinpending", state.PendingMessage{
					Text: "arrived while finishing",
				}); err != nil {
					return nil, err
				}
				return toolCallReply("call-fin", "finish", map[string]any{"summary": "done"})(callCtx, req)
			},
		}}
		sessObj := pendingSession("sessfinpending", herorunv1alpha1.SessionSpec{
			RunID:         testRunID,
			Repository:    testRepoURL,
			InitialPrompt: "Work.",
		})
		var env *testEnv
		env, stateStore = newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessfinpending"}}
		if _, err := env.reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		got := env.getSession(t, "sessfinpending")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeFinished {
			t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/Finished", got.Status.Phase, got.Status.Outcome)
		}
		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls = %d, want 1", fakeLLM.calls)
		}
	})

	t.Run("max turns stops continuation", func(t *testing.T) {
		var stateStore state.Store
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(callCtx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				if err := stateStore.AppendPendingMessage(callCtx, repoKey, testRunID, "sessmaxpending", state.PendingMessage{
					Text: "arrived on final allowed turn",
				}); err != nil {
					return nil, err
				}
				return textReply("Turn 1 complete.")(callCtx, req)
			},
		}}
		sessObj := pendingSession("sessmaxpending", herorunv1alpha1.SessionSpec{
			RunID:         testRunID,
			Repository:    testRepoURL,
			InitialPrompt: "Work.",
			MaxTurns:      1,
		})
		var env *testEnv
		env, stateStore = newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessmaxpending"}}
		if _, err := env.reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		got := env.getSession(t, "sessmaxpending")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeMaxTurns {
			t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/MaxTurns", got.Status.Phase, got.Status.Outcome)
		}
		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls = %d, want 1", fakeLLM.calls)
		}
	})

	t.Run("deadline exceeded stops continuation", func(t *testing.T) {
		var stateStore state.Store
		var env *testEnv
		deadline := metav1.NewTime(time.Date(2026, 11, 24, 12, 1, 0, 0, time.UTC))
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(callCtx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				if err := stateStore.AppendPendingMessage(callCtx, repoKey, testRunID, "sessdeadpending", state.PendingMessage{
					Text: "arrived just as deadline passed",
				}); err != nil {
					return nil, err
				}
				env.mu.Lock()
				env.now = deadline.Time
				env.mu.Unlock()
				return textReply("Turn 1 complete.")(callCtx, req)
			},
		}}
		sessObj := pendingSession("sessdeadpending", herorunv1alpha1.SessionSpec{
			RunID:         testRunID,
			Repository:    testRepoURL,
			InitialPrompt: "Work.",
			Deadline:      &deadline,
		})
		env, stateStore = newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessdeadpending"}}
		if _, err := env.reconciler.Reconcile(ctx, req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		got := env.getSession(t, "sessdeadpending")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeDeadlineExceeded {
			t.Fatalf("got Phase=%q Outcome=%q, want Failed/DeadlineExceeded", got.Status.Phase, got.Status.Outcome)
		}
		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls = %d, want 1", fakeLLM.calls)
		}
	})
}

func TestReconcile_PendingMessages_WriteFailureRetainsMessages(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)
	pendingText := "critical instruction that must survive write failure"

	baseSessionStore := session.NewFileStore(t.TempDir())
	failingStore := &failingAppendStore{
		Store:     baseSessionStore,
		failMatch: pendingText,
		err:       errors.New("simulated disk full"),
	}

	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		textReply("Recovered and received message."),
	}}
	sessObj := pendingSession("sesswritefail", herorunv1alpha1.SessionSpec{
		RunID:         testRunID,
		Repository:    testRepoURL,
		InitialPrompt: "Begin.",
	})
	env, stateStore := newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj}, func(o *Options) {
		o.SessionStore = failingStore
	})

	if err := stateStore.AppendPendingMessage(ctx, repoKey, testRunID, "sesswritefail", state.PendingMessage{
		Text: pendingText,
	}); err != nil {
		t.Fatalf("AppendPendingMessage: %v", err)
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesswritefail"}}
	if _, err := env.reconciler.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := env.getSession(t, "sesswritefail")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || !strings.Contains(got.Status.Error, "simulated disk full") {
		t.Fatalf("got Phase=%q Error=%q, want Failed with disk full error", got.Status.Phase, got.Status.Error)
	}
	if fakeLLM.calls != 0 {
		t.Errorf("LLM calls = %d, want 0 when delivery write fails", fakeLLM.calls)
	}

	// Pending message MUST still exist in StateStore because transcript write failed.
	pending, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, "sesswritefail")
	if err != nil {
		t.Fatalf("ReadPendingMessages: %v", err)
	}
	if len(pending) != 1 || pending[0].Text != pendingText {
		t.Fatalf("pending after write failure = %+v, want [%q]", pending, pendingText)
	}

	// Once the session store recovers, retrying delivery writes to transcript and clears StateStore.
	failingStore.mu.Lock()
	failingStore.err = nil
	failingStore.mu.Unlock()

	h, err := env.reconciler.opts.HarnessFactory("basic")
	if err != nil {
		t.Fatalf("HarnessFactory: %v", err)
	}
	sess, err := h.LoadSession(ctx, baseSessionStore, "sesswritefail")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if err := env.reconciler.deliverPendingMessages(ctx, repoKey, testRunID, sess); err != nil {
		t.Fatalf("deliverPendingMessages after recovery: %v", err)
	}

	pendingAfter, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, "sesswritefail")
	if err != nil || len(pendingAfter) != 0 {
		t.Fatalf("pending after recovery = %+v (err=%v), want empty", pendingAfter, err)
	}
	msgs, _, err := baseSessionStore.LoadSession(ctx, "sesswritefail")
	if err != nil {
		t.Fatalf("LoadSession after recovery: %v", err)
	}
	found := slices.ContainsFunc(msgs, func(m llm.Message) bool {
		return m.Role == "user" && m.Content != nil && *m.Content == pendingText
	})
	if !found {
		t.Errorf("recovered transcript missing %q: %+v", pendingText, msgs)
	}
}

func TestReconcile_PendingMessages_SurviveRestart(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)

	turn1Started := make(chan struct{})
	var resumedReq []llm.Message

	fakeLLM1 := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(callCtx context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			close(turn1Started)
			<-callCtx.Done()
			return nil, callCtx.Err()
		},
	}}

	sessObj := pendingSession("sessrestart", herorunv1alpha1.SessionSpec{
		RunID:         testRunID,
		Repository:    testRepoURL,
		InitialPrompt: "Start work.",
	})
	env1, stateStore := newMessagingTestEnv(t, fakeLLM1, []client.Object{sessObj})
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessrestart"}}

	ctrlCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := env1.reconciler.Reconcile(ctrlCtx, req)
		done <- err
	}()

	select {
	case <-turn1Started:
	case err := <-done:
		t.Fatalf("Reconcile exited before turn 1 started (err=%v)", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn 1 to start")
	}

	// Persist a pending message while controller 1 is running, then crash/shut down controller 1.
	survivedMsg := "instruction persisted before restart"
	if err := stateStore.AppendPendingMessage(ctx, repoKey, testRunID, "sessrestart", state.PendingMessage{
		From: "coord",
		Text: survivedMsg,
	}); err != nil {
		t.Fatalf("AppendPendingMessage: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Reconcile on shutdown: %v", err)
	}

	// Simulate restart with a brand new Reconciler sharing Client, SessionStore, and StateStore.
	fakeLLM2 := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			resumedReq = slices.Clone(req.Messages)
			return textReply("Completed after restart.")(ctx, req)
		},
	}}
	reconciler2, err := NewReconciler(Options{
		Client:       env1.client,
		SessionStore: env1.store,
		StateStore:   stateStore,
		HarnessFactory: func(_ string) (*session.Harness, error) {
			reg := tools.NewRegistry()
			var runOpts session.RunOptions
			runOpts.InitDefaults()
			runOpts.ModelName = "default-model"
			runOpts.SystemPrompt = "default system prompt"
			h := session.NewHarness(fakeLLM2, nil, reg, runOpts)
			h.SetSandboxOverride(env1.sandbox)
			return h, nil
		},
		DefaultToolset: "basic",
		Namespace:      "ao-sandboxes",
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	if _, err := reconciler2.Reconcile(ctx, req); err != nil {
		t.Fatalf("resumed Reconcile: %v", err)
	}

	got := env1.getSession(t, "sessrestart")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/Completed", got.Status.Phase, got.Status.Outcome)
	}

	wantText := "Message from coord: " + survivedMsg
	sawSurvived := slices.ContainsFunc(resumedReq, func(m llm.Message) bool {
		return m.Role == "user" && m.Content != nil && *m.Content == wantText
	})
	if !sawSurvived {
		t.Fatalf("resumed LLM request missing %q; messages = %+v", wantText, resumedReq)
	}

	pending, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, "sessrestart")
	if err != nil || len(pending) != 0 {
		t.Errorf("pending after restart delivery = %+v (err=%v), want empty", pending, err)
	}
}

func TestReconcile_SendMessageTool_AndWakeup(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)

	var senderToolResult string
	var recipientSaw []llm.Message

	senderLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		toolCallReply("call-send", "send_message", map[string]any{
			"recipient": "sessb",
			"message":   "inspect profile.out",
		}),
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			for _, m := range req.Messages {
				if m.Role == "tool" && m.ToolCallID == "call-send" && m.Content != nil {
					senderToolResult = *m.Content
				}
			}
			return textReply("Sent message to sessb.")(ctx, req)
		},
	}}

	sessA := pendingSession("sessa", herorunv1alpha1.SessionSpec{
		RunID:         testRunID,
		Repository:    testRepoURL,
		InitialPrompt: "Coordinate with sessb.",
	})
	sessB := pendingSession("sessb", herorunv1alpha1.SessionSpec{
		RunID:         testRunID,
		Repository:    testRepoURL,
		Parent:        "sessa",
		InitialPrompt: "Analyze profiles.",
	})
	sessB.Status.Phase = herorunv1alpha1.SessionPhaseRunning

	env, stateStore := newMessagingTestEnv(t, senderLLM, []client.Object{sessA, sessB})

	// Register an in-process waiter for sessb before sessa sends the message.
	waitCh := registerWaiter(env.reconciler, "sessb")

	reqA := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessa"}}
	if _, err := env.reconciler.Reconcile(ctx, reqA); err != nil {
		t.Fatalf("Reconcile sessa: %v", err)
	}

	gotA := env.getSession(t, "sessa")
	if gotA.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || gotA.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Fatalf("sessa got Phase=%q Outcome=%q Error=%q, want Succeeded/Completed", gotA.Status.Phase, gotA.Status.Outcome, gotA.Status.Error)
	}
	if !strings.Contains(senderToolResult, "sessb") {
		t.Errorf("send_message tool result = %q, want confirmation mentioning sessb", senderToolResult)
	}

	// Verify recipient waiter channel was woken.
	select {
	case <-waitCh:
	default:
		t.Fatal("expected sessb message waiter channel to be closed by send_message")
	}

	// Verify reconcile wakeup event was enqueued for sessb.
	select {
	case ev := <-env.reconciler.notifyCh:
		if ev.Object.Namespace != "ao-sandboxes" || ev.Object.Name != "sessb" {
			t.Errorf("notifyCh event = %+v, want ao-sandboxes/sessb", ev.Object)
		}
	default:
		t.Fatal("expected notifyCh reconcile wakeup for sessb")
	}

	// Verify message was persisted to StateStore with sender attribution.
	pending, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, "sessb")
	if err != nil {
		t.Fatalf("ReadPendingMessages: %v", err)
	}
	if len(pending) != 1 || pending[0].From != "sessa" || pending[0].Text != "inspect profile.out" {
		t.Fatalf("pending messages for sessb = %+v, want [{From:sessa Text:inspect profile.out}]", pending)
	}

	// Now reconcile sessb and verify it receives the formatted message before its LLM call.
	recipientLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			recipientSaw = slices.Clone(req.Messages)
			return textReply("Inspected profile.out.")(ctx, req)
		},
	}}
	env.reconciler.opts.HarnessFactory = func(_ string) (*session.Harness, error) {
		reg := tools.NewRegistry()
		var runOpts session.RunOptions
		runOpts.InitDefaults()
		runOpts.ModelName = "default-model"
		runOpts.SystemPrompt = "default system prompt"
		h := session.NewHarness(recipientLLM, nil, reg, runOpts)
		h.SetSandboxOverride(env.sandbox)
		return h, nil
	}

	reqB := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessb"}}
	if _, err := env.reconciler.Reconcile(ctx, reqB); err != nil {
		t.Fatalf("Reconcile sessb: %v", err)
	}
	gotB := env.getSession(t, "sessb")
	if gotB.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || gotB.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Fatalf("sessb got Phase=%q Outcome=%q Error=%q, want Succeeded/Completed", gotB.Status.Phase, gotB.Status.Outcome, gotB.Status.Error)
	}
	wantMsg := "Message from sessa: inspect profile.out"
	if !slices.ContainsFunc(recipientSaw, func(m llm.Message) bool {
		return m.Role == "user" && m.Content != nil && *m.Content == wantMsg
	}) {
		t.Errorf("sessb LLM request missing %q; messages = %+v", wantMsg, recipientSaw)
	}
}

func TestSendMessage_Validation(t *testing.T) {
	ctx := t.Context()
	repoKey := testRepoKey(t)

	sender := pendingSession("sesssender", herorunv1alpha1.SessionSpec{
		RunID:      testRunID,
		Repository: testRepoURL,
	})
	sender.Status.Phase = herorunv1alpha1.SessionPhaseRunning

	otherRun := pendingSession("sessotherrun", herorunv1alpha1.SessionSpec{
		RunID:      "different-run-id",
		Repository: testRepoURL,
	})
	otherRun.Status.Phase = herorunv1alpha1.SessionPhaseRunning

	succeeded := pendingSession("sesssucceeded", herorunv1alpha1.SessionSpec{
		RunID:      testRunID,
		Repository: testRepoURL,
	})
	succeeded.Status.Phase = herorunv1alpha1.SessionPhaseSucceeded

	failed := pendingSession("sessfailed", herorunv1alpha1.SessionSpec{
		RunID:      testRunID,
		Repository: testRepoURL,
	})
	failed.Status.Phase = herorunv1alpha1.SessionPhaseFailed

	canceled := pendingSession("sesscanceled", herorunv1alpha1.SessionSpec{
		RunID:      testRunID,
		Repository: testRepoURL,
	})
	canceled.Status.Phase = herorunv1alpha1.SessionPhaseCanceled

	waiting := pendingSession("sesswaiting", herorunv1alpha1.SessionSpec{
		RunID:      testRunID,
		Repository: testRepoURL,
	})
	waiting.Status.Phase = herorunv1alpha1.SessionPhaseWaiting

	pendingPeer := pendingSession("sesspending", herorunv1alpha1.SessionSpec{
		RunID:      testRunID,
		Repository: testRepoURL,
	})

	env, stateStore := newMessagingTestEnv(t, &scriptedLLM{}, []client.Object{
		sender, otherRun, succeeded, failed, canceled, waiting, pendingPeer,
	})

	cases := []struct {
		name      string
		sender    *herorunv1alpha1.Session
		recipient string
		wantErr   string
	}{
		{
			name:      "sender belongs to no run",
			sender:    pendingSession("standalone", herorunv1alpha1.SessionSpec{Repository: testRepoURL}),
			recipient: "sesswaiting",
			wantErr:   "session belongs to no run",
		},
		{
			name:      "send to self",
			sender:    sender,
			recipient: "sesssender",
			wantErr:   "cannot send message to self",
		},
		{
			name:      "missing recipient",
			sender:    sender,
			recipient: "doesnotexist",
			wantErr:   "not found",
		},
		{
			name:      "different run",
			sender:    sender,
			recipient: "sessotherrun",
			wantErr:   "not in the same run",
		},
		{
			name:      "succeeded recipient",
			sender:    sender,
			recipient: "sesssucceeded",
			wantErr:   "is not running",
		},
		{
			name:      "failed recipient",
			sender:    sender,
			recipient: "sessfailed",
			wantErr:   "is not running",
		},
		{
			name:      "canceled recipient",
			sender:    sender,
			recipient: "sesscanceled",
			wantErr:   "is not running",
		},
		{
			name:      "waiting recipient succeeds",
			sender:    sender,
			recipient: "sesswaiting",
		},
		{
			name:      "pending recipient succeeds",
			sender:    sender,
			recipient: "sesspending",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := env.reconciler.sendMessage(ctx, tc.sender, repoKey, tc.recipient, "hello")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("sendMessage err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("sendMessage unexpected error: %v", err)
			}
			msgs, err := stateStore.ReadPendingMessages(ctx, repoKey, testRunID, tc.recipient)
			if err != nil || len(msgs) != 1 || msgs[0].From != tc.sender.Name || msgs[0].Text != "hello" {
				t.Errorf("pending messages for %s = %+v (err=%v), want 1 message from %s", tc.recipient, msgs, err, tc.sender.Name)
			}
		})
	}
}

func TestReconcile_StandaloneSessionHasNoSendMessageTool(t *testing.T) {
	var seenToolNames []string
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			for _, tool := range req.Tools {
				seenToolNames = append(seenToolNames, tool.Function.Name)
			}
			return textReply("ok")(t.Context(), req)
		},
	}}
	sessObj := pendingSession("sessstandalone", herorunv1alpha1.SessionSpec{
		Repository:    testRepoURL,
		InitialPrompt: "Hello.",
	})
	env, _ := newMessagingTestEnv(t, fakeLLM, []client.Object{sessObj})
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessstandalone"}}
	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := env.getSession(t, "sessstandalone")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || fakeLLM.calls != 1 {
		t.Fatalf("got Phase=%q Error=%q calls=%d, want Succeeded and 1 call", got.Status.Phase, got.Status.Error, fakeLLM.calls)
	}
	if slices.Contains(seenToolNames, "send_message") {
		t.Errorf("standalone session unexpectedly exposed send_message tool; tools = %v", seenToolNames)
	}
}

func TestNotifyMessage_WakesWaiterAndEnqueuesReconcile(t *testing.T) {
	// Nil receiver and empty session name are safe no-ops.
	var nilRec *Reconciler
	nilRec.NotifyMessage("anything")

	env, _ := newMessagingTestEnv(t, &scriptedLLM{}, nil)
	env.reconciler.NotifyMessage("")

	waitCh := registerWaiter(env.reconciler, "extsess")

	env.reconciler.NotifyMessage("extsess")

	select {
	case <-waitCh:
	default:
		t.Fatal("expected waiter channel to close on NotifyMessage")
	}

	select {
	case ev := <-env.reconciler.notifyCh:
		if ev.Object.Namespace != "ao-sandboxes" || ev.Object.Name != "extsess" {
			t.Errorf("notifyCh event = %+v, want ao-sandboxes/extsess", ev.Object)
		}
	default:
		t.Fatal("expected NotifyMessage to enqueue reconcile request")
	}
}
