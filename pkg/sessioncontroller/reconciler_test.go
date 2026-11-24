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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

type fakeSandbox struct {
	mu       sync.Mutex
	commands [][]string
}

func (f *fakeSandbox) ExecCommand(_ context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	f.mu.Lock()
	f.commands = append(f.commands, slices.Clone(opts.Command))
	f.mu.Unlock()
	return &tools.ExecCommandResult{ExitCode: 0}, nil
}

type scriptedLLM struct {
	mu       sync.Mutex
	calls    int
	models   []string
	handlers []func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error)
}

func (s *scriptedLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	s.mu.Lock()
	idx := s.calls
	s.calls++
	s.models = append(s.models, req.Model)
	var h func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error)
	if idx < len(s.handlers) {
		h = s.handlers[idx]
	} else if len(s.handlers) > 0 {
		h = s.handlers[len(s.handlers)-1]
	}
	s.mu.Unlock()
	if h == nil {
		return nil, errors.New("unexpected LLM call")
	}
	return h(ctx, req)
}

func textReply(text string) func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	return func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
		return &llm.ChatCompletionResponse{
			ID: "resp-text",
			Choices: []llm.Choice{{
				Message:      llm.Message{Role: "assistant", Content: &text},
				FinishReason: "stop",
			}},
		}, nil
	}
}

func toolCallReply(callID, name string, args map[string]any) func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	raw, err := json.Marshal(args)
	if err != nil {
		panic(err)
	}
	return func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
		return &llm.ChatCompletionResponse{
			ID: "resp-tool",
			Choices: []llm.Choice{{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{{
						ID:   callID,
						Type: "function",
						Function: llm.FunctionCall{
							Name:      name,
							Arguments: string(raw),
						},
					}},
				},
				FinishReason: "tool_calls",
			}},
		}, nil
	}
}

type testEnv struct {
	client     client.Client
	store      session.Store
	reconciler *Reconciler
	sandbox    *fakeSandbox
	mu         sync.Mutex
	statuses   []herorunv1alpha1.SessionStatus
	now        time.Time
}

func newTestEnv(t *testing.T, llmClient llm.ChatClient, initObjs []client.Object, mutateOpts ...func(*Options)) *testEnv {
	t.Helper()

	scheme := apiruntime.NewScheme()
	if err := herorunv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}

	env := &testEnv{
		store:   session.NewFileStore(t.TempDir()),
		sandbox: &fakeSandbox{},
		now:     time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC),
	}

	env.client = fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.Session{}).
		WithObjects(initObjs...).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if err := c.SubResource(subResourceName).Update(ctx, obj, opts...); err != nil {
					return err
				}
				if s, ok := obj.(*herorunv1alpha1.Session); ok {
					env.mu.Lock()
					env.statuses = append(env.statuses, *s.Status.DeepCopy())
					env.mu.Unlock()
				}
				return nil
			},
		}).
		Build()

	opts := Options{
		Client:       env.client,
		SessionStore: env.store,
		HarnessFactory: func(_ string) (*session.Harness, error) {
			reg := tools.NewRegistry()
			reg.Add(&tools.RunCommand{})
			var runOpts session.RunOptions
			runOpts.InitDefaults()
			runOpts.ModelName = "default-model"
			runOpts.SystemPrompt = "default system prompt"
			h := session.NewHarness(llmClient, nil, reg, runOpts)
			h.SetSandboxOverride(env.sandbox)
			return h, nil
		},
		DefaultToolset: "basic",
		Clock:          func() time.Time { env.mu.Lock(); defer env.mu.Unlock(); return env.now },
		EstimateCost: func(_ string, msgs []llm.Message) float64 {
			return float64(countTurns(msgs)) * 0.25
		},
	}
	for _, fn := range mutateOpts {
		fn(&opts)
	}

	r, err := NewReconciler(opts)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	env.reconciler = r
	return env
}

func (e *testEnv) recordedPhases() []herorunv1alpha1.SessionPhase {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]herorunv1alpha1.SessionPhase, len(e.statuses))
	for i, st := range e.statuses {
		out[i] = st.Phase
	}
	return out
}

func (e *testEnv) getSession(t *testing.T, name string) *herorunv1alpha1.Session {
	t.Helper()
	var got herorunv1alpha1.Session
	if err := e.client.Get(t.Context(), types.NamespacedName{Namespace: "ao-sandboxes", Name: name}, &got); err != nil {
		t.Fatalf("Get Session %s: %v", name, err)
	}
	return &got
}

func pendingSession(name string, spec herorunv1alpha1.SessionSpec) *herorunv1alpha1.Session {
	return &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ao-sandboxes",
		},
		Spec: spec,
		Status: herorunv1alpha1.SessionStatus{
			Phase: herorunv1alpha1.SessionPhasePending,
		},
	}
}

func TestNewReconciler_Validation(t *testing.T) {
	scheme := apiruntime.NewScheme()
	_ = herorunv1alpha1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	store := session.NewFileStore(t.TempDir())
	factory := func(string) (*session.Harness, error) { return nil, nil }

	cases := []struct {
		name string
		opts Options
	}{
		{name: "missing client", opts: Options{SessionStore: store, HarnessFactory: factory}},
		{name: "missing store", opts: Options{Client: c, HarnessFactory: factory}},
		{name: "missing factory", opts: Options{Client: c, SessionStore: store}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewReconciler(tc.opts); err == nil {
				t.Fatalf("NewReconciler(%s) succeeded, want error", tc.name)
			}
		})
	}
}

func TestReconcile_NotFound(t *testing.T) {
	env := newTestEnv(t, &scriptedLLM{}, nil)
	res, err := env.reconciler.Reconcile(t.Context(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "missing"},
	})
	if err != nil || res != (reconcile.Result{}) {
		t.Fatalf("Reconcile missing = (%+v, %v), want empty result and nil error", res, err)
	}
}

func TestReconcile_Completed(t *testing.T) {
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		textReply("All done."),
	}}
	sessObj := pendingSession("sesscompleted", herorunv1alpha1.SessionSpec{
		RunID:         "20261124T120000Z-0001",
		Repository:    "https://github.com/gke-labs/test",
		Parent:        "rootsess",
		Ancestors:     []string{"rootsess"},
		Toolset:       "basic",
		Model:         "custom-model",
		SystemPrompt:  "Be concise.",
		InitialPrompt: "Inspect the repo.",
	})
	env := newTestEnv(t, fakeLLM, []client.Object{sessObj})

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesscompleted"}}
	res, err := env.reconciler.Reconcile(t.Context(), req)
	if err != nil || res != (reconcile.Result{}) {
		t.Fatalf("Reconcile = (%+v, %v), want empty result and nil error", res, err)
	}

	got := env.getSession(t, "sesscompleted")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded {
		t.Errorf("Phase = %q, want %q", got.Status.Phase, herorunv1alpha1.SessionPhaseSucceeded)
	}
	if got.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Errorf("Outcome = %q, want %q", got.Status.Outcome, herorunv1alpha1.SessionOutcomeCompleted)
	}
	if got.Status.Turns != 1 {
		t.Errorf("Turns = %d, want 1", got.Status.Turns)
	}
	if got.Status.MessageCount != 3 {
		t.Errorf("MessageCount = %d, want 3 (system, user, assistant)", got.Status.MessageCount)
	}
	if got.Status.CostUSD != 0.25 {
		t.Errorf("CostUSD = %v, want 0.25", got.Status.CostUSD)
	}
	if got.Status.StartedAt == nil || got.Status.FinishedAt == nil || got.Status.ObservedAt == nil {
		t.Errorf("expected StartedAt, FinishedAt, ObservedAt to be set; got %+v", got.Status)
	}
	if len(fakeLLM.models) != 1 || fakeLLM.models[0] != "custom-model" {
		t.Errorf("LLM models = %v, want [custom-model]", fakeLLM.models)
	}

	phases := env.recordedPhases()
	if len(phases) < 2 || phases[0] != herorunv1alpha1.SessionPhaseRunning || phases[len(phases)-1] != herorunv1alpha1.SessionPhaseSucceeded {
		t.Errorf("recorded phases = %v, want transition from Running to Succeeded", phases)
	}

	msgs, meta, err := env.store.LoadSession(t.Context(), "sesscompleted")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 3 {
		t.Errorf("persisted transcript len = %d, want 3", len(msgs))
	}
	if meta.RunID != "20261124T120000Z-0001" || meta.Parent != "rootsess" || !slices.Equal(meta.Ancestors, []string{"rootsess"}) {
		t.Errorf("persisted metadata = %+v, want RunID/Parent/Ancestors preserved", meta)
	}

	// Reconciling a terminal Session again must be a no-op.
	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if fakeLLM.calls != 1 {
		t.Errorf("LLM calls after terminal reconcile = %d, want 1", fakeLLM.calls)
	}
}

func TestReconcile_PullRequestRequested(t *testing.T) {
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		toolCallReply("call-pr", "open_pull_request", map[string]any{
			"repository": "https://github.com/gke-labs/test",
			"branch":     "ao/perf/20261124-0001",
			"title":      "perf: optimize hot path",
			"body":       "Cuts allocations by 25%.",
		}),
	}}
	sessObj := pendingSession("sesspr", herorunv1alpha1.SessionSpec{
		Repository:       "https://github.com/gke-labs/test",
		Branch:           "ao/perf/20261124-0001",
		InitialPrompt:    "Optimize the hot path.",
		AllowPullRequest: true,
	})
	env := newTestEnv(t, fakeLLM, []client.Object{sessObj})

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesspr"}}
	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := env.getSession(t, "sesspr")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomePullRequest {
		t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/PullRequest", got.Status.Phase, got.Status.Outcome)
	}
	if got.Status.PRTitle != "perf: optimize hot path" || got.Status.PRBody != "Cuts allocations by 25%." {
		t.Errorf("PR fields = (%q, %q), want title/body recorded", got.Status.PRTitle, got.Status.PRBody)
	}
	if got.Status.Turns != 1 || got.Status.MessageCount != 4 {
		t.Errorf("Turns=%d MessageCount=%d, want 1 and 4", got.Status.Turns, got.Status.MessageCount)
	}
	if phases := env.recordedPhases(); len(phases) < 2 || phases[0] != herorunv1alpha1.SessionPhaseRunning {
		t.Errorf("recorded phases = %v, want Running before terminal", phases)
	}
}

func TestReconcile_Finished(t *testing.T) {
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		toolCallReply("call-fin", "finish", map[string]any{
			"summary": "No measurable improvement found.",
		}),
	}}
	sessObj := pendingSession("sessfinish", herorunv1alpha1.SessionSpec{
		InitialPrompt: "Try optimizing.",
	})
	env := newTestEnv(t, fakeLLM, []client.Object{sessObj})

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessfinish"}}
	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := env.getSession(t, "sessfinish")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeFinished {
		t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/Finished", got.Status.Phase, got.Status.Outcome)
	}
	if got.Status.Summary != "No measurable improvement found." {
		t.Errorf("Summary = %q, want finish summary", got.Status.Summary)
	}
	if got.Status.Turns != 1 || got.Status.FinishedAt == nil {
		t.Errorf("Turns=%d FinishedAt=%v, want 1 and non-nil", got.Status.Turns, got.Status.FinishedAt)
	}
}

func TestReconcile_MaxTurns(t *testing.T) {
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		toolCallReply("call-1", "run_command", map[string]any{"command": "echo 1"}),
		toolCallReply("call-2", "run_command", map[string]any{"command": "echo 2"}),
		toolCallReply("call-3", "run_command", map[string]any{"command": "echo 3"}),
	}}
	sessObj := pendingSession("sessmaxturns", herorunv1alpha1.SessionSpec{
		InitialPrompt: "Keep running commands.",
		MaxTurns:      2,
	})
	env := newTestEnv(t, fakeLLM, []client.Object{sessObj})

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessmaxturns"}}
	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := env.getSession(t, "sessmaxturns")
	if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeMaxTurns {
		t.Fatalf("got Phase=%q Outcome=%q, want Succeeded/MaxTurns", got.Status.Phase, got.Status.Outcome)
	}
	if got.Status.Turns != 2 || fakeLLM.calls != 2 {
		t.Errorf("Turns=%d LLM calls=%d, want 2 and 2", got.Status.Turns, fakeLLM.calls)
	}
	if got.Status.CostUSD != 0.50 {
		t.Errorf("CostUSD = %v, want 0.50", got.Status.CostUSD)
	}

	// Verify intermediate Running progress updates were recorded.
	var sawRunningTurn1 bool
	for _, st := range env.statuses {
		if st.Phase == herorunv1alpha1.SessionPhaseRunning && st.Turns == 1 {
			sawRunningTurn1 = true
		}
	}
	if !sawRunningTurn1 {
		t.Errorf("expected intermediate Running status with Turns=1; statuses = %+v", env.statuses)
	}
}

func TestReconcile_DeadlineExceeded(t *testing.T) {
	t.Run("expired before start", func(t *testing.T) {
		fakeLLM := &scriptedLLM{}
		past := metav1.NewTime(time.Date(2026, 11, 24, 11, 59, 0, 0, time.UTC))
		sessObj := pendingSession("sessexpired", herorunv1alpha1.SessionSpec{
			InitialPrompt: "Do work.",
			Deadline:      &past,
		})
		env := newTestEnv(t, fakeLLM, []client.Object{sessObj})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessexpired"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		got := env.getSession(t, "sessexpired")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeDeadlineExceeded {
			t.Fatalf("got Phase=%q Outcome=%q, want Failed/DeadlineExceeded", got.Status.Phase, got.Status.Outcome)
		}
		if got.Status.Error != "deadline exceeded" || got.Status.FinishedAt == nil {
			t.Errorf("Error=%q FinishedAt=%v, want deadline exceeded and non-nil", got.Status.Error, got.Status.FinishedAt)
		}
		if fakeLLM.calls != 0 {
			t.Errorf("LLM calls = %d, want 0", fakeLLM.calls)
		}
	})

	t.Run("expires during turn", func(t *testing.T) {
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}}
		deadline := metav1.NewTime(time.Date(2026, 11, 24, 12, 0, 1, 0, time.UTC))
		sessObj := pendingSession("sesstimeout", herorunv1alpha1.SessionSpec{
			InitialPrompt: "Slow task.",
			Deadline:      &deadline,
		})
		env := newTestEnv(t, fakeLLM, []client.Object{sessObj}, func(o *Options) {
			o.Clock = func() time.Time { return deadline.Time.Add(-40 * time.Millisecond) }
		})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesstimeout"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		got := env.getSession(t, "sesstimeout")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeDeadlineExceeded {
			t.Fatalf("got Phase=%q Outcome=%q, want Failed/DeadlineExceeded", got.Status.Phase, got.Status.Outcome)
		}
		if got.Status.StartedAt == nil || got.Status.FinishedAt == nil {
			t.Errorf("expected StartedAt and FinishedAt to be set; got %+v", got.Status)
		}
		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls = %d, want 1", fakeLLM.calls)
		}
	})
}

func TestReconcile_Failure(t *testing.T) {
	t.Run("LLM error", func(t *testing.T) {
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				return nil, errors.New("upstream 503")
			},
		}}
		sessObj := pendingSession("sessfailllm", herorunv1alpha1.SessionSpec{
			InitialPrompt: "Work.",
		})
		env := newTestEnv(t, fakeLLM, []client.Object{sessObj})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessfailllm"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		got := env.getSession(t, "sessfailllm")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeFailed {
			t.Fatalf("got Phase=%q Outcome=%q, want Failed/Failed", got.Status.Phase, got.Status.Outcome)
		}
		if !strings.Contains(got.Status.Error, "upstream 503") {
			t.Errorf("Error = %q, want substring %q", got.Status.Error, "upstream 503")
		}
		if got.Status.StartedAt == nil || got.Status.FinishedAt == nil {
			t.Errorf("expected StartedAt and FinishedAt set; got %+v", got.Status)
		}
	})

	t.Run("harness factory error", func(t *testing.T) {
		sessObj := pendingSession("sessfailtoolset", herorunv1alpha1.SessionSpec{
			Toolset:       "unknown",
			InitialPrompt: "Work.",
		})
		env := newTestEnv(t, &scriptedLLM{}, []client.Object{sessObj}, func(o *Options) {
			o.HarnessFactory = func(toolset string) (*session.Harness, error) {
				return nil, fmt.Errorf("unknown toolset %q", toolset)
			}
		})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessfailtoolset"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		got := env.getSession(t, "sessfailtoolset")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeFailed {
			t.Fatalf("got Phase=%q Outcome=%q, want Failed/Failed", got.Status.Phase, got.Status.Outcome)
		}
		if !strings.Contains(got.Status.Error, "unknown toolset") {
			t.Errorf("Error = %q, want substring %q", got.Status.Error, "unknown toolset")
		}
	})
}

func TestReconcile_ShutdownCanceledDoesNotFailAndResumes(t *testing.T) {
	turn2Started := make(chan struct{})
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		toolCallReply("call-1", "run_command", map[string]any{"command": "echo step1"}),
		func(ctx context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			close(turn2Started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		textReply("Resumed and completed."),
	}}
	sessObj := pendingSession("sessshutdown", herorunv1alpha1.SessionSpec{
		InitialPrompt: "Multi-step work.",
		MaxTurns:      2,
	})
	env := newTestEnv(t, fakeLLM, []client.Object{sessObj})
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessshutdown"}}

	ctrlCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := env.reconciler.Reconcile(ctrlCtx, req)
		done <- err
	}()

	select {
	case <-turn2Started:
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Reconcile on shutdown returned error: %v", err)
		}
	case err := <-done:
		t.Fatalf("Reconcile exited before turn 2 started (err=%v, status=%+v)", err, env.getSession(t, "sessshutdown").Status)
	}

	mid := env.getSession(t, "sessshutdown")
	if mid.Status.Phase != herorunv1alpha1.SessionPhaseRunning {
		t.Fatalf("Phase after shutdown = %q, want Running", mid.Status.Phase)
	}
	if mid.Status.Outcome != "" || mid.Status.Error != "" || mid.Status.FinishedAt != nil {
		t.Errorf("expected no terminal outcome/error/FinishedAt on shutdown; got %+v", mid.Status)
	}
	if mid.Status.Turns != 1 || mid.Status.StartedAt == nil {
		t.Errorf("expected Turns=1 and StartedAt set; got %+v", mid.Status)
	}
	origStart := mid.Status.StartedAt.Time

	// Simulate controller restart: advance clock and reconcile with fresh context.
	env.mu.Lock()
	env.now = env.now.Add(5 * time.Minute)
	env.mu.Unlock()

	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("resumed Reconcile: %v", err)
	}

	final := env.getSession(t, "sessshutdown")
	if final.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || final.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
		t.Fatalf("final Phase=%q Outcome=%q, want Succeeded/Completed", final.Status.Phase, final.Status.Outcome)
	}
	if final.Status.Turns != 2 {
		t.Errorf("final Turns = %d, want 2", final.Status.Turns)
	}
	if !final.Status.StartedAt.Time.Equal(origStart) {
		t.Errorf("StartedAt changed on resume: got %v, want %v", final.Status.StartedAt.Time, origStart)
	}
	if final.Status.FinishedAt == nil || !final.Status.FinishedAt.Time.After(origStart) {
		t.Errorf("FinishedAt = %v, want after StartedAt %v", final.Status.FinishedAt, origStart)
	}
}
