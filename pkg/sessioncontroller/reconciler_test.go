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
	"crypto/ed25519"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	crcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
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
		WithStatusSubresource(&herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).
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
	if meta.Repo != "github.com/gke-labs/test" {
		t.Errorf("persisted metadata Repo = %q, want %q", meta.Repo, "github.com/gke-labs/test")
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

type echoSandboxTool struct{}

func (*echoSandboxTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name:       "echo_sandbox",
			Parameters: map[string]any{"type": "object"},
		},
	}
}

func (*echoSandboxTool) Run(_ context.Context, sb tools.Sandbox) (llm.Message, error) {
	out := fmt.Sprintf("%p", sb)
	return llm.Message{Role: "tool", Content: &out}, nil
}

func newFakeSandboxHarness(t *testing.T, llmClient llm.ChatClient) (*session.Harness, *fakeagents.Clientset) {
	t.Helper()

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	sbClient := session.NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	agentsCS.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		sb := action.(k8stesting.CreateAction).GetObject().(*sandboxv1beta1.Sandbox).DeepCopy()
		sb.Status.Conditions = []metav1.Condition{{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		}}
		sb.UID = types.UID("uid-" + sb.Name)
		_ = agentsCS.Tracker().Create(action.GetResource(), sb, action.GetNamespace())
		return true, sb, nil
	})

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	reg := tools.NewRegistry()
	reg.Add(&echoSandboxTool{})
	var runOpts session.RunOptions
	runOpts.InitDefaults()
	runOpts.Namespace = "ao-sandboxes"
	runOpts.ControlPlaneNamespace = "ao-system"
	runOpts.ModelName = "default-model"
	runOpts.SystemPrompt = "default system prompt"
	runOpts.SigningKey = privKey

	return session.NewHarness(llmClient, sbClient, reg, runOpts), agentsCS
}

func TestReconcile_NormalizedRepoMetadata(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{name: "https url", input: "https://github.com/gke-labs/test", want: "github.com/gke-labs/test"},
		{name: "https url with .git", input: "https://github.com/gke-labs/test.git", want: "github.com/gke-labs/test"},
		{name: "ssh scp url", input: "git@github.com:gke-labs/test.git", want: "github.com/gke-labs/test"},
		{name: "file url", input: "file:///tmp/org/myrepo.git", want: "local/org/myrepo"},
		{name: "already repo key", input: "github.com/gke-labs/test", want: "github.com/gke-labs/test"},
		{name: "already local repo key", input: "local/org/myrepo", want: "local/org/myrepo"},
		{name: "empty", input: "", want: ""},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := fmt.Sprintf("sessrepo%d", i)
			fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
				textReply("Done."),
			}}
			sessObj := pendingSession(name, herorunv1alpha1.SessionSpec{
				Repository:    tc.input,
				InitialPrompt: "Check repo metadata.",
			})
			env := newTestEnv(t, fakeLLM, []client.Object{sessObj})
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: name}}
			if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			_, meta, err := env.store.LoadSession(t.Context(), name)
			if err != nil {
				t.Fatalf("LoadSession: %v", err)
			}
			if meta.Repo != tc.want {
				t.Errorf("Metadata.Repo = %q, want %q", meta.Repo, tc.want)
			}
			if tc.want != "" {
				if _, err := state.ParseRepoKey(meta.Repo); err != nil {
					t.Errorf("state.ParseRepoKey(%q) failed: %v", meta.Repo, err)
				}
			}
		})
	}
}

func TestReconcile_SandboxOwnerHookAndStandaloneDefault(t *testing.T) {
	t.Run("run sessions share SandboxOwner with replacement options and preparer", func(t *testing.T) {
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			toolCallReply("call-1", "echo_sandbox", nil),
			textReply("Root done."),
			toolCallReply("call-2", "echo_sandbox", nil),
			textReply("Child done."),
		}}
		harness, agentsCS := newFakeSandboxHarness(t, fakeLLM)

		sharedOwner := session.NewSandboxOwner(harness)
		shutdownAt := time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC)
		sharedOwner.SetCreateSandboxOptions(session.CreateSandboxOptions{
			Name:               "run-sb-1",
			Labels:             map[string]string{"ao.gke.io/run": "run-1"},
			Annotations:        map[string]string{"ao.gke.io/repo": "github.com/gke-labs/test"},
			OwnerReferences:    []metav1.OwnerReference{{Kind: "HeroRun", Name: "run-sb-1", UID: "hr-uid-1"}},
			ShutdownTime:       shutdownAt,
			WorkspaceClaimName: "ws-claim-1",
			PortalIntercept:    true,
			ExtendLifecycle:    false,
		})
		prepareCalls := 0
		sharedOwner.SetSandboxPreparer(func(_ context.Context, _ *session.Sandbox) error {
			prepareCalls++
			return nil
		})

		var hookSessions []string
		rootObj := pendingSession("runroot", herorunv1alpha1.SessionSpec{
			RunID:              "run-1",
			SandboxName:        "run-sb-1",
			WorkspaceClaimName: "ws-claim-1",
			InitialPrompt:      "Root work.",
		})
		childObj := pendingSession("runchild", herorunv1alpha1.SessionSpec{
			RunID:              "run-1",
			Parent:             "runroot",
			Ancestors:          []string{"runroot"},
			SandboxName:        "run-sb-1",
			WorkspaceClaimName: "ws-claim-1",
			InitialPrompt:      "Child work.",
		})
		env := newTestEnv(t, fakeLLM, []client.Object{rootObj, childObj}, func(o *Options) {
			o.HarnessFactory = func(string) (*session.Harness, error) { return harness, nil }
			o.SandboxOwnerFor = func(_ context.Context, obj *herorunv1alpha1.Session) (*session.SandboxOwner, error) {
				hookSessions = append(hookSessions, obj.Name)
				return sharedOwner, nil
			}
		})

		var seenSandboxes []string
		for _, name := range []string{"runroot", "runchild"} {
			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: name}}
			if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
				t.Fatalf("Reconcile(%s): %v", name, err)
			}
			got := env.getSession(t, name)
			if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded {
				t.Fatalf("%s Phase = %q, want Succeeded (err=%q)", name, got.Status.Phase, got.Status.Error)
			}
			msgs, _, err := env.store.LoadSession(t.Context(), name)
			if err != nil {
				t.Fatalf("LoadSession(%s): %v", name, err)
			}
			for _, m := range msgs {
				if m.Role == "tool" && m.Content != nil {
					seenSandboxes = append(seenSandboxes, *m.Content)
				}
			}
		}

		if !slices.Equal(hookSessions, []string{"runroot", "runchild"}) {
			t.Errorf("SandboxOwnerFor called for %v, want [runroot runchild]", hookSessions)
		}
		if prepareCalls != 1 {
			t.Errorf("prepareCalls = %d, want 1 (shared across root and child)", prepareCalls)
		}
		wantPtr := fmt.Sprintf("%p", sharedOwner.ActiveSandbox())
		if !slices.Equal(seenSandboxes, []string{wantPtr, wantPtr}) {
			t.Fatalf("expected both sessions to execute in %s, got %v", wantPtr, seenSandboxes)
		}

		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(t.Context(), "run-sb-1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get Sandbox run-sb-1: %v", err)
		}
		if sb.Labels["ao.gke.io/run"] != "run-1" {
			t.Errorf("Sandbox label ao.gke.io/run = %q, want run-1", sb.Labels["ao.gke.io/run"])
		}
		if sb.Spec.PodTemplate.ObjectMeta.Labels[session.PortalInterceptLabel] != "true" {
			t.Errorf("expected PortalIntercept label on Sandbox pod template, got %v", sb.Spec.PodTemplate.ObjectMeta.Labels)
		}
		if len(sb.OwnerReferences) != 1 || sb.OwnerReferences[0].Kind != "HeroRun" || sb.OwnerReferences[0].UID != "hr-uid-1" {
			t.Errorf("Sandbox OwnerReferences = %+v, want HeroRun hr-uid-1", sb.OwnerReferences)
		}
		if sharedOwner.ExtendLifecycleEnabled() {
			t.Error("expected ExtendLifecycleEnabled=false to remain preserved on shared owner")
		}
	})

	t.Run("standalone session skips hook and keeps default sandbox behavior", func(t *testing.T) {
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			toolCallReply("call-1", "echo_sandbox", nil),
			textReply("Standalone done."),
		}}
		harness, agentsCS := newFakeSandboxHarness(t, fakeLLM)

		hookCalled := false
		standaloneObj := pendingSession("standalone1", herorunv1alpha1.SessionSpec{
			InitialPrompt: "Standalone work.",
		})
		env := newTestEnv(t, fakeLLM, []client.Object{standaloneObj}, func(o *Options) {
			o.HarnessFactory = func(string) (*session.Harness, error) { return harness, nil }
			o.SandboxOwnerFor = func(_ context.Context, _ *herorunv1alpha1.Session) (*session.SandboxOwner, error) {
				hookCalled = true
				return nil, errors.New("unexpected call for standalone session")
			}
		})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "standalone1"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if hookCalled {
			t.Fatal("SandboxOwnerFor was called for standalone session")
		}
		got := env.getSession(t, "standalone1")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded {
			t.Fatalf("Phase = %q, want Succeeded (err=%q)", got.Status.Phase, got.Status.Error)
		}
		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(t.Context(), "standalone1", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("expected default Sandbox named standalone1: %v", err)
		}
		if sb.Spec.PodTemplate.ObjectMeta.Labels[session.PortalInterceptLabel] != "" {
			t.Errorf("unexpected PortalIntercept label on standalone sandbox: %v", sb.Spec.PodTemplate.ObjectMeta.Labels)
		}
	})
}

func TestReconcile_SharedRunTurnBudget(t *testing.T) {
	t.Run("sessions share budget and release only when root finishes", func(t *testing.T) {
		const runID = "run-shared-budget"
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			// Child takes 2 turns and completes.
			toolCallReply("child-1", "run_command", map[string]any{"command": "echo child"}),
			textReply("Child complete."),
			// Root gets 1 remaining turn out of 3, then hits MaxTurns.
			toolCallReply("root-1", "run_command", map[string]any{"command": "echo root"}),
			toolCallReply("root-2", "run_command", map[string]any{"command": "echo exceeded"}),
		}}
		childObj := pendingSession("budgetchild", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			Parent:        "budgetroot",
			Ancestors:     []string{"budgetroot"},
			InitialPrompt: "Child task.",
			MaxTurns:      3,
		})
		rootObj := pendingSession("budgetroot", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Root task.",
			MaxTurns:      3,
		})
		env := newTestEnv(t, fakeLLM, []client.Object{childObj, rootObj})

		childReq := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "budgetchild"}}
		if _, err := env.reconciler.Reconcile(t.Context(), childReq); err != nil {
			t.Fatalf("Reconcile child: %v", err)
		}
		childGot := env.getSession(t, "budgetchild")
		if childGot.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || childGot.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted || childGot.Status.Turns != 2 {
			t.Fatalf("child status = %+v, want Succeeded/Completed with Turns=2", childGot.Status)
		}

		// Budget must still be retained after child finishes (only root releases it).
		env.reconciler.runBudgetsMu.Lock()
		retained := env.reconciler.runBudgets[runID]
		env.reconciler.runBudgetsMu.Unlock()
		if retained == nil || retained.Used() != 2 {
			t.Fatalf("retained budget after child = %v, want non-nil with Used=2", retained)
		}

		rootReq := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "budgetroot"}}
		if _, err := env.reconciler.Reconcile(t.Context(), rootReq); err != nil {
			t.Fatalf("Reconcile root: %v", err)
		}
		rootGot := env.getSession(t, "budgetroot")
		if rootGot.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || rootGot.Status.Outcome != herorunv1alpha1.SessionOutcomeMaxTurns {
			t.Fatalf("root status = %+v, want Succeeded/MaxTurns", rootGot.Status)
		}
		if rootGot.Status.Turns != 1 || fakeLLM.calls != 3 {
			t.Errorf("root Turns=%d total LLM calls=%d, want 1 and 3", rootGot.Status.Turns, fakeLLM.calls)
		}

		// Budget must be released once the root session finishes.
		env.reconciler.runBudgetsMu.Lock()
		_, stillPresent := env.reconciler.runBudgets[runID]
		env.reconciler.runBudgetsMu.Unlock()
		if stillPresent {
			t.Errorf("expected run budget for %q to be released after root session finished", runID)
		}
	})

	t.Run("seeded from existing transcripts across run sessions on restart", func(t *testing.T) {
		const runID = "run-restart-budget"
		storeDir := t.TempDir()
		store := session.NewFileStore(storeDir)

		sysPrompt, userPrompt, toolRes, childReply := "sys", "do work", "ok", "child done"
		if err := store.NewSession(t.Context(), "restartroot", session.Metadata{RunID: runID, CreatedAt: time.Now()}); err != nil {
			t.Fatalf("NewSession root: %v", err)
		}
		if err := store.AppendMessages(t.Context(), "restartroot",
			llm.Message{Role: "system", Content: &sysPrompt},
			llm.Message{Role: "user", Content: &userPrompt},
			llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: "c1", Type: "function", Function: llm.FunctionCall{Name: "run_command", Arguments: `{"command":"echo 1"}`}}}},
			llm.Message{Role: "tool", ToolCallID: "c1", Content: &toolRes},
		); err != nil {
			t.Fatalf("AppendMessages root: %v", err)
		}
		if err := store.NewSession(t.Context(), "restartchild", session.Metadata{RunID: runID, Parent: "restartroot", Ancestors: []string{"restartroot"}, CreatedAt: time.Now()}); err != nil {
			t.Fatalf("NewSession child: %v", err)
		}
		if err := store.AppendMessages(t.Context(), "restartchild",
			llm.Message{Role: "system", Content: &sysPrompt},
			llm.Message{Role: "user", Content: &userPrompt},
			llm.Message{Role: "assistant", Content: &childReply},
		); err != nil {
			t.Fatalf("AppendMessages child: %v", err)
		}

		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			toolCallReply("c2", "run_command", map[string]any{"command": "echo 2"}),
			toolCallReply("c3", "run_command", map[string]any{"command": "echo 3"}),
		}}
		rootObj := pendingSession("restartroot", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "do work",
			MaxTurns:      3,
		})
		rootObj.Status.Phase = herorunv1alpha1.SessionPhaseRunning
		started := metav1.NewTime(time.Date(2026, 11, 24, 11, 55, 0, 0, time.UTC))
		rootObj.Status.StartedAt = &started

		env := newTestEnv(t, fakeLLM, []client.Object{rootObj}, func(o *Options) {
			o.SessionStore = store
		})

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "restartroot"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		got := env.getSession(t, "restartroot")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeMaxTurns {
			t.Fatalf("status = %+v, want Succeeded/MaxTurns", got.Status)
		}
		// 1 prior root turn + 1 prior child turn = 2 seeded; only 1 more turn allowed before MaxTurns=3.
		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls after restart = %d, want 1", fakeLLM.calls)
		}
		if got.Status.Turns != 2 {
			t.Errorf("root Turns = %d, want 2", got.Status.Turns)
		}
		env.reconciler.runBudgetsMu.Lock()
		_, stillPresent := env.reconciler.runBudgets[runID]
		env.reconciler.runBudgetsMu.Unlock()
		if stillPresent {
			t.Errorf("expected run budget for %q to be released after root finished", runID)
		}
	})
}

func TestReconcile_TransientCancellation(t *testing.T) {
	t.Run("gRPC codes.Canceled before deadline is non-terminal and resumes", func(t *testing.T) {
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			toolCallReply("call-1", "run_command", map[string]any{"command": "echo step1"}),
			func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				return nil, fmt.Errorf("sandbox stream interrupted: %w", status.Error(codes.Canceled, "transport is closing"))
			},
			textReply("Resumed after gRPC cancel."),
		}}
		deadline := metav1.NewTime(time.Date(2026, 11, 24, 12, 30, 0, 0, time.UTC))
		sessObj := pendingSession("sessgrpccancel", herorunv1alpha1.SessionSpec{
			InitialPrompt: "Multi-step work.",
			Deadline:      &deadline,
		})
		env := newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessgrpccancel"}}

		res, err := env.reconciler.Reconcile(t.Context(), req)
		if err != nil {
			t.Fatalf("first Reconcile error = %v, want nil", err)
		}
		if res.RequeueAfter <= 0 {
			t.Errorf("first Reconcile RequeueAfter = %v, want > 0", res.RequeueAfter)
		}

		mid := env.getSession(t, "sessgrpccancel")
		if mid.Status.Phase != herorunv1alpha1.SessionPhaseRunning || mid.Status.Outcome != "" || mid.Status.Error != "" || mid.Status.FinishedAt != nil {
			t.Fatalf("status after gRPC cancel = %+v, want non-terminal Running", mid.Status)
		}
		if mid.Status.Turns != 1 {
			t.Errorf("Turns after turn 1 = %d, want 1", mid.Status.Turns)
		}

		res, err = env.reconciler.Reconcile(t.Context(), req)
		if err != nil || res != (reconcile.Result{}) {
			t.Fatalf("second Reconcile = (%+v, %v), want empty result and nil error", res, err)
		}
		final := env.getSession(t, "sessgrpccancel")
		if final.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || final.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted || final.Status.Turns != 2 {
			t.Fatalf("final status = %+v, want Succeeded/Completed with Turns=2", final.Status)
		}
	})

	t.Run("context.Canceled before deadline is non-terminal and resumes", func(t *testing.T) {
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				return nil, fmt.Errorf("transient cancel: %w", context.Canceled)
			},
			textReply("Resumed after context cancel."),
		}}
		deadline := metav1.NewTime(time.Date(2026, 11, 24, 12, 30, 0, 0, time.UTC))
		sessObj := pendingSession("sessctxcancel", herorunv1alpha1.SessionSpec{
			InitialPrompt: "Work.",
			Deadline:      &deadline,
		})
		env := newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessctxcancel"}}

		res, err := env.reconciler.Reconcile(t.Context(), req)
		if err != nil || res.RequeueAfter <= 0 {
			t.Fatalf("first Reconcile = (%+v, %v), want RequeueAfter > 0 and nil error", res, err)
		}
		if got := env.getSession(t, "sessctxcancel"); got.Status.Phase != herorunv1alpha1.SessionPhaseRunning || got.Status.Outcome != "" {
			t.Fatalf("mid status = %+v, want non-terminal Running", got.Status)
		}

		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("second Reconcile: %v", err)
		}
		if final := env.getSession(t, "sessctxcancel"); final.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || final.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
			t.Fatalf("final status = %+v, want Succeeded/Completed", final.Status)
		}
	})

	t.Run("gRPC codes.Canceled after deadline marks DeadlineExceeded", func(t *testing.T) {
		deadline := metav1.NewTime(time.Date(2026, 11, 24, 12, 5, 0, 0, time.UTC))
		var env *testEnv
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				env.mu.Lock()
				env.now = deadline.Time.Add(time.Second)
				env.mu.Unlock()
				return nil, status.Error(codes.Canceled, "canceled at deadline")
			},
		}}
		sessObj := pendingSession("sesscanceldeadline", herorunv1alpha1.SessionSpec{
			RunID:         "run-cancel-deadline",
			InitialPrompt: "Work.",
			MaxTurns:      5,
			Deadline:      &deadline,
		})
		env = newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesscanceldeadline"}}

		res, err := env.reconciler.Reconcile(t.Context(), req)
		if err != nil || res != (reconcile.Result{}) {
			t.Fatalf("Reconcile = (%+v, %v), want empty result and nil error", res, err)
		}
		got := env.getSession(t, "sesscanceldeadline")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeDeadlineExceeded {
			t.Fatalf("status = %+v, want Failed/DeadlineExceeded", got.Status)
		}
		env.reconciler.runBudgetsMu.Lock()
		_, stillPresent := env.reconciler.runBudgets["run-cancel-deadline"]
		env.reconciler.runBudgetsMu.Unlock()
		if stillPresent {
			t.Error("expected run budget to be released after deadline exceeded")
		}
	})
}

func newSessionMessage(name string, createdAt time.Time, spec herorunv1alpha1.SessionMessageSpec) *herorunv1alpha1.SessionMessage {
	return &herorunv1alpha1.SessionMessage{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ao-sandboxes",
			CreationTimestamp: metav1.NewTime(createdAt),
			Labels:            spec.Labels(),
		},
		Spec: spec,
	}
}

func (e *testEnv) getSessionMessage(t *testing.T, name string) *herorunv1alpha1.SessionMessage {
	t.Helper()
	var got herorunv1alpha1.SessionMessage
	if err := e.client.Get(t.Context(), types.NamespacedName{Namespace: "ao-sandboxes", Name: name}, &got); err != nil {
		t.Fatalf("Get SessionMessage %s: %v", name, err)
	}
	return &got
}

type failingSessionStore struct {
	session.Store
	mu     sync.Mutex
	failOn func(messages []llm.Message) error
}

func (f *failingSessionStore) AppendMessages(ctx context.Context, sessionID string, messages ...llm.Message) error {
	f.mu.Lock()
	fn := f.failOn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(messages); err != nil {
			return err
		}
	}
	return f.Store.AppendMessages(ctx, sessionID, messages...)
}

func TestSetupWithManager(t *testing.T) {
	scheme := apiruntime.NewScheme()
	if err := herorunv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	skipNameValidation := true
	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:65534"}, ctrl.Options{
		Scheme:                 scheme,
		Logger:                 klog.NewKlogr(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Cache: crcache.Options{
			DefaultNamespaces: map[string]crcache.Config{
				"ao-sandboxes": {},
			},
		},
		Controller: crconfig.Controller{
			SkipNameValidation: &skipNameValidation,
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	r, err := NewReconciler(Options{
		Client:       mgr.GetClient(),
		SessionStore: session.NewFileStore(t.TempDir()),
		HarnessFactory: func(string) (*session.Harness, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager failed: %v", err)
	}
}

func TestReconcile_PreModelMessageDelivery(t *testing.T) {
	t.Run("delivers undelivered messages in creation order before first model call", func(t *testing.T) {
		const runID = "run-premodel"
		t0 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

		// Name msg-z has an earlier creation timestamp than msg-a to verify CreationTimestamp ordering.
		msgFirst := newSessionMessage("msg-z", t0.Add(time.Second), herorunv1alpha1.SessionMessageSpec{
			SessionName: "sesspre",
			RunID:       runID,
			From:        "peer1",
			Text:        "first peer note",
		})
		msgSecond := newSessionMessage("msg-a", t0.Add(2*time.Second), herorunv1alpha1.SessionMessageSpec{
			SessionName: "sesspre",
			RunID:       runID,
			Text:        "second user note",
		})
		alreadyDeliveredAt := metav1.NewTime(t0)
		msgAlready := newSessionMessage("msg-already", t0, herorunv1alpha1.SessionMessageSpec{
			SessionName: "sesspre",
			RunID:       runID,
			Text:        "old already delivered",
		})
		msgAlready.Status.DeliveredAt = &alreadyDeliveredAt
		msgOther := newSessionMessage("msg-other", t0, herorunv1alpha1.SessionMessageSpec{
			SessionName: "other",
			RunID:       runID,
			Text:        "for another session",
		})

		sessObj := pendingSession("sesspre", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Start work.",
		})

		var seenUserContents []string
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, m := range req.Messages {
					if m.Role == "user" && m.Content != nil {
						seenUserContents = append(seenUserContents, *m.Content)
					}
				}
				return textReply("Handled both messages.")(t.Context(), req)
			},
		}}

		env := newTestEnv(t, fakeLLM, []client.Object{sessObj, msgFirst, msgSecond, msgAlready, msgOther})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesspre"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		wantUsers := []string{
			"Start work.",
			"Message from peer1: first peer note",
			"second user note",
		}
		if !slices.Equal(seenUserContents, wantUsers) {
			t.Errorf("LLM saw user messages = %q, want %q", seenUserContents, wantUsers)
		}

		for _, name := range []string{"msg-z", "msg-a"} {
			got := env.getSessionMessage(t, name)
			if !got.IsDelivered() {
				t.Errorf("SessionMessage %s DeliveredAt = nil, want non-nil", name)
			}
		}
		if got := env.getSessionMessage(t, "msg-other"); got.IsDelivered() {
			t.Errorf("SessionMessage msg-other should not have been delivered, got DeliveredAt=%v", got.Status.DeliveredAt)
		}
		if got := env.getSession(t, "sesspre"); got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
			t.Fatalf("status = %+v, want Succeeded/Completed", got.Status)
		}
	})

	t.Run("delivers message arriving during a tool call before next model call", func(t *testing.T) {
		const runID = "run-midtool"
		var env *testEnv
		var sawMidToolMsg bool

		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				// Enqueue a SessionMessage while the turn is running a tool call.
				msg := newSessionMessage("msg-midtool", env.now.Add(time.Second), herorunv1alpha1.SessionMessageSpec{
					SessionName: "sessmidtool",
					RunID:       runID,
					From:        "peer2",
					Text:        "abort benchmark and inspect cache",
				})
				if err := env.client.Create(ctx, msg); err != nil {
					return nil, err
				}
				return toolCallReply("call-1", "run_command", map[string]any{"command": "echo hi"})(ctx, req)
			},
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, m := range req.Messages {
					if m.Role == "user" && m.Content != nil && *m.Content == "Message from peer2: abort benchmark and inspect cache" {
						sawMidToolMsg = true
					}
				}
				return textReply("Inspected cache.")(ctx, req)
			},
		}}

		sessObj := pendingSession("sessmidtool", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Run benchmark.",
		})
		env = newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessmidtool"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if !sawMidToolMsg {
			t.Errorf("second model call did not see message that arrived before it")
		}
		if got := env.getSessionMessage(t, "msg-midtool"); !got.IsDelivered() {
			t.Errorf("msg-midtool was not marked delivered")
		}
	})
}

func TestReconcile_MidTurnMessageContinuation(t *testing.T) {
	t.Run("continues running when message arrives during turn that ends normally", func(t *testing.T) {
		const runID = "run-continue"
		var env *testEnv
		var secondCallUsers []string

		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				// Message arrives while turn 1 is executing its final LLM call.
				msg := newSessionMessage("msg-midturn", env.now.Add(time.Second), herorunv1alpha1.SessionMessageSpec{
					SessionName: "sesscont",
					RunID:       runID,
					Text:        "Please also check memory allocations.",
				})
				if err := env.client.Create(ctx, msg); err != nil {
					return nil, err
				}
				return textReply("Finished initial pass.")(ctx, req)
			},
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, m := range req.Messages {
					if m.Role == "user" && m.Content != nil {
						secondCallUsers = append(secondCallUsers, *m.Content)
					}
				}
				return textReply("Checked memory allocations too.")(ctx, req)
			},
		}}

		sessObj := pendingSession("sesscont", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Optimize CPU.",
		})
		env = newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesscont"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if fakeLLM.calls != 2 {
			t.Fatalf("LLM calls = %d, want 2", fakeLLM.calls)
		}
		wantUsers := []string{"Optimize CPU.", "Please also check memory allocations."}
		if !slices.Equal(secondCallUsers, wantUsers) {
			t.Errorf("second call user messages = %q, want %q", secondCallUsers, wantUsers)
		}

		// Ensure the session never transitioned to a terminal phase before turn 2 completed.
		phases := env.recordedPhases()
		if len(phases) < 2 {
			t.Fatalf("recorded phases = %v, want at least 2 transitions", phases)
		}
		for i, p := range phases[:len(phases)-1] {
			if p.IsTerminal() {
				t.Errorf("recorded premature terminal phase %s at index %d (all phases: %v)", p, i, phases)
			}
		}

		got := env.getSession(t, "sesscont")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted {
			t.Fatalf("status = %+v, want Succeeded/Completed", got.Status)
		}
		if got.Status.Turns != 2 || got.Status.MessageCount != 5 {
			t.Errorf("Turns=%d MessageCount=%d, want 2 and 5", got.Status.Turns, got.Status.MessageCount)
		}
		if msg := env.getSessionMessage(t, "msg-midturn"); !msg.IsDelivered() {
			t.Errorf("msg-midturn was not marked delivered")
		}
	})

	t.Run("does not continue after explicit finish tool", func(t *testing.T) {
		const runID = "run-finish-no-cont"
		var env *testEnv

		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				msg := newSessionMessage("msg-after-finish", env.now.Add(time.Second), herorunv1alpha1.SessionMessageSpec{
					SessionName: "sessfincont",
					RunID:       runID,
					Text:        "late message",
				})
				if err := env.client.Create(ctx, msg); err != nil {
					return nil, err
				}
				return toolCallReply("call-fin", "finish", map[string]any{"summary": "all done"})(ctx, req)
			},
		}}

		sessObj := pendingSession("sessfincont", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Work.",
		})
		env = newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessfincont"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls = %d, want 1", fakeLLM.calls)
		}
		got := env.getSession(t, "sessfincont")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || got.Status.Outcome != herorunv1alpha1.SessionOutcomeFinished {
			t.Fatalf("status = %+v, want Succeeded/Finished", got.Status)
		}
		if msg := env.getSessionMessage(t, "msg-after-finish"); msg.IsDelivered() {
			t.Errorf("late message should not have been delivered after finish")
		}
	})

	t.Run("stops continuation if deadline passed before next turn", func(t *testing.T) {
		const runID = "run-cont-deadline"
		deadline := metav1.NewTime(time.Date(2026, 11, 24, 12, 5, 0, 0, time.UTC))
		var env *testEnv

		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				msg := newSessionMessage("msg-deadline", deadline.Time, herorunv1alpha1.SessionMessageSpec{
					SessionName: "sesscontdl",
					RunID:       runID,
					Text:        "too late",
				})
				if err := env.client.Create(ctx, msg); err != nil {
					return nil, err
				}
				env.mu.Lock()
				env.now = deadline.Time.Add(time.Second)
				env.mu.Unlock()
				return textReply("Turn 1 complete.")(ctx, req)
			},
		}}

		sessObj := pendingSession("sesscontdl", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Work.",
			Deadline:      &deadline,
		})
		env = newTestEnv(t, fakeLLM, []client.Object{sessObj})
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sesscontdl"}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if fakeLLM.calls != 1 {
			t.Errorf("LLM calls = %d, want 1", fakeLLM.calls)
		}
		got := env.getSession(t, "sesscontdl")
		if got.Status.Phase != herorunv1alpha1.SessionPhaseFailed || got.Status.Outcome != herorunv1alpha1.SessionOutcomeDeadlineExceeded {
			t.Fatalf("status = %+v, want Failed/DeadlineExceeded", got.Status)
		}
		if msg := env.getSessionMessage(t, "msg-deadline"); msg.IsDelivered() {
			t.Errorf("msg-deadline should remain undelivered when deadline expired before continuation")
		}
	})
}

func TestReconcile_MessageWriteFailureRetention(t *testing.T) {
	const runID = "run-writefail"
	t0 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	msg1 := newSessionMessage("msg-1", t0.Add(time.Second), herorunv1alpha1.SessionMessageSpec{
		SessionName: "sessfailwrite",
		RunID:       runID,
		Text:        "first ok",
	})
	msg2 := newSessionMessage("msg-2", t0.Add(2*time.Second), herorunv1alpha1.SessionMessageSpec{
		SessionName: "sessfailwrite",
		RunID:       runID,
		Text:        "must survive write error",
	})
	msg3 := newSessionMessage("msg-3", t0.Add(3*time.Second), herorunv1alpha1.SessionMessageSpec{
		SessionName: "sessfailwrite",
		RunID:       runID,
		Text:        "third untouched",
	})

	baseStore := session.NewFileStore(t.TempDir())
	failingStore := &failingSessionStore{
		Store: baseStore,
		failOn: func(messages []llm.Message) error {
			for _, m := range messages {
				if m.Content != nil && *m.Content == "must survive write error" {
					return errors.New("disk full")
				}
			}
			return nil
		},
	}

	sessObj := pendingSession("sessfailwrite", herorunv1alpha1.SessionSpec{
		RunID:         runID,
		InitialPrompt: "Start.",
	})
	fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		textReply("should not be called"),
	}}

	env := newTestEnv(t, fakeLLM, []client.Object{sessObj, msg1, msg2, msg3}, func(o *Options) {
		o.SessionStore = failingStore
	})
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessfailwrite"}}
	if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if fakeLLM.calls != 0 {
		t.Errorf("LLM calls = %d, want 0 when message delivery write failed", fakeLLM.calls)
	}
	gotSess := env.getSession(t, "sessfailwrite")
	if gotSess.Status.Phase != herorunv1alpha1.SessionPhaseFailed || !strings.Contains(gotSess.Status.Error, "disk full") {
		t.Fatalf("status = %+v, want Failed with disk full error", gotSess.Status)
	}

	// msg-1 succeeded and was marked delivered; msg-2 failed AddMessages and msg-3 was not reached, so both remain undelivered.
	if !env.getSessionMessage(t, "msg-1").IsDelivered() {
		t.Errorf("msg-1 should have been marked delivered after AddMessages succeeded")
	}
	if env.getSessionMessage(t, "msg-2").IsDelivered() {
		t.Errorf("msg-2 must NOT be marked delivered when AddMessages failed")
	}
	if env.getSessionMessage(t, "msg-3").IsDelivered() {
		t.Errorf("msg-3 must NOT be marked delivered after earlier write failure")
	}

	// Once the store recovers, delivering pending messages delivers msg-2 and msg-3 without duplicating msg-1.
	failingStore.mu.Lock()
	failingStore.failOn = nil
	failingStore.mu.Unlock()

	h, err := env.reconciler.opts.HarnessFactory("basic")
	if err != nil {
		t.Fatalf("HarnessFactory: %v", err)
	}
	loaded, err := h.LoadSession(t.Context(), failingStore, "sessfailwrite")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if err := env.reconciler.deliverPendingMessages(t.Context(), gotSess, loaded); err != nil {
		t.Fatalf("deliverPendingMessages after recovery: %v", err)
	}

	var userTexts []string
	for _, m := range loaded.Messages() {
		if m.Role == "user" && m.Content != nil {
			userTexts = append(userTexts, *m.Content)
		}
	}
	wantTexts := []string{"Start.", "first ok", "must survive write error", "third untouched"}
	if !slices.Equal(userTexts, wantTexts) {
		t.Errorf("transcript user messages after recovery = %q, want %q", userTexts, wantTexts)
	}
	for _, name := range []string{"msg-1", "msg-2", "msg-3"} {
		if !env.getSessionMessage(t, name).IsDelivered() {
			t.Errorf("%s should be marked delivered after recovery", name)
		}
	}
}

func TestReconcile_MessageSurvivesRestart(t *testing.T) {
	const runID = "run-restart-msg"
	t0 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	store := session.NewFileStore(t.TempDir())

	// Seed a session whose first turn finished before a simulated controller restart,
	// leaving Session non-terminal (Running) and an undelivered SessionMessage in the API.
	sysPrompt, initPrompt, turn1Reply := "default system prompt", "Initial task.", "Initial reply."
	if err := store.NewSession(t.Context(), "sessrestart", session.Metadata{RunID: runID, CreatedAt: t0}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := store.AppendMessages(t.Context(), "sessrestart",
		llm.Message{Role: "system", Content: &sysPrompt},
		llm.Message{Role: "user", Content: &initPrompt},
		llm.Message{Role: "assistant", Content: &turn1Reply},
	); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}

	sessObj := pendingSession("sessrestart", herorunv1alpha1.SessionSpec{
		RunID:         runID,
		InitialPrompt: initPrompt,
	})
	sessObj.Status.Phase = herorunv1alpha1.SessionPhaseRunning
	started := metav1.NewTime(t0)
	sessObj.Status.StartedAt = &started

	pendingMsg := newSessionMessage("msg-survive", t0.Add(time.Second), herorunv1alpha1.SessionMessageSpec{
		SessionName: "sessrestart",
		RunID:       runID,
		From:        "peer-a",
		Text:        "follow up after restart",
	})

	// Reconciler 1 exists only to establish shared Kubernetes client and store; Reconciler 2 is a fresh instance.
	env1 := newTestEnv(t, &scriptedLLM{}, []client.Object{sessObj, pendingMsg}, func(o *Options) {
		o.SessionStore = store
	})

	var sawRestartMsg bool
	llm2 := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
		func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			for _, m := range req.Messages {
				if m.Role == "user" && m.Content != nil && *m.Content == "Message from peer-a: follow up after restart" {
					sawRestartMsg = true
				}
			}
			return textReply("Handled after restart.")(ctx, req)
		},
	}}

	reconciler2, err := NewReconciler(Options{
		Client:       env1.client,
		SessionStore: store,
		HarnessFactory: func(_ string) (*session.Harness, error) {
			reg := tools.NewRegistry()
			reg.Add(&tools.RunCommand{})
			var runOpts session.RunOptions
			runOpts.InitDefaults()
			runOpts.ModelName = "default-model"
			runOpts.SystemPrompt = "default system prompt"
			h := session.NewHarness(llm2, nil, reg, runOpts)
			h.SetSandboxOverride(&fakeSandbox{})
			return h, nil
		},
		DefaultToolset: "basic",
		Clock:          func() time.Time { return t0.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatalf("NewReconciler(2): %v", err)
	}

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sessrestart"}}
	if _, err := reconciler2.Reconcile(t.Context(), req); err != nil {
		t.Fatalf("reconciler2.Reconcile: %v", err)
	}

	if !sawRestartMsg {
		t.Errorf("restarted reconciler did not deliver pending SessionMessage to model")
	}
	if llm2.calls != 1 {
		t.Errorf("llm2 calls = %d, want 1", llm2.calls)
	}
	gotSess := env1.getSession(t, "sessrestart")
	if gotSess.Status.Phase != herorunv1alpha1.SessionPhaseSucceeded || gotSess.Status.Outcome != herorunv1alpha1.SessionOutcomeCompleted || gotSess.Status.Turns != 2 {
		t.Fatalf("status = %+v, want Succeeded/Completed with Turns=2", gotSess.Status)
	}
	if gotMsg := env1.getSessionMessage(t, "msg-survive"); !gotMsg.IsDelivered() {
		t.Errorf("msg-survive was not marked delivered after restart")
	}
}

func TestReconcile_SendMessage(t *testing.T) {
	t.Run("send_message tool creates owned and labeled SessionMessage delivered to peer", func(t *testing.T) {
		const runID = "run-sendmsg"
		sender := pendingSession("sender", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Notify peer.",
		})
		sender.UID = "sender-uid"

		peer := pendingSession("peer", herorunv1alpha1.SessionSpec{
			RunID:         runID,
			InitialPrompt: "Wait for note.",
		})
		peer.UID = "peer-uid"

		var toolReplyContent string
		var peerSeenUsers []string
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			toolCallReply("call-sm-1", "send_message", map[string]any{
				"recipient": "peer",
				"message":   "profile results in profile.txt",
			}),
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, m := range req.Messages {
					if m.Role == "tool" && m.ToolCallID == "call-sm-1" && m.Content != nil {
						toolReplyContent = *m.Content
					}
				}
				return textReply("Notified peer.")(ctx, req)
			},
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, m := range req.Messages {
					if m.Role == "user" && m.Content != nil {
						peerSeenUsers = append(peerSeenUsers, *m.Content)
					}
				}
				return textReply("Read profile.txt.")(ctx, req)
			},
		}}

		env := newTestEnv(t, fakeLLM, []client.Object{sender, peer})

		senderReq := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "sender"}}
		if _, err := env.reconciler.Reconcile(t.Context(), senderReq); err != nil {
			t.Fatalf("Reconcile sender: %v", err)
		}
		if toolReplyContent != "Message sent to peer." {
			t.Errorf("send_message tool result = %q, want %q", toolReplyContent, "Message sent to peer.")
		}

		var list herorunv1alpha1.SessionMessageList
		if err := env.client.List(t.Context(), &list,
			client.InNamespace("ao-sandboxes"),
			client.MatchingLabels{
				herorunv1alpha1.LabelSession: "peer",
				herorunv1alpha1.LabelRun:     runID,
			},
		); err != nil {
			t.Fatalf("List SessionMessages: %v", err)
		}
		if len(list.Items) != 1 {
			t.Fatalf("got %d SessionMessages for peer, want 1", len(list.Items))
		}
		created := list.Items[0]
		wantSpec := herorunv1alpha1.SessionMessageSpec{
			SessionName: "peer",
			RunID:       runID,
			From:        "sender",
			Text:        "profile results in profile.txt",
		}
		if created.Spec != wantSpec {
			t.Errorf("SessionMessage.Spec = %+v, want %+v", created.Spec, wantSpec)
		}
		if created.Labels[herorunv1alpha1.LabelSession] != "peer" || created.Labels[herorunv1alpha1.LabelRun] != runID {
			t.Errorf("SessionMessage.Labels = %v, want %v", created.Labels, wantSpec.Labels())
		}
		if len(created.OwnerReferences) != 1 {
			t.Fatalf("OwnerReferences = %+v, want 1 controller owner ref", created.OwnerReferences)
		}
		ref := created.OwnerReferences[0]
		if ref.Kind != "Session" || ref.Name != "peer" || ref.UID != peer.UID || ref.Controller == nil || !*ref.Controller {
			t.Errorf("OwnerReference = %+v, want controller ref to peer (%s)", ref, peer.UID)
		}
		if created.IsDelivered() {
			t.Errorf("newly created SessionMessage should not be delivered yet")
		}

		// Reconciling peer delivers the SessionMessage and marks DeliveredAt.
		peerReq := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: "peer"}}
		if _, err := env.reconciler.Reconcile(t.Context(), peerReq); err != nil {
			t.Fatalf("Reconcile peer: %v", err)
		}
		wantPeerUsers := []string{"Wait for note.", "Message from sender: profile results in profile.txt"}
		if !slices.Equal(peerSeenUsers, wantPeerUsers) {
			t.Errorf("peer saw user messages = %q, want %q", peerSeenUsers, wantPeerUsers)
		}
		if got := env.getSessionMessage(t, created.Name); !got.IsDelivered() {
			t.Errorf("SessionMessage %s was not marked delivered after peer reconciled", created.Name)
		}
	})

	t.Run("multiple send_message calls preserve order with fixed clock", func(t *testing.T) {
		const runID = "run-sm-order"
		sender := pendingSession("senderord", herorunv1alpha1.SessionSpec{RunID: runID})
		peer := pendingSession("peerord", herorunv1alpha1.SessionSpec{RunID: runID, InitialPrompt: "Go."})
		peer.UID = "peerord-uid"

		var peerUsers []string
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, m := range req.Messages {
					if m.Role == "user" && m.Content != nil {
						peerUsers = append(peerUsers, *m.Content)
					}
				}
				return textReply("Done.")(ctx, req)
			},
		}}

		env := newTestEnv(t, fakeLLM, []client.Object{sender, peer})
		for i := 1; i <= 3; i++ {
			if err := env.reconciler.sendMessage(t.Context(), sender, peer.Name, fmt.Sprintf("step %d", i)); err != nil {
				t.Fatalf("sendMessage %d: %v", i, err)
			}
		}

		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: peer.Name}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile peer: %v", err)
		}
		want := []string{
			"Go.",
			"Message from senderord: step 1",
			"Message from senderord: step 2",
			"Message from senderord: step 3",
		}
		if !slices.Equal(peerUsers, want) {
			t.Errorf("peer saw user messages = %q, want %q", peerUsers, want)
		}
	})

	t.Run("validation rejects invalid recipients and standalone sessions omit send_message", func(t *testing.T) {
		const runID = "run-sm-val"
		sender := pendingSession("valsender", herorunv1alpha1.SessionSpec{RunID: runID})
		otherRun := pendingSession("valother", herorunv1alpha1.SessionSpec{RunID: "different-run"})
		succeeded := pendingSession("valsucceeded", herorunv1alpha1.SessionSpec{RunID: runID})
		succeeded.Status.Phase = herorunv1alpha1.SessionPhaseSucceeded
		failed := pendingSession("valfailed", herorunv1alpha1.SessionSpec{RunID: runID})
		failed.Status.Phase = herorunv1alpha1.SessionPhaseFailed
		canceled := pendingSession("valcanceled", herorunv1alpha1.SessionSpec{RunID: runID})
		canceled.Status.Phase = herorunv1alpha1.SessionPhaseCanceled
		waiting := pendingSession("valwaiting", herorunv1alpha1.SessionSpec{RunID: runID})
		waiting.UID = "valwaiting-uid"
		waiting.Status.Phase = herorunv1alpha1.SessionPhaseWaiting
		standalone := pendingSession("valstandalone", herorunv1alpha1.SessionSpec{InitialPrompt: "Solo."})

		var standaloneTools []string
		fakeLLM := &scriptedLLM{handlers: []func(context.Context, llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error){
			func(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
				for _, tool := range req.Tools {
					standaloneTools = append(standaloneTools, tool.Function.Name)
				}
				return textReply("Solo done.")(ctx, req)
			},
		}}

		env := newTestEnv(t, fakeLLM, []client.Object{sender, otherRun, succeeded, failed, canceled, waiting, standalone})

		cases := []struct {
			name      string
			from      *herorunv1alpha1.Session
			recipient string
			wantErr   string
		}{
			{name: "no run", from: standalone, recipient: "valwaiting", wantErr: "session belongs to no run"},
			{name: "self", from: sender, recipient: "valsender", wantErr: "cannot send message to self"},
			{name: "missing", from: sender, recipient: "doesnotexist", wantErr: `recipient session "doesnotexist" not found`},
			{name: "different run", from: sender, recipient: "valother", wantErr: "not in the same run"},
			{name: "succeeded", from: sender, recipient: "valsucceeded", wantErr: "is not running"},
			{name: "failed", from: sender, recipient: "valfailed", wantErr: "is not running"},
			{name: "canceled", from: sender, recipient: "valcanceled", wantErr: "is not running"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := env.reconciler.sendMessage(t.Context(), tc.from, tc.recipient, "hello")
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("sendMessage(%s -> %s) = %v, want error containing %q", tc.from.Name, tc.recipient, err, tc.wantErr)
				}
			})
		}

		// Waiting (non-terminal) peer in same run is accepted.
		if err := env.reconciler.sendMessage(t.Context(), sender, waiting.Name, "wake up"); err != nil {
			t.Errorf("sendMessage to Waiting peer failed: %v", err)
		}

		// Standalone session must not advertise send_message tool.
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ao-sandboxes", Name: standalone.Name}}
		if _, err := env.reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatalf("Reconcile standalone: %v", err)
		}
		if slices.Contains(standaloneTools, "send_message") {
			t.Errorf("standalone session should not expose send_message tool, got tools: %v", standaloneTools)
		}
	})
}
