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

package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

func TestTurnBudget_Unit(t *testing.T) {
	t.Run("nil budget allows unlimited", func(t *testing.T) {
		var b *TurnBudget
		if !b.Take() {
			t.Error("expected nil budget Take to return true")
		}
		if b.MaxTurns() != 0 {
			t.Errorf("MaxTurns = %d, want 0", b.MaxTurns())
		}
		if b.Used() != 0 {
			t.Errorf("Used = %d, want 0", b.Used())
		}
	})

	t.Run("unlimited budget", func(t *testing.T) {
		b := NewTurnBudget(0)
		for i := 0; i < 100; i++ {
			if !b.Take() {
				t.Fatalf("take %d failed for unlimited budget", i)
			}
		}
		if b.Used() != 100 {
			t.Errorf("Used = %d, want 100", b.Used())
		}
	})

	t.Run("bounded budget", func(t *testing.T) {
		limit := 3
		b := NewTurnBudget(limit)
		if b.MaxTurns() != limit {
			t.Errorf("MaxTurns = %d, want %d", b.MaxTurns(), limit)
		}

		for i := 0; i < limit; i++ {
			if !b.Take() {
				t.Fatalf("take %d failed", i)
			}
		}

		if b.Used() != limit {
			t.Errorf("Used = %d, want %d", b.Used(), limit)
		}

		// Subsequent calls must return false
		if b.Take() {
			t.Error("Take succeeded when budget should be exhausted")
		}
		if b.Used() != limit {
			t.Errorf("Used changed after exhaustion: %d", b.Used())
		}
	})
}

type budgetTestLLM struct {
	mu           sync.Mutex
	callCount    int
	maxTextTurns int // number of text responses before looping with tool calls
}

func (l *budgetTestLLM) CreateChatCompletion(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.callCount++
	callIdx := l.callCount

	if l.maxTextTurns > 0 && callIdx <= l.maxTextTurns {
		text := fmt.Sprintf("text reply %d", callIdx)
		return &llm.ChatCompletionResponse{
			ID: fmt.Sprintf("resp-%d", callIdx),
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role:    "assistant",
						Content: &text,
					},
					FinishReason: "stop",
				},
			},
		}, nil
	}

	return &llm.ChatCompletionResponse{
		ID: fmt.Sprintf("resp-%d", callIdx),
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   fmt.Sprintf("call-%d", callIdx),
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "run_command",
								Arguments: `{"command":"echo test"}`,
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func newTestHarnessWithBudget(t *testing.T, llmClient llm.ChatClient, budget *TurnBudget) (*Harness, Store) {
	t.Helper()

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get(basic): %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts RunOptions
	opts.InitDefaults()
	opts.ModelName = "test-model"

	fakeSB := &recordingSandbox{}
	harness := NewHarness(llmClient, nil, toolsRegistry, opts)
	harness.SetSandboxOverride(fakeSB)
	if budget != nil {
		harness = harness.WithTurnBudget(budget)
	}

	storeDir := t.TempDir()
	sessionStore := NewFileStore(storeDir)

	return harness, sessionStore
}

func TestTurnBudget_TwoSessionsSharedBudget(t *testing.T) {
	ctx := t.Context()

	// Configure an LLM where call 1 is a finished text reply,
	// and subsequent calls return tool calls (looping).
	mockLLM := &budgetTestLLM{maxTextTurns: 1}
	budgetLimit := 3
	budget := NewTurnBudget(budgetLimit)

	harness, store := newTestHarnessWithBudget(t, mockLLM, budget)

	sess1, err := harness.NewSession(ctx, store, "session1", Metadata{RunID: "run123"})
	if err != nil {
		t.Fatalf("NewSession sess1: %v", err)
	}
	sess2, err := harness.NewSession(ctx, store, "session2", Metadata{RunID: "run123"})
	if err != nil {
		t.Fatalf("NewSession sess2: %v", err)
	}

	events1 := &recordingEvents{}
	events2 := &recordingEvents{}

	// 1. Session 1 runs turn 1. It finishes cleanly with text response.
	if err := harness.RunTurn(ctx, sess1, "work task 1", events1); err != nil {
		t.Fatalf("session 1 RunTurn failed: %v", err)
	}
	if budget.Used() != 1 {
		t.Fatalf("expected 1 turn used after session 1, got %d", budget.Used())
	}

	// 2. Session 2 runs. It loops with tool calls:
	// Turn 2 is used (tool call executed).
	// Turn 3 is used (tool call executed).
	// Turn 4 exceeds limit (3 >= 3) -> ends with ErrMaxTurns.
	err = harness.RunTurn(ctx, sess2, "work task 2", events2)
	if !errors.Is(err, ErrMaxTurns) {
		t.Fatalf("expected ErrMaxTurns for session 2, got %v", err)
	}

	if budget.Used() != budgetLimit {
		t.Errorf("budget.Used() = %d, want %d", budget.Used(), budgetLimit)
	}

	// 3. Next turn on Session 1 also ends with ErrMaxTurns immediately.
	err = harness.RunTurn(ctx, sess1, "work task 1 again", events1)
	if !errors.Is(err, ErrMaxTurns) {
		t.Fatalf("expected ErrMaxTurns on spent budget for session 1, got %v", err)
	}

	mockLLM.mu.Lock()
	totalCalls := mockLLM.callCount
	mockLLM.mu.Unlock()

	// Verify exactly 3 LLM calls occurred across both sessions together
	if totalCalls != budgetLimit {
		t.Errorf("total LLM calls = %d, want exactly %d", totalCalls, budgetLimit)
	}
}

func TestTurnBudget_SessionWithoutRun_KeepsOwnLimit(t *testing.T) {
	ctx := t.Context()

	// Both calls loop forever with tool calls.
	mockLLM := &budgetTestLLM{maxTextTurns: 0}
	limitPerSession := 2

	// Harness configured with MaxTurns on opts, but NO shared TurnBudget.
	harness, store := newTestHarnessWithBudget(t, mockLLM, nil)
	harness = harness.WithMaxTurns(limitPerSession)

	sess1, err := harness.NewSession(ctx, store, "norunsess1", Metadata{})
	if err != nil {
		t.Fatalf("NewSession sess1: %v", err)
	}
	sess2, err := harness.NewSession(ctx, store, "norunsess2", Metadata{})
	if err != nil {
		t.Fatalf("NewSession sess2: %v", err)
	}

	// Session 1 runs and hits its own limit of 2 turns.
	err = harness.RunTurn(ctx, sess1, "task 1", &recordingEvents{})
	if !errors.Is(err, ErrMaxTurns) {
		t.Fatalf("sess1: expected ErrMaxTurns, got %v", err)
	}

	// Session 2 runs and also gets its own full 2 turns before hitting the limit.
	err = harness.RunTurn(ctx, sess2, "task 2", &recordingEvents{})
	if !errors.Is(err, ErrMaxTurns) {
		t.Fatalf("sess2: expected ErrMaxTurns, got %v", err)
	}

	mockLLM.mu.Lock()
	totalCalls := mockLLM.callCount
	mockLLM.mu.Unlock()

	// Total calls across both sessions is 2 + 2 = 4 (each kept its own limit).
	expectedCalls := limitPerSession * 2
	if totalCalls != expectedCalls {
		t.Errorf("total LLM calls = %d, want %d", totalCalls, expectedCalls)
	}
}
