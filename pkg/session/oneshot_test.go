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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

// recordingSandbox is an in-memory double of tools.Sandbox that records
// commands executed against it without requiring a Kubernetes cluster or network.
type recordingSandbox struct {
	mu    sync.Mutex
	calls []tools.ExecCommandOptions
}

func (s *recordingSandbox) ExecCommand(_ context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, opts)

	return &tools.ExecCommandResult{
		Stdout:   "Linux fake-test-node 6.6.0 #1 SMP\n",
		Stderr:   "",
		ExitCode: 0,
	}, nil
}

type recordingEvents struct {
	assistantMessages []string
	approvedCalls     []llm.ToolCall
}

func (e *recordingEvents) AssistantMessage(_ context.Context, text string) {
	e.assistantMessages = append(e.assistantMessages, text)
}

func (e *recordingEvents) ApproveToolCall(_ context.Context, call llm.ToolCall) (bool, error) {
	e.approvedCalls = append(e.approvedCalls, call)
	return true, nil
}

func (e *recordingEvents) ToolCallStarted(_ context.Context, _ llm.ToolCall) {}

func (e *recordingEvents) ToolCallFinished(_ context.Context, _ llm.ToolCall, _ error) {}

func (e *recordingEvents) MessageAppended(_ context.Context, _ llm.Message) {}

func TestOneShot_FakeEliza(t *testing.T) {
	ctx := t.Context()

	llmClient, err := llm.NewFromEnv("fake-eliza")
	if err != nil {
		t.Fatalf("NewFromEnv(fake-eliza): %v", err)
	}

	toolset, err := toolsets.Get("basic")
	if err != nil {
		t.Fatalf("toolsets.Get(basic): %v", err)
	}

	toolsRegistry := tools.NewRegistry()
	toolsRegistry.ToolTimeout = DefaultToolTimeout
	toolset.RegisterTools(toolsRegistry)

	var opts RunOptions
	opts.InitDefaults()
	opts.ModelName = "fake-eliza"
	opts.SystemPrompt = toolset.SystemPrompt()

	fakeSB := &recordingSandbox{}

	harness := NewHarness(llmClient, nil, toolsRegistry, opts)
	harness.SetSandboxOverride(fakeSB)

	storeDir := t.TempDir()
	sessionStore := NewFileStore(storeDir)

	sess, err := harness.NewSession(ctx, sessionStore, "oneshottest", Metadata{Toolset: "basic"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if err := harness.EnsureSystemPrompt(ctx, sess); err != nil {
		t.Fatalf("EnsureSystemPrompt: %v", err)
	}

	events := &recordingEvents{}
	prompt := "run: uname -a"
	if err := harness.RunTurn(ctx, sess, prompt, events); err != nil {
		t.Fatalf("RunTurn failed: %v", err)
	}

	// Verify fake sandbox recorded the command
	fakeSB.mu.Lock()
	callCount := len(fakeSB.calls)
	var lastCmd []string
	if callCount > 0 {
		lastCmd = fakeSB.calls[0].Command
	}
	fakeSB.mu.Unlock()

	if callCount != 1 {
		t.Fatalf("expected 1 command executed in sandbox, got %d", callCount)
	}
	expectedCmd := []string{"sh", "-c", "uname -a"}
	if !slices.Equal(lastCmd, expectedCmd) {
		t.Errorf("command = %v, want %v", lastCmd, expectedCmd)
	}

	// Verify tool call approval
	if len(events.approvedCalls) != 1 {
		t.Fatalf("expected 1 approved tool call, got %d", len(events.approvedCalls))
	}
	if events.approvedCalls[0].Function.Name != "run_command" {
		t.Errorf("approved tool name = %q, want run_command", events.approvedCalls[0].Function.Name)
	}

	// Verify assistant messages contain "Linux"
	foundLinux := false
	for _, msg := range events.assistantMessages {
		if strings.Contains(msg, "Linux") {
			foundLinux = true
			break
		}
	}
	if !foundLinux {
		t.Errorf("assistant messages %v did not contain 'Linux'", events.assistantMessages)
	}

	// Verify session history
	msgs := sess.Messages()
	if len(msgs) < 3 {
		t.Fatalf("expected at least 3 messages in history (user, tool call, tool result, assistant), got %d", len(msgs))
	}
}

type fakeFinishTool struct {
	Summary string `json:"summary"`
}

func (f *fakeFinishTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "finish",
		},
	}
}

func (f *fakeFinishTool) Run(_ context.Context, _ tools.Sandbox) (llm.Message, error) {
	content := f.Summary
	return llm.Message{Content: &content}, ErrSessionFinished
}

type fakeFailingTool struct{}

func (f *fakeFailingTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "failing_tool",
		},
	}
}

func (f *fakeFailingTool) Run(_ context.Context, _ tools.Sandbox) (llm.Message, error) {
	return llm.Message{}, errors.New("something went wrong")
}

type scriptedLLM struct {
	responses []*llm.ChatCompletionResponse
	idx       int
}

func (s *scriptedLLM) CreateChatCompletion(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	if s.idx >= len(s.responses) {
		return nil, errors.New("unexpected call to LLM")
	}
	resp := s.responses[s.idx]
	s.idx++
	return resp, nil
}

func TestHarness_WithTools_RunScopedTool_FinishesSession(t *testing.T) {
	ctx := t.Context()

	scripted := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			{
				ID: "resp-1",
				Choices: []llm.Choice{
					{
						Message: llm.Message{
							Role: "assistant",
							ToolCalls: []llm.ToolCall{
								{
									ID:   "call-finish",
									Type: "function",
									Function: llm.FunctionCall{
										Name:      "finish",
										Arguments: `{"summary":"session completed"}`,
									},
								},
							},
						},
						FinishReason: "tool_calls",
					},
				},
			},
		},
	}

	baseRegistry := tools.NewRegistry()
	baseHarness := NewHarness(scripted, nil, baseRegistry, RunOptions{})

	finishTool := &fakeFinishTool{}
	runHarness := baseHarness.WithTools(finishTool)

	// Verify original harness was not mutated
	if len(baseHarness.extraTools) != 0 {
		t.Errorf("baseHarness.extraTools length = %d, want 0", len(baseHarness.extraTools))
	}
	if len(baseRegistry.All()) != 0 {
		t.Errorf("baseRegistry has tools, want empty")
	}

	storeDir := t.TempDir()
	sessionStore := NewFileStore(storeDir)
	sess, err := runHarness.NewSession(ctx, sessionStore, "finishsess", Metadata{})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	events := &recordingEvents{}
	err = runHarness.RunTurn(ctx, sess, "start work", events)
	if !errors.Is(err, ErrSessionFinished) {
		t.Fatalf("RunTurn err = %v, want ErrSessionFinished", err)
	}

	// Verify finishTool captured arguments
	if finishTool.Summary != "session completed" {
		t.Errorf("finishTool.Summary = %q, want 'session completed'", finishTool.Summary)
	}

	// Verify transcript in session history contains the tool result
	msgs := sess.Messages()
	var foundToolMsg bool
	for _, m := range msgs {
		if m.Role == "tool" && m.Content != nil && *m.Content == "session completed" {
			foundToolMsg = true
			if m.ToolCallID != "call-finish" {
				t.Errorf("ToolCallID = %q, want call-finish", m.ToolCallID)
			}
		}
	}
	if !foundToolMsg {
		t.Errorf("expected tool message with content 'session completed' in transcript, got: %+v", msgs)
	}
}

func TestHarness_WithTools_OrdinaryError_ContinuesSession(t *testing.T) {
	ctx := t.Context()

	finalReply := "I recovered from the failing tool."
	scripted := &scriptedLLM{
		responses: []*llm.ChatCompletionResponse{
			{
				ID: "resp-1",
				Choices: []llm.Choice{
					{
						Message: llm.Message{
							Role: "assistant",
							ToolCalls: []llm.ToolCall{
								{
									ID:   "call-fail",
									Type: "function",
									Function: llm.FunctionCall{
										Name:      "failing_tool",
										Arguments: `{}`,
									},
								},
							},
						},
						FinishReason: "tool_calls",
					},
				},
			},
			{
				ID: "resp-2",
				Choices: []llm.Choice{
					{
						Message: llm.Message{
							Role:    "assistant",
							Content: &finalReply,
						},
						FinishReason: "stop",
					},
				},
			},
		},
	}

	baseRegistry := tools.NewRegistry()
	failingTool := &fakeFailingTool{}
	runHarness := NewHarness(scripted, nil, baseRegistry, RunOptions{}).WithTools(failingTool)

	storeDir := t.TempDir()
	sessionStore := NewFileStore(storeDir)
	sess, err := runHarness.NewSession(ctx, sessionStore, "errorsess", Metadata{})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	events := &recordingEvents{}
	err = runHarness.RunTurn(ctx, sess, "do work", events)
	if err != nil {
		t.Fatalf("RunTurn failed unexpectedly: %v", err)
	}

	// Verify transcript has recorded the error tool message
	msgs := sess.Messages()
	var foundErrorMsg bool
	for _, m := range msgs {
		if m.Role == "tool" && m.Content != nil && strings.Contains(*m.Content, "something went wrong") {
			foundErrorMsg = true
		}
	}
	if !foundErrorMsg {
		t.Errorf("expected error tool message in transcript, got: %+v", msgs)
	}

	// Verify assistant received next turn and stopped with finalReply
	if len(events.assistantMessages) == 0 || events.assistantMessages[len(events.assistantMessages)-1] != finalReply {
		t.Errorf("expected final assistant message %q, got %v", finalReply, events.assistantMessages)
	}
}
