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

package llm

import (
	"encoding/json"
	"testing"
)

func userMessage(text string) Message {
	return Message{Role: "user", Content: &text}
}

func TestElizaReflectsUserMessage(t *testing.T) {
	client := &ElizaClient{}

	resp, err := client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Messages: []Message{userMessage("What is the capital of France?")},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}

	got := *resp.Choices[0].Message.Content
	want := "What do you think it means when you say 'What is the capital of France?'?"
	if got != want {
		t.Errorf("reply = %q, want %q", got, want)
	}
	if len(resp.Choices[0].Message.ToolCalls) != 0 {
		t.Errorf("unexpected tool calls: %v", resp.Choices[0].Message.ToolCalls)
	}
}

func TestElizaRunCommand(t *testing.T) {
	client := &ElizaClient{}

	resp, err := client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Messages: []Message{userMessage("run: uname -a")},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}

	toolCalls := resp.Choices[0].Message.ToolCalls
	if len(toolCalls) != 1 {
		t.Fatalf("tool calls = %v, want exactly one", toolCalls)
	}
	if toolCalls[0].Function.Name != "run_command" {
		t.Errorf("tool name = %q, want run_command", toolCalls[0].Function.Name)
	}

	var args struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal([]byte(toolCalls[0].Function.Arguments), &args); err != nil {
		t.Fatalf("unmarshaling arguments %q: %v", toolCalls[0].Function.Arguments, err)
	}
	if args.Command != "uname -a" {
		t.Errorf("command = %q, want %q", args.Command, "uname -a")
	}
}

func TestElizaReportsToolResult(t *testing.T) {
	client := &ElizaClient{}

	toolOutput := "stdout:\nLinux"
	resp, err := client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Messages: []Message{
			userMessage("run: uname"),
			{Role: "tool", ToolCallID: "eliza-run-1", Content: &toolOutput},
		},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}

	got := *resp.Choices[0].Message.Content
	want := "The tool told me:\nstdout:\nLinux"
	if got != want {
		t.Errorf("reply = %q, want %q", got, want)
	}
}

func TestElizaReadReadme(t *testing.T) {
	client := &ElizaClient{}

	systemMsg := Message{Role: "system", Content: ptr("read the README and summarise it")}
	userMsg := Message{Role: "user", Content: ptr("Please work on the goal.")}

	// 1. With read_file in tools
	resp, err := client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Messages: []Message{systemMsg, userMsg},
		Tools: []Tool{
			{Function: ToolFunction{Name: "read_file"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}
	if len(resp.Choices[0].Message.ToolCalls) != 1 {
		t.Fatalf("expected 1 tool call, got %d", len(resp.Choices[0].Message.ToolCalls))
	}
	tc := resp.Choices[0].Message.ToolCalls[0]
	if tc.Function.Name != "read_file" {
		t.Errorf("tool name = %q, want read_file", tc.Function.Name)
	}
	if tc.Function.Arguments != `{"file_path":"/workspace/repo/README.md"}` {
		t.Errorf("arguments = %q, want file_path", tc.Function.Arguments)
	}

	// 2. Tool result produces final summary
	toolOutput := "# My Project\nThis is a cool project."
	resp, err = client.CreateChatCompletion(t.Context(), ChatCompletionRequest{
		Messages: []Message{
			systemMsg,
			userMsg,
			{Role: "tool", ToolCallID: tc.ID, Content: &toolOutput},
		},
	})
	if err != nil {
		t.Fatalf("CreateChatCompletion: %v", err)
	}
	if len(resp.Choices[0].Message.ToolCalls) != 0 {
		t.Errorf("expected 0 tool calls after tool result, got %d", len(resp.Choices[0].Message.ToolCalls))
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Errorf("finish reason = %q, want stop", resp.Choices[0].FinishReason)
	}
}

func ptr(s string) *string {
	return &s
}
