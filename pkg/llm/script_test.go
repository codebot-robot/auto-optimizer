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
	"os"
	"path/filepath"
	"testing"
)

func TestScriptClient_ThreeStepExecution(t *testing.T) {
	script := `[
		{"text": "inspecting"},
		{"tool": "run_command", "args": {"command": "ls"}},
		{"text": "done"}
	]`
	client, err := NewScriptClient([]byte(script))
	if err != nil {
		t.Fatalf("NewScriptClient: %v", err)
	}

	calls := []struct {
		content, finishReason, toolName, args string
	}{
		{content: "inspecting", finishReason: "stop"},
		{finishReason: "tool_calls", toolName: "run_command", args: `{"command": "ls"}`},
		{content: "done", finishReason: "stop"},
		{content: "Script finished.", finishReason: "stop"},
		{content: "Script finished.", finishReason: "stop"},
	}
	for i, want := range calls {
		resp, err := client.CreateChatCompletion(t.Context(), ChatCompletionRequest{})
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if resp.ID != FakeModelScript {
			t.Errorf("call %d ID = %q, want %q", i, resp.ID, FakeModelScript)
		}
		c := resp.Choices[0]
		if c.FinishReason != want.finishReason {
			t.Errorf("call %d FinishReason = %q, want %q", i, c.FinishReason, want.finishReason)
		}
		if want.content != "" && (c.Message.Content == nil || *c.Message.Content != want.content) {
			t.Errorf("call %d content = %v, want %q", i, c.Message.Content, want.content)
		}
		if want.toolName != "" {
			if len(c.Message.ToolCalls) != 1 || c.Message.ToolCalls[0].Function.Name != want.toolName || c.Message.ToolCalls[0].Function.Arguments != want.args {
				t.Errorf("call %d tool call = %+v, want %s(%s)", i, c.Message.ToolCalls, want.toolName, want.args)
			}
		}
	}
}

func TestScriptClient_UniqueToolCallIDs(t *testing.T) {
	c, _ := NewScriptClient([]byte(`[{"tool": "a"}, {"tool": "b"}]`))
	r1, _ := c.CreateChatCompletion(t.Context(), ChatCompletionRequest{})
	r2, _ := c.CreateChatCompletion(t.Context(), ChatCompletionRequest{})
	id1, id2 := r1.Choices[0].Message.ToolCalls[0].ID, r2.Choices[0].Message.ToolCalls[0].ID
	if id1 == id2 || r1.Choices[0].Message.ToolCalls[0].Function.Arguments != "{}" {
		t.Errorf("IDs: %q vs %q, args: %q", id1, id2, r1.Choices[0].Message.ToolCalls[0].Function.Arguments)
	}
}

func TestScriptClient_MalformedFailsAtConstruction(t *testing.T) {
	invalids := []string{
		"", "   ", "[{invalid", `{"text": "hi"}`, `[{}]`, `[{"tool": ""}]`,
		`[{"text": "a", "tool": "b"}]`, `[{"text": "a", "args": {}}]`,
		`[{"tool": "a", "args": "bad"}]`,
	}
	for _, bad := range invalids {
		if _, err := NewScriptClient([]byte(bad)); err == nil {
			t.Errorf("NewScriptClient(%q) succeeded, want error", bad)
		}
	}
}

func TestNewFromEnv_FakeModelScript(t *testing.T) {
	assertReply := func(t *testing.T, script, file, wantReply string) {
		t.Helper()
		t.Setenv("AO_FAKE_SCRIPT", script)
		t.Setenv("AO_FAKE_SCRIPT_FILE", file)
		c, err := NewFromEnv(FakeModelScript)
		if err != nil {
			t.Fatalf("NewFromEnv: %v", err)
		}
		resp, err := c.CreateChatCompletion(t.Context(), ChatCompletionRequest{})
		if err != nil || resp.Choices[0].Message.Content == nil || *resp.Choices[0].Message.Content != wantReply {
			t.Fatalf("CreateChatCompletion: got %v, err %v, want %q", resp, err, wantReply)
		}
	}

	t.Run("inline script", func(t *testing.T) {
		assertReply(t, `[{"text": "from env"}]`, "", "from env")
	})

	t.Run("script file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "s.json")
		_ = os.WriteFile(path, []byte(`[{"text": "from file"}]`), 0600)
		assertReply(t, "", path, "from file")
	})

	t.Run("missing and malformed errors", func(t *testing.T) {
		for _, tc := range [][2]string{{"", ""}, {"", "/nonexistent"}, {"bad-json", ""}} {
			t.Setenv("AO_FAKE_SCRIPT", tc[0])
			t.Setenv("AO_FAKE_SCRIPT_FILE", tc[1])
			if _, err := NewFromEnv(FakeModelScript); err == nil {
				t.Errorf("NewFromEnv(%q, %q) succeeded, want error", tc[0], tc[1])
			}
		}
	})

	t.Run("FakeModelEliza unaffected", func(t *testing.T) {
		c, err := NewFromEnv(FakeModelEliza)
		if _, ok := c.(*ElizaClient); err != nil || !ok {
			t.Errorf("NewFromEnv(FakeModelEliza) = (%T, %v), want *ElizaClient", c, err)
		}
	})
}
