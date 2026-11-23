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

package ao

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestStartSessionTool_Schema(t *testing.T) {
	tool := NewStartSessionTool(nil)
	schema := tool.Schema()

	if schema.Function.Name != "start_session" {
		t.Errorf("schema function name = %q, want start_session", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}
	if !strings.Contains(schema.Function.Description, "workspace volume and Sandbox") {
		t.Errorf("expected description to mention sharing workspace volume and Sandbox, got %q", schema.Function.Description)
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	req, ok := params["required"].([]string)
	if !ok || len(req) != 1 || req[0] != "task" {
		t.Errorf("required parameters = %v, want ['task']", req)
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	taskProp, ok := props["task"].(map[string]any)
	if !ok || taskProp["type"] != "string" {
		t.Errorf("task property = %v, want type: string", taskProp)
	}
}

func TestStartSessionTool_Run(t *testing.T) {
	ctx := t.Context()

	// 1. Calling tool without starter returns error (session belongs to no run)
	t.Run("nil starter returns error", func(t *testing.T) {
		tool := NewStartSessionTool(nil)
		tool.Task = "analyze code"
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "session belongs to no run") {
			t.Fatalf("expected 'session belongs to no run' error, got %v", err)
		}
	})

	// 2. Empty task returns error
	t.Run("empty task returns error", func(t *testing.T) {
		tool := NewStartSessionTool(func(_ context.Context, _ string) (string, error) {
			return "child1", nil
		})
		tool.Task = "   "
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "task is required") {
			t.Fatalf("expected 'task is required' error, got %v", err)
		}
	})

	// 3. Successful starter call
	t.Run("successful start returns child name", func(t *testing.T) {
		var receivedTask string
		tool := NewStartSessionTool(func(_ context.Context, task string) (string, error) {
			receivedTask = task
			return "runchild123", nil
		})
		tool.Task = "measure baseline latency"

		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedTask != "measure baseline latency" {
			t.Errorf("received task = %q, want 'measure baseline latency'", receivedTask)
		}
		if msg.Role != "tool" {
			t.Errorf("msg.Role = %q, want 'tool'", msg.Role)
		}
		if msg.Content == nil || !strings.Contains(*msg.Content, "runchild123") {
			t.Errorf("expected child name in content, got %v", msg.Content)
		}
	})

	// 4. Starter returns error (e.g. depth limit exceeded)
	t.Run("starter error propagated", func(t *testing.T) {
		tool := NewStartSessionTool(func(_ context.Context, _ string) (string, error) {
			return "", errors.New("depth limit reached")
		})
		tool.Task = "subtask"

		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "depth limit reached") {
			t.Fatalf("expected 'depth limit reached' error, got %v", err)
		}
	})

	// 5. Unmarshal JSON into tool
	t.Run("json unmarshal sets task", func(t *testing.T) {
		var executedTask string
		tool := NewStartSessionTool(func(_ context.Context, task string) (string, error) {
			executedTask = task
			return "childfromjson", nil
		})

		args := `{"task": "inspect config files"}`
		if err := json.Unmarshal([]byte(args), tool); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		if tool.Task != "inspect config files" {
			t.Errorf("Task = %q, want 'inspect config files'", tool.Task)
		}

		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("Run err = %v", err)
		}
		if executedTask != "inspect config files" {
			t.Errorf("executedTask = %q, want 'inspect config files'", executedTask)
		}
		if msg.Content == nil || !strings.Contains(*msg.Content, "childfromjson") {
			t.Errorf("expected child name in content, got %v", msg.Content)
		}
	})
}
