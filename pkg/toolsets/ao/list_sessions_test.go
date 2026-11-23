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

func TestListSessionsTool_Schema(t *testing.T) {
	tool := NewListSessionsTool(nil)
	schema := tool.Schema()

	if schema.Function.Name != "list_sessions" {
		t.Errorf("schema function name = %q, want list_sessions", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}
	if !strings.Contains(schema.Function.Description, "running or stopped") {
		t.Errorf("expected description to mention 'running or stopped', got %q", schema.Function.Description)
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	if params["type"] != "object" {
		t.Errorf("parameters type = %v, want object", params["type"])
	}
}

func TestListSessionsTool_Run(t *testing.T) {
	ctx := t.Context()

	// 1. Calling tool without lister returns error (session belongs to no run)
	t.Run("nil lister returns error", func(t *testing.T) {
		tool := NewListSessionsTool(nil)
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "session belongs to no run") {
			t.Fatalf("expected 'session belongs to no run' error, got %v", err)
		}
	})

	// 2. Lister returns error
	t.Run("lister error propagated", func(t *testing.T) {
		tool := NewListSessionsTool(func(_ context.Context) ([]SessionInfo, error) {
			return nil, errors.New("storage failure")
		})
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "storage failure") {
			t.Fatalf("expected 'storage failure' error, got %v", err)
		}
	})

	// 3. Successful lister returns sessions JSON
	t.Run("successful list returns sessions JSON", func(t *testing.T) {
		mockSessions := []SessionInfo{
			{Name: "root-sess", Parent: "", State: "running"},
			{Name: "child-sess", Parent: "root-sess", State: "stopped"},
		}
		tool := NewListSessionsTool(func(_ context.Context) ([]SessionInfo, error) {
			return mockSessions, nil
		})

		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.Role != "tool" {
			t.Errorf("msg.Role = %q, want 'tool'", msg.Role)
		}
		if msg.Content == nil {
			t.Fatal("msg.Content is nil")
		}

		var parsed []SessionInfo
		if err := json.Unmarshal([]byte(*msg.Content), &parsed); err != nil {
			t.Fatalf("failed to unmarshal tool content %q: %v", *msg.Content, err)
		}
		if len(parsed) != 2 {
			t.Fatalf("got %d sessions, want 2", len(parsed))
		}
		if parsed[0].Name != "root-sess" || parsed[0].Parent != "" || parsed[0].State != "running" {
			t.Errorf("unexpected parsed[0]: %+v", parsed[0])
		}
		if parsed[1].Name != "child-sess" || parsed[1].Parent != "root-sess" || parsed[1].State != "stopped" {
			t.Errorf("unexpected parsed[1]: %+v", parsed[1])
		}
	})

	// 4. Empty list returns empty array []
	t.Run("empty list returns empty array JSON", func(t *testing.T) {
		tool := NewListSessionsTool(func(_ context.Context) ([]SessionInfo, error) {
			return nil, nil
		})

		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.Content == nil || strings.TrimSpace(*msg.Content) != "[]" {
			t.Errorf("content = %v, want '[]'", msg.Content)
		}
	})

	// 5. Unmarshal JSON into tool
	t.Run("json unmarshal into tool struct", func(t *testing.T) {
		tool := NewListSessionsTool(func(_ context.Context) ([]SessionInfo, error) {
			return []SessionInfo{{Name: "test-sess", State: "running"}}, nil
		})
		if err := json.Unmarshal([]byte("{}"), tool); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("Run err = %v", err)
		}
		if msg.Content == nil || !strings.Contains(*msg.Content, "test-sess") {
			t.Errorf("expected session name in content, got %v", msg.Content)
		}
	})
}
