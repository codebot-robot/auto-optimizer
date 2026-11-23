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

func TestSendMessageTool_Schema(t *testing.T) {
	tool := NewSendMessageTool(nil)
	schema := tool.Schema()

	if schema.Function.Name != "send_message" {
		t.Errorf("schema function name = %q, want send_message", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}
	if !strings.Contains(schema.Function.Description, "share the workspace") {
		t.Errorf("expected description to mention sharing workspace, got %q", schema.Function.Description)
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	req, ok := params["required"].([]string)
	if !ok || len(req) != 2 || req[0] != "recipient" || req[1] != "message" {
		t.Errorf("required parameters = %v, want ['recipient', 'message']", req)
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	recProp, ok := props["recipient"].(map[string]any)
	if !ok || recProp["type"] != "string" {
		t.Errorf("recipient property = %v, want type: string", recProp)
	}
	msgProp, ok := props["message"].(map[string]any)
	if !ok || msgProp["type"] != "string" {
		t.Errorf("message property = %v, want type: string", msgProp)
	}
}

func TestSendMessageTool_Run(t *testing.T) {
	ctx := t.Context()

	// 1. Calling tool without sender returns error (session belongs to no run)
	t.Run("nil sender returns error", func(t *testing.T) {
		tool := NewSendMessageTool(nil)
		tool.Recipient = "child1"
		tool.Message = "hello"
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "session belongs to no run") {
			t.Fatalf("expected 'session belongs to no run' error, got %v", err)
		}
	})

	// 2. Missing recipient returns error
	t.Run("empty recipient returns error", func(t *testing.T) {
		tool := NewSendMessageTool(func(_ context.Context, _, _ string) error {
			return nil
		})
		tool.Message = "hello"
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "recipient is required") {
			t.Fatalf("expected 'recipient is required' error, got %v", err)
		}
	})

	// 3. Missing message returns error
	t.Run("empty message returns error", func(t *testing.T) {
		tool := NewSendMessageTool(func(_ context.Context, _, _ string) error {
			return nil
		})
		tool.Recipient = "child1"
		tool.Message = "   "
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "message is required") {
			t.Fatalf("expected 'message is required' error, got %v", err)
		}
	})

	// 4. Successful sender call
	t.Run("successful send returns confirmation", func(t *testing.T) {
		var gotRecipient, gotMessage string
		tool := NewSendMessageTool(func(_ context.Context, recipient, message string) error {
			gotRecipient = recipient
			gotMessage = message
			return nil
		})
		tool.Recipient = "worker-1"
		tool.Message = "see report in results.txt"

		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotRecipient != "worker-1" {
			t.Errorf("got recipient = %q, want 'worker-1'", gotRecipient)
		}
		if gotMessage != "see report in results.txt" {
			t.Errorf("got message = %q, want 'see report in results.txt'", gotMessage)
		}
		if msg.Role != "tool" {
			t.Errorf("msg.Role = %q, want 'tool'", msg.Role)
		}
		if msg.Content == nil || !strings.Contains(*msg.Content, "worker-1") {
			t.Errorf("expected recipient in content, got %v", msg.Content)
		}
	})

	// 5. Sender returns error (e.g. not in same run)
	t.Run("sender error propagated", func(t *testing.T) {
		tool := NewSendMessageTool(func(_ context.Context, _, _ string) error {
			return errors.New("cannot send message to session \"other\": not in the same run")
		})
		tool.Recipient = "other"
		tool.Message = "hello"

		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "not in the same run") {
			t.Fatalf("expected 'not in the same run' error, got %v", err)
		}
	})

	// 6. Unmarshal JSON into tool (recipient and message)
	t.Run("json unmarshal sets recipient and message", func(t *testing.T) {
		var gotRecipient, gotMessage string
		tool := NewSendMessageTool(func(_ context.Context, recipient, message string) error {
			gotRecipient = recipient
			gotMessage = message
			return nil
		})

		args := `{"recipient": "child-a", "message": "check file /workspace/foo"}`
		if err := json.Unmarshal([]byte(args), tool); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		if tool.Recipient != "child-a" {
			t.Errorf("Recipient = %q, want 'child-a'", tool.Recipient)
		}
		if tool.Message != "check file /workspace/foo" {
			t.Errorf("Message = %q, want 'check file /workspace/foo'", tool.Message)
		}

		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("Run err = %v", err)
		}
		if gotRecipient != "child-a" || gotMessage != "check file /workspace/foo" {
			t.Errorf("unexpected call args: recipient=%q msg=%q", gotRecipient, gotMessage)
		}
		if msg.Content == nil || !strings.Contains(*msg.Content, "child-a") {
			t.Errorf("expected child name in content, got %v", msg.Content)
		}
	})
}
