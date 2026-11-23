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
	"time"
)

func TestWaitTool_Schema(t *testing.T) {
	tool := NewWaitTool(nil)
	schema := tool.Schema()

	if schema.Function.Name != "wait" {
		t.Errorf("schema function name = %q, want wait", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	timeoutProp, ok := props["timeout"].(map[string]any)
	if !ok || timeoutProp["type"] != "string" {
		t.Errorf("timeout property = %v, want type: string", timeoutProp)
	}
}

func TestWaitTool_Run(t *testing.T) {
	ctx := t.Context()

	t.Run("nil waiter returns error", func(t *testing.T) {
		tool := NewWaitTool(nil)
		tool.Timeout = "5s"
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "session belongs to no run") {
			t.Fatalf("expected 'session belongs to no run' error, got %v", err)
		}
	})

	t.Run("invalid timeout returns error", func(t *testing.T) {
		tool := NewWaitTool(func(_ context.Context, _ time.Duration) (string, error) {
			return "", nil
		})
		tool.Timeout = "notaduration"
		_, err := tool.Run(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "invalid timeout") {
			t.Fatalf("expected invalid timeout error, got %v", err)
		}
	})

	t.Run("non-positive timeout returns error", func(t *testing.T) {
		for _, val := range []string{"0s", "-5s"} {
			tool := NewWaitTool(func(_ context.Context, _ time.Duration) (string, error) {
				return "", nil
			})
			tool.Timeout = val
			_, err := tool.Run(ctx, nil)
			if err == nil || !strings.Contains(err.Error(), "timeout must be positive") {
				t.Fatalf("timeout=%q: expected positive timeout error, got %v", val, err)
			}
		}
	})

	t.Run("default timeout and reset across reused calls", func(t *testing.T) {
		var got []time.Duration
		tool := NewWaitTool(func(_ context.Context, d time.Duration) (string, error) {
			got = append(got, d)
			return "A message arrived.", nil
		})

		if err := json.Unmarshal([]byte(`{"timeout": "250ms"}`), tool); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		msg, err := tool.Run(ctx, nil)
		if err != nil {
			t.Fatalf("first Run: %v", err)
		}
		if msg.Role != "tool" || msg.Content == nil || *msg.Content != "A message arrived." {
			t.Errorf("unexpected first message: %+v", msg)
		}

		// Second call with empty JSON should fall back to DefaultWaitTimeout, not reuse 250ms.
		if err := json.Unmarshal([]byte(`{}`), tool); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		if _, err := tool.Run(ctx, nil); err != nil {
			t.Fatalf("second Run: %v", err)
		}

		if len(got) != 2 || got[0] != 250*time.Millisecond || got[1] != DefaultWaitTimeout {
			t.Errorf("got durations %v, want [250ms %v]", got, DefaultWaitTimeout)
		}
	})

	t.Run("waiter error propagated", func(t *testing.T) {
		wantErr := errors.New("wait failed")
		tool := NewWaitTool(func(_ context.Context, _ time.Duration) (string, error) {
			return "", wantErr
		})
		tool.Timeout = "1s"
		_, err := tool.Run(ctx, nil)
		if !errors.Is(err, wantErr) {
			t.Fatalf("got err %v, want %v", err, wantErr)
		}
	})
}
