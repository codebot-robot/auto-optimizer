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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

// DefaultWaitTimeout is used when a wait call does not specify a timeout.
const DefaultWaitTimeout = time.Minute

var _ tools.Tool = &WaitTool{}

// WaitFunc blocks until a message arrives for the calling session, a child
// session stops, the timeout elapses, or the run ends or reaches its deadline.
type WaitFunc func(ctx context.Context, timeout time.Duration) (string, error)

// WaitTool pauses a session without spending model turns until an event arrives.
type WaitTool struct {
	waiter WaitFunc

	Timeout string `json:"timeout"`
}

// NewWaitTool returns a new WaitTool instance.
func NewWaitTool(waiter WaitFunc) *WaitTool {
	return &WaitTool{waiter: waiter}
}

// Schema implements tools.Tool.
func (t *WaitTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "wait",
			Description: "Pause until a message arrives for this session or one of its child sessions stops, " +
				"instead of calling list_sessions in a loop. Waiting uses no model turns.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"timeout": map[string]any{
						"type":        "string",
						"description": `Maximum time to wait as a Go duration such as "30s" or "5m". Defaults to 1m.`,
					},
				},
			},
		},
	}
}

// Run implements tools.Tool.
func (t *WaitTool) Run(ctx context.Context, _ tools.Sandbox) (llm.Message, error) {
	raw := strings.TrimSpace(t.Timeout)
	t.Timeout = ""
	if t.waiter == nil {
		return llm.Message{}, errors.New("cannot call wait: session belongs to no run")
	}
	timeout := DefaultWaitTimeout
	if raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return llm.Message{}, fmt.Errorf("invalid timeout %q: %w", raw, err)
		}
		if d <= 0 {
			return llm.Message{}, errors.New("timeout must be positive")
		}
		timeout = d
	}
	content, err := t.waiter(ctx, timeout)
	if err != nil {
		return llm.Message{}, err
	}
	return llm.Message{Role: "tool", Content: &content}, nil
}
