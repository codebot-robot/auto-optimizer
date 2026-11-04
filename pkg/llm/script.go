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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

// FakeModelScript is the model name that selects the scripted fake LLM
// which replays a fixed sequence of tool calls and replies.
const FakeModelScript = "fake-script"

type scriptStep struct {
	Text *string         `json:"text,omitempty"`
	Tool string          `json:"tool,omitempty"`
	Args json.RawMessage `json:"args,omitempty"`
}

// ScriptClient is a fake ChatClient that replays a fixed sequence of steps.
// Each step is either an assistant text reply or a tool call.
type ScriptClient struct {
	mu     sync.Mutex
	steps  []scriptStep
	index  int
	callID int
}

var _ ChatClient = &ScriptClient{}

// NewScriptClient parses a JSON array of steps and returns a ScriptClient.
func NewScriptClient(data []byte) (*ScriptClient, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("llm: script cannot be empty")
	}

	var steps []scriptStep
	if err := json.Unmarshal(data, &steps); err != nil {
		return nil, fmt.Errorf("llm: parsing script JSON: %w", err)
	}

	for i := range steps {
		step := &steps[i]
		step.Tool = strings.TrimSpace(step.Tool)
		if step.Text == nil && step.Tool == "" {
			return nil, fmt.Errorf("llm: step %d must have 'text' or 'tool'", i)
		}
		if step.Text != nil && step.Tool != "" {
			return nil, fmt.Errorf("llm: step %d cannot have both 'text' and 'tool'", i)
		}
		if step.Text != nil && len(step.Args) > 0 {
			return nil, fmt.Errorf("llm: step %d cannot have 'args' with 'text'", i)
		}
		if step.Tool != "" {
			trimmed := bytes.TrimSpace(step.Args)
			if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) {
				var obj map[string]any
				if err := json.Unmarshal(trimmed, &obj); err != nil {
					return nil, fmt.Errorf("llm: step %d tool args must be a JSON object: %w", i, err)
				}
			}
		}
	}

	return &ScriptClient{steps: steps}, nil
}

// NewScriptClientFromEnv returns a ScriptClient configured from the environment.
// It reads the JSON script from AO_FAKE_SCRIPT or, if that is empty, from the file
// named by AO_FAKE_SCRIPT_FILE.
func NewScriptClientFromEnv() (*ScriptClient, error) {
	if s := os.Getenv("AO_FAKE_SCRIPT"); s != "" {
		return NewScriptClient([]byte(s))
	}
	if path := os.Getenv("AO_FAKE_SCRIPT_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("llm: reading AO_FAKE_SCRIPT_FILE %q: %w", path, err)
		}
		return NewScriptClient(data)
	}
	return nil, fmt.Errorf("llm: AO_FAKE_SCRIPT or AO_FAKE_SCRIPT_FILE is required for %s", FakeModelScript)
}

// CreateChatCompletion implements ChatClient.
func (c *ScriptClient) CreateChatCompletion(_ context.Context, _ ChatCompletionRequest) (*ChatCompletionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.index >= len(c.steps) {
		reply := "Script finished."
		return &ChatCompletionResponse{
			ID: FakeModelScript,
			Choices: []Choice{
				{
					Message:      Message{Role: "assistant", Content: &reply},
					FinishReason: "stop",
				},
			},
		}, nil
	}

	step := c.steps[c.index]
	c.index++

	if step.Tool != "" {
		c.callID++
		args := string(bytes.TrimSpace(step.Args))
		if args == "" || args == "null" {
			args = "{}"
		}
		return &ChatCompletionResponse{
			ID: FakeModelScript,
			Choices: []Choice{
				{
					Message: Message{
						Role: "assistant",
						ToolCalls: []ToolCall{
							{
								ID:   fmt.Sprintf("call_%d", c.callID),
								Type: "function",
								Function: FunctionCall{
									Name:      step.Tool,
									Arguments: args,
								},
							},
						},
					},
					FinishReason: "tool_calls",
				},
			},
		}, nil
	}

	reply := *step.Text
	return &ChatCompletionResponse{
		ID: FakeModelScript,
		Choices: []Choice{
			{
				Message:      Message{Role: "assistant", Content: &reply},
				FinishReason: "stop",
			},
		},
	}, nil
}
