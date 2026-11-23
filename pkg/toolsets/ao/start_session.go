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

// Package ao provides control-plane tools for auto-optimizer sessions.
package ao

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var _ tools.Tool = &StartSessionTool{}

// StartSessionStarter is the callback invoked to create and start a child session.
type StartSessionStarter func(ctx context.Context, task string) (string, error)

// StartSessionTool is a control-plane tool that starts a child session in the
// same run to work on a task in the background.
type StartSessionTool struct {
	starter StartSessionStarter

	Task string `json:"task"`
}

// NewStartSessionTool returns a new StartSessionTool instance.
func NewStartSessionTool(starter StartSessionStarter) *StartSessionTool {
	return &StartSessionTool{
		starter: starter,
	}
}

// Schema implements tools.Tool.
func (t *StartSessionTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "start_session",
			Description: "Call this tool to start a child session in the same run to work on a task. " +
				"The child shares the run's workspace volume and Sandbox with its parent: " +
				"the child sees the same files, so the task can refer to files by path " +
				"and the child can leave results in files. " +
				"Returns immediately with the child session's name; does not wait for the child to finish.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"task": map[string]any{
						"type":        "string",
						"description": "Task for the child session to perform.",
					},
				},
				"required": []string{"task"},
			},
		},
	}
}

// Run implements tools.Tool.
func (t *StartSessionTool) Run(ctx context.Context, _ tools.Sandbox) (llm.Message, error) {
	if t.starter == nil {
		return llm.Message{}, errors.New("cannot call start_session: session belongs to no run")
	}
	task := strings.TrimSpace(t.Task)
	if task == "" {
		return llm.Message{}, errors.New("task is required")
	}

	childName, err := t.starter(ctx, task)
	if err != nil {
		return llm.Message{}, err
	}

	content := fmt.Sprintf("Started child session %s.", childName)
	return llm.Message{
		Role:    "tool",
		Content: &content,
	}, nil
}
