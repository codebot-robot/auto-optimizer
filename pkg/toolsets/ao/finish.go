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

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var _ tools.Tool = &FinishTool{}

// FinishTool is a run-scoped tool that signals the session has finished
// and records a summary of the work done for repository maintainers.
type FinishTool struct {
	Summary string `json:"summary"`
}

// NewFinishTool returns a new FinishTool instance.
func NewFinishTool() *FinishTool {
	return &FinishTool{}
}

// Schema implements tools.Tool.
func (t *FinishTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name:        "finish",
			Description: "Call this tool when you have completed all work and have nothing more to do. Provide a summary of the work done for the repository maintainers.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"summary": map[string]any{
						"type":        "string",
						"description": "Summary of work done and current status, written for repository maintainers.",
					},
				},
				"required": []string{"summary"},
			},
		},
	}
}

// Run implements tools.Tool. It captures the summary and returns
// session.ErrSessionFinished to signal the harness to end the session.
func (t *FinishTool) Run(_ context.Context, _ tools.Sandbox) (llm.Message, error) {
	content := t.Summary
	if content == "" {
		content = "Session finished."
	}
	return llm.Message{
		Role:    "tool",
		Content: &content,
	}, session.ErrSessionFinished
}
