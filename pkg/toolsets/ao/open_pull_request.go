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
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var _ tools.Tool = &OpenPullRequestTool{}

// OpenPullRequestTool is a run-scoped tool that records the model's request
// to open a pull request and ends the session so the control plane can take over.
type OpenPullRequestTool struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

// NewOpenPullRequestTool returns a new OpenPullRequestTool instance.
func NewOpenPullRequestTool() *OpenPullRequestTool {
	return &OpenPullRequestTool{}
}

// Schema implements tools.Tool.
func (t *OpenPullRequestTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "open_pull_request",
			Description: "Call this tool to request opening a pull request. The control plane " +
				"reviews, commits and pushes your changes and opens the pull request. " +
				"The session ends after this call.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"title": map[string]any{
						"type":        "string",
						"description": "Title of the pull request.",
					},
					"body": map[string]any{
						"type":        "string",
						"description": "Description of the pull request.",
					},
				},
				"required": []string{"title", "body"},
			},
		},
	}
}

// Run implements tools.Tool. It records the arguments, rejects an empty title
// with a tool error, and returns session.ErrPullRequestRequested to signal
// the control plane to take over.
func (t *OpenPullRequestTool) Run(_ context.Context, _ tools.Sandbox) (llm.Message, error) {
	if strings.TrimSpace(t.Title) == "" {
		return llm.Message{}, errors.New("title is required")
	}

	content := "Pull request recorded. The control plane will review and push your changes."
	return llm.Message{
		Role:    "tool",
		Content: &content,
	}, session.ErrPullRequestRequested
}
