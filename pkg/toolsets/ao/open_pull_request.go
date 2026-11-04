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
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var _ tools.Tool = &OpenPullRequestTool{}

// OpenPullRequestExpectation holds the expected repository URL and branch for a run.
type OpenPullRequestExpectation struct {
	Repository string
	Branch     string
}

// OpenPullRequestTool is a run-scoped tool that records the model's request
// to open a pull request and ends the session so the control plane can take over.
type OpenPullRequestTool struct {
	expected OpenPullRequestExpectation

	Repository string `json:"repository"`
	Branch     string `json:"branch"`
	Title      string `json:"title"`
	Body       string `json:"body"`
}

// NewOpenPullRequestTool returns a new OpenPullRequestTool instance.
func NewOpenPullRequestTool(expected OpenPullRequestExpectation) *OpenPullRequestTool {
	return &OpenPullRequestTool{
		expected: expected,
	}
}

// Schema implements tools.Tool.
func (t *OpenPullRequestTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "open_pull_request",
			Description: "Call this tool to request opening a pull request. " +
				"You must commit your changes with git before calling this tool: " +
				"the control plane pushes the commits that exist on the branch and " +
				"opens the pull request from them; nothing uncommitted is included. " +
				"The session ends after this call.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"repository": map[string]any{
						"type":        "string",
						"description": "Repository URL for this run as given in the repository facts.",
					},
					"branch": map[string]any{
						"type":        "string",
						"description": "Branch name for this run as given in the repository facts.",
					},
					"title": map[string]any{
						"type":        "string",
						"description": "Title of the pull request.",
					},
					"body": map[string]any{
						"type":        "string",
						"description": "Description of the pull request.",
					},
				},
				"required": []string{"repository", "branch", "title", "body"},
			},
		},
	}
}

// Run implements tools.Tool. It verifies the repository URL and branch match
// the expected values, rejects an empty title with a tool error, verifies the
// working tree is clean via git status in the sandbox, and returns
// session.ErrPullRequestRequested to signal the control plane to take over.
func (t *OpenPullRequestTool) Run(ctx context.Context, sandbox tools.Sandbox) (llm.Message, error) {
	if !repoMatches(t.Repository, t.expected.Repository) {
		return llm.Message{}, fmt.Errorf("unexpected repository %q, expected %q", t.Repository, t.expected.Repository)
	}
	if t.Branch != t.expected.Branch {
		return llm.Message{}, fmt.Errorf("unexpected branch %q, expected %q", t.Branch, t.expected.Branch)
	}
	if strings.TrimSpace(t.Title) == "" {
		return llm.Message{}, errors.New("title is required")
	}

	if sandbox == nil {
		return llm.Message{}, errors.New("sandbox is required")
	}

	res, err := sandbox.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", "/workspace/repo", "status", "--porcelain"},
	})
	if err != nil {
		return llm.Message{}, fmt.Errorf("git status: %w", err)
	}
	if res == nil {
		return llm.Message{}, errors.New("git status returned nil result")
	}
	if res.ExitCode != 0 {
		stderr := strings.TrimSpace(res.Stderr)
		if stderr != "" {
			return llm.Message{}, fmt.Errorf("git status failed with exit code %d: %s", res.ExitCode, stderr)
		}
		return llm.Message{}, fmt.Errorf("git status failed with exit code %d", res.ExitCode)
	}

	if strings.TrimSpace(res.Stdout) != "" {
		var paths []string
		for _, line := range strings.Split(res.Stdout, "\n") {
			line = strings.TrimRight(line, "\r")
			if strings.TrimSpace(line) == "" {
				continue
			}
			path := strings.TrimSpace(line)
			if len(line) > 3 {
				path = strings.TrimSpace(line[3:])
			}
			paths = append(paths, path)
			if len(paths) == 5 {
				break
			}
		}
		msg := "the working tree has uncommitted changes; commit them with git, then call open_pull_request again"
		if len(paths) > 0 {
			msg += ": " + strings.Join(paths, ", ")
		}
		return llm.Message{}, errors.New(msg)
	}

	content := "Pull request recorded. The control plane will review and push your changes."
	return llm.Message{
		Role:    "tool",
		Content: &content,
	}, session.ErrPullRequestRequested
}

func repoMatches(got, expected string) bool {
	return normalizeRepo(got) == normalizeRepo(expected)
}

func normalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	for {
		trimmed := strings.TrimSuffix(repo, "/")
		trimmed = strings.TrimSuffix(trimmed, ".git")
		if trimmed == repo {
			break
		}
		repo = trimmed
	}
	return repo
}
