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
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/session"
)

func TestOpenPullRequestTool_Schema(t *testing.T) {
	expected := OpenPullRequestExpectation{
		Repository: "https://github.com/example/repo",
		Branch:     "ao/goal/run-1",
	}
	tool := NewOpenPullRequestTool(expected)
	schema := tool.Schema()

	if schema.Function.Name != "open_pull_request" {
		t.Errorf("schema function name = %q, want open_pull_request", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	req, ok := params["required"].([]string)
	wantReq := []string{"repository", "branch", "title", "body"}
	if !ok || !slices.Equal(req, wantReq) {
		t.Errorf("required parameters = %v, want %v", req, wantReq)
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	for _, prop := range wantReq {
		p, ok := props[prop].(map[string]any)
		if !ok || p["type"] != "string" {
			t.Errorf("property %q = %v, want type: string", prop, p)
		}
		if desc, ok := p["description"].(string); !ok || desc == "" {
			t.Errorf("property %q description is empty", prop)
		}
	}
}

func TestOpenPullRequestTool_Run(t *testing.T) {
	ctx := t.Context()
	expected := OpenPullRequestExpectation{
		Repository: "https://github.com/example/repo",
		Branch:     "ao/goal/run-1",
	}

	// 1. Run with matching repository, branch, title, body succeeds with sentinel
	tool := NewOpenPullRequestTool(expected)
	tool.Repository = "https://github.com/example/repo"
	tool.Branch = "ao/goal/run-1"
	tool.Title = "Optimize database index queries"
	tool.Body = "Adds compound index on (repo, goal, run_id)."

	msg, err := tool.Run(ctx, nil)
	if !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run err = %v, want ErrPullRequestRequested", err)
	}
	if msg.Content == nil || !strings.Contains(*msg.Content, "recorded") {
		t.Errorf("msg.Content = %v, want non-nil containing 'recorded'", msg.Content)
	}
	if msg.Role != "tool" {
		t.Errorf("msg.Role = %q, want 'tool'", msg.Role)
	}
	if tool.Title != "Optimize database index queries" {
		t.Errorf("tool.Title = %q, want 'Optimize database index queries'", tool.Title)
	}
	if tool.Body != "Adds compound index on (repo, goal, run_id)." {
		t.Errorf("tool.Body = %q, want 'Adds compound index on (repo, goal, run_id).'", tool.Body)
	}

	// 2. Trailing .git and slash variants match successfully
	repoVariants := []string{
		"https://github.com/example/repo.git",
		"https://github.com/example/repo/",
		"https://github.com/example/repo.git/",
	}
	for _, variant := range repoVariants {
		vTool := NewOpenPullRequestTool(expected)
		vTool.Repository = variant
		vTool.Branch = expected.Branch
		vTool.Title = "Valid title"
		vTool.Body = "Valid body"
		if _, err := vTool.Run(ctx, nil); !errors.Is(err, session.ErrPullRequestRequested) {
			t.Errorf("Run with repo variant %q err = %v, want ErrPullRequestRequested", variant, err)
		}
	}

	// 3. Wrong repository returns error naming expected value without sentinel
	wrongRepoTool := NewOpenPullRequestTool(expected)
	wrongRepoTool.Repository = "https://github.com/other/repo"
	wrongRepoTool.Branch = expected.Branch
	wrongRepoTool.Title = "Valid title"
	wrongRepoTool.Body = "Valid body"
	if _, err := wrongRepoTool.Run(ctx, nil); err == nil {
		t.Error("Run with wrong repo returned nil error, want error")
	} else {
		if errors.Is(err, session.ErrPullRequestRequested) {
			t.Error("Run with wrong repo returned ErrPullRequestRequested, want ordinary error")
		}
		if !strings.Contains(err.Error(), expected.Repository) {
			t.Errorf("Run with wrong repo error %q does not name expected repo %q", err.Error(), expected.Repository)
		}
	}

	// 4. Wrong branch returns error naming expected value without sentinel
	wrongBranchTool := NewOpenPullRequestTool(expected)
	wrongBranchTool.Repository = expected.Repository
	wrongBranchTool.Branch = "main"
	wrongBranchTool.Title = "Valid title"
	wrongBranchTool.Body = "Valid body"
	if _, err := wrongBranchTool.Run(ctx, nil); err == nil {
		t.Error("Run with wrong branch returned nil error, want error")
	} else {
		if errors.Is(err, session.ErrPullRequestRequested) {
			t.Error("Run with wrong branch returned ErrPullRequestRequested, want ordinary error")
		}
		if !strings.Contains(err.Error(), expected.Branch) {
			t.Errorf("Run with wrong branch error %q does not name expected branch %q", err.Error(), expected.Branch)
		}
	}

	// 5. Run with empty or whitespace title returns tool error without sentinel
	for _, badTitle := range []string{"", "   "} {
		badTool := NewOpenPullRequestTool(expected)
		badTool.Repository = expected.Repository
		badTool.Branch = expected.Branch
		badTool.Title = badTitle
		badTool.Body = "Some body"
		if _, err := badTool.Run(ctx, nil); err == nil {
			t.Errorf("Run with title %q returned nil error, want error", badTitle)
		} else if errors.Is(err, session.ErrPullRequestRequested) {
			t.Errorf("Run with title %q returned ErrPullRequestRequested, want ordinary error", badTitle)
		}
	}

	// 6. Unmarshal JSON into tool, keeps Title, Body, Repository, Branch and returns sentinel
	unmarshalTool := NewOpenPullRequestTool(expected)
	jsonInput := `{"repository":"https://github.com/example/repo","branch":"ao/goal/run-1","title":"Refactor cache layer","body":"Replaces map with sync.Map."}`
	if err := json.Unmarshal([]byte(jsonInput), unmarshalTool); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if unmarshalTool.Repository != "https://github.com/example/repo" {
		t.Errorf("Repository = %q, want 'https://github.com/example/repo'", unmarshalTool.Repository)
	}
	if unmarshalTool.Branch != "ao/goal/run-1" {
		t.Errorf("Branch = %q, want 'ao/goal/run-1'", unmarshalTool.Branch)
	}
	if unmarshalTool.Title != "Refactor cache layer" {
		t.Errorf("Title = %q, want 'Refactor cache layer'", unmarshalTool.Title)
	}
	if unmarshalTool.Body != "Replaces map with sync.Map." {
		t.Errorf("Body = %q, want 'Replaces map with sync.Map.'", unmarshalTool.Body)
	}

	jsonMsg, err := unmarshalTool.Run(ctx, nil)
	if !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run err = %v, want ErrPullRequestRequested", err)
	}
	if jsonMsg.Content == nil || !strings.Contains(*jsonMsg.Content, "recorded") {
		t.Errorf("jsonMsg.Content = %v, want non-nil containing 'recorded'", jsonMsg.Content)
	}
}
