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
	"slices"
	"strings"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/google/go-cmp/cmp"
)

type fakeSandbox struct {
	calls []tools.ExecCommandOptions
	res   *tools.ExecCommandResult
	err   error
}

func (f *fakeSandbox) ExecCommand(_ context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	f.calls = append(f.calls, opts)
	if f.err != nil {
		return nil, f.err
	}
	if f.res != nil {
		return f.res, nil
	}
	return &tools.ExecCommandResult{}, nil
}

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
	if !strings.Contains(schema.Function.Description, "nothing uncommitted is included") {
		t.Errorf("schema function description = %q, want it to mention nothing uncommitted is included", schema.Function.Description)
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	req, ok := params["required"].([]string)
	wantReq := []string{"title", "body"}
	if !ok || !slices.Equal(req, wantReq) {
		t.Errorf("required parameters = %v, want %v", req, wantReq)
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	for _, prop := range []string{"repository", "branch", "title", "body"} {
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

	// 1. Run with matching repository, branch, title, body succeeds with sentinel on clean tree
	sandbox := &fakeSandbox{}
	tool := NewOpenPullRequestTool(expected)
	tool.Repository = "https://github.com/example/repo"
	tool.Branch = "ao/goal/run-1"
	tool.Title = "Optimize database index queries"
	tool.Body = "Adds compound index on (repo, goal, run_id)."

	msg, err := tool.Run(ctx, sandbox)
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
	if len(sandbox.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(sandbox.calls))
	}
	wantCmd := strings.Fields("git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all")
	if diff := cmp.Diff(wantCmd, sandbox.calls[0].Command); diff != "" {
		t.Errorf("command diff (-want +got):\n%s", diff)
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
		if _, err := vTool.Run(ctx, &fakeSandbox{}); !errors.Is(err, session.ErrPullRequestRequested) {
			t.Errorf("Run with repo variant %q err = %v, want ErrPullRequestRequested", variant, err)
		}
	}

	// 3. Wrong repository returns error naming expected value without sentinel (even with nil sandbox)
	wrongRepoTool := NewOpenPullRequestTool(expected)
	wrongRepoTool.Repository = "https://github.com/other/repo"
	wrongRepoTool.Branch = expected.Branch
	wrongRepoTool.Title = "Valid title"
	wrongRepoTool.Body = "Valid body"
	wantErrRepo := `unexpected repository "https://github.com/other/repo", expected "https://github.com/example/repo"`
	if _, err := wrongRepoTool.Run(ctx, nil); err == nil {
		t.Error("Run with wrong repo returned nil error, want error")
	} else {
		if errors.Is(err, session.ErrPullRequestRequested) {
			t.Error("Run with wrong repo returned ErrPullRequestRequested, want ordinary error")
		}
		if err.Error() != wantErrRepo {
			t.Errorf("Run with wrong repo error = %q, want %q", err.Error(), wantErrRepo)
		}
	}

	// 4. Wrong branch returns error naming expected value without sentinel (even with nil sandbox)
	wrongBranchTool := NewOpenPullRequestTool(expected)
	wrongBranchTool.Repository = expected.Repository
	wrongBranchTool.Branch = "main"
	wrongBranchTool.Title = "Valid title"
	wrongBranchTool.Body = "Valid body"
	wantErrBranch := `unexpected branch "main", expected "ao/goal/run-1"`
	if _, err := wrongBranchTool.Run(ctx, nil); err == nil {
		t.Error("Run with wrong branch returned nil error, want error")
	} else {
		if errors.Is(err, session.ErrPullRequestRequested) {
			t.Error("Run with wrong branch returned ErrPullRequestRequested, want ordinary error")
		}
		if err.Error() != wantErrBranch {
			t.Errorf("Run with wrong branch error = %q, want %q", err.Error(), wantErrBranch)
		}
	}

	// 5. Run with empty or whitespace title returns tool error without sentinel (even with nil sandbox)
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

	// 6. Cheap argument checks fail before git status is called on a dirty sandbox
	dirtySandbox := &fakeSandbox{
		res: &tools.ExecCommandResult{Stdout: "? file.go\x00"},
	}
	cheapCheckTool := NewOpenPullRequestTool(expected)
	cheapCheckTool.Repository = "https://github.com/wrong/repo"
	cheapCheckTool.Branch = expected.Branch
	cheapCheckTool.Title = "Valid title"
	cheapCheckTool.Body = "Valid body"
	if _, err := cheapCheckTool.Run(ctx, dirtySandbox); err == nil || !strings.Contains(err.Error(), expected.Repository) {
		t.Errorf("Run err = %v, want error naming expected repository before git status", err)
	}
	if len(dirtySandbox.calls) != 0 {
		t.Errorf("ExecCommand called %d times, want 0 when argument check fails first", len(dirtySandbox.calls))
	}

	// 7. Unmarshal JSON into tool, keeps Title, Body, Repository, Branch and returns sentinel
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

	jsonMsg, err := unmarshalTool.Run(ctx, &fakeSandbox{})
	if !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run err = %v, want ErrPullRequestRequested", err)
	}
	if jsonMsg.Content == nil || !strings.Contains(*jsonMsg.Content, "recorded") {
		t.Errorf("jsonMsg.Content = %v, want non-nil containing 'recorded'", jsonMsg.Content)
	}

	// 8. Omitted repository and branch succeed and default to expected values
	omittedTool := NewOpenPullRequestTool(expected)
	omittedTool.Title = "Optimize database index queries"
	omittedTool.Body = "Adds compound index on (repo, goal, run_id)."
	omittedMsg, err := omittedTool.Run(ctx, &fakeSandbox{})
	if !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run with omitted values err = %v, want ErrPullRequestRequested", err)
	}
	if omittedMsg.Content == nil || !strings.Contains(*omittedMsg.Content, "recorded") {
		t.Errorf("omittedMsg.Content = %v, want non-nil containing 'recorded'", omittedMsg.Content)
	}
	if omittedTool.Repository != expected.Repository {
		t.Errorf("omittedTool.Repository = %q, want %q", omittedTool.Repository, expected.Repository)
	}
	if omittedTool.Branch != expected.Branch {
		t.Errorf("omittedTool.Branch = %q, want %q", omittedTool.Branch, expected.Branch)
	}
	if omittedTool.Title != "Optimize database index queries" {
		t.Errorf("omittedTool.Title = %q, want 'Optimize database index queries'", omittedTool.Title)
	}
	if omittedTool.Body != "Adds compound index on (repo, goal, run_id)." {
		t.Errorf("omittedTool.Body = %q, want 'Adds compound index on (repo, goal, run_id).'", omittedTool.Body)
	}

	// 9. Omitted repository with valid branch succeeds and defaults repository
	omitRepoTool := NewOpenPullRequestTool(expected)
	omitRepoTool.Branch = expected.Branch
	omitRepoTool.Title = "Valid title"
	omitRepoTool.Body = "Valid body"
	if _, err := omitRepoTool.Run(ctx, &fakeSandbox{}); !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run with omitted repo err = %v, want ErrPullRequestRequested", err)
	}
	if omitRepoTool.Repository != expected.Repository {
		t.Errorf("omitRepoTool.Repository = %q, want %q", omitRepoTool.Repository, expected.Repository)
	}

	// 10. Omitted branch with valid repository succeeds and defaults branch
	omitBranchTool := NewOpenPullRequestTool(expected)
	omitBranchTool.Repository = expected.Repository
	omitBranchTool.Title = "Valid title"
	omitBranchTool.Body = "Valid body"
	if _, err := omitBranchTool.Run(ctx, &fakeSandbox{}); !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run with omitted branch err = %v, want ErrPullRequestRequested", err)
	}
	if omitBranchTool.Branch != expected.Branch {
		t.Errorf("omitBranchTool.Branch = %q, want %q", omitBranchTool.Branch, expected.Branch)
	}

	// 11. Wrong repository with omitted branch is refused with same error text
	wrongRepoOmitBranch := NewOpenPullRequestTool(expected)
	wrongRepoOmitBranch.Repository = "https://github.com/other/repo"
	wrongRepoOmitBranch.Title = "Valid title"
	wrongRepoOmitBranch.Body = "Valid body"
	if _, err := wrongRepoOmitBranch.Run(ctx, nil); err == nil || err.Error() != wantErrRepo {
		t.Errorf("Run with wrong repo (omitted branch) err = %v, want %q", err, wantErrRepo)
	}

	// 12. Wrong branch with omitted repository is refused with same error text
	wrongBranchOmitRepo := NewOpenPullRequestTool(expected)
	wrongBranchOmitRepo.Branch = "main"
	wrongBranchOmitRepo.Title = "Valid title"
	wrongBranchOmitRepo.Body = "Valid body"
	if _, err := wrongBranchOmitRepo.Run(ctx, nil); err == nil || err.Error() != wantErrBranch {
		t.Errorf("Run with wrong branch (omitted repo) err = %v, want %q", err, wantErrBranch)
	}

	// 13. Unmarshal JSON without repository and branch defaults them on Run and preserves title and body
	unmarshalOmitted := NewOpenPullRequestTool(expected)
	jsonOmitted := `{"title":"Refactor cache layer","body":"Replaces map with sync.Map."}`
	if err := json.Unmarshal([]byte(jsonOmitted), unmarshalOmitted); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if unmarshalOmitted.Repository != "" {
		t.Errorf("Repository before Run = %q, want empty", unmarshalOmitted.Repository)
	}
	if unmarshalOmitted.Branch != "" {
		t.Errorf("Branch before Run = %q, want empty", unmarshalOmitted.Branch)
	}
	if unmarshalOmitted.Title != "Refactor cache layer" {
		t.Errorf("Title = %q, want 'Refactor cache layer'", unmarshalOmitted.Title)
	}
	if unmarshalOmitted.Body != "Replaces map with sync.Map." {
		t.Errorf("Body = %q, want 'Replaces map with sync.Map.'", unmarshalOmitted.Body)
	}
	if _, err := unmarshalOmitted.Run(ctx, &fakeSandbox{}); !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("unmarshalOmitted.Run err = %v, want ErrPullRequestRequested", err)
	}
	if unmarshalOmitted.Repository != expected.Repository {
		t.Errorf("Repository after Run = %q, want %q", unmarshalOmitted.Repository, expected.Repository)
	}
	if unmarshalOmitted.Branch != expected.Branch {
		t.Errorf("Branch after Run = %q, want %q", unmarshalOmitted.Branch, expected.Branch)
	}
	if unmarshalOmitted.Title != "Refactor cache layer" {
		t.Errorf("Title after Run = %q, want 'Refactor cache layer'", unmarshalOmitted.Title)
	}
	if unmarshalOmitted.Body != "Replaces map with sync.Map." {
		t.Errorf("Body after Run = %q, want 'Replaces map with sync.Map.'", unmarshalOmitted.Body)
	}
}

func TestOpenPullRequestTool_Run_DirtyTree(t *testing.T) {
	ctx := t.Context()
	expected := OpenPullRequestExpectation{
		Repository: "https://github.com/example/repo",
		Branch:     "ao/goal/run-1",
	}
	dirtySandbox := &fakeSandbox{
		res: &tools.ExecCommandResult{
			Stdout: "1 .M N... 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 pkg/tool.go\x00? new_file.txt\x00",
		},
	}
	tool := NewOpenPullRequestTool(expected)
	tool.Repository = expected.Repository
	tool.Branch = expected.Branch
	tool.Title = "Some PR"
	tool.Body = "Some body"

	_, err := tool.Run(ctx, dirtySandbox)
	if err == nil {
		t.Fatal("Run returned nil error for dirty tree, want error")
	}
	if errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatal("Run returned ErrPullRequestRequested for dirty tree, want ordinary error")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "the working tree has uncommitted changes; commit them with git, then call open_pull_request again") {
		t.Errorf("err = %q, want message instructing to commit uncommitted changes", errMsg)
	}
	if !strings.Contains(errMsg, "pkg/tool.go") || !strings.Contains(errMsg, "new_file.txt") {
		t.Errorf("err = %q, want it to list dirty paths", errMsg)
	}
}

func TestOpenPullRequestTool_Run_DirtyTree_MaxFivePaths(t *testing.T) {
	ctx := t.Context()
	expected := OpenPullRequestExpectation{
		Repository: "https://github.com/example/repo",
		Branch:     "ao/goal/run-1",
	}
	dirtySandbox := &fakeSandbox{
		res: &tools.ExecCommandResult{
			Stdout: "? f1.go\x00? f2.go\x00? f3.go\x00? f4.go\x00? f5.go\x00? f6.go\x00? f7.go\x00",
		},
	}
	tool := NewOpenPullRequestTool(expected)
	tool.Repository = expected.Repository
	tool.Branch = expected.Branch
	tool.Title = "Some PR"
	tool.Body = "Some body"

	_, err := tool.Run(ctx, dirtySandbox)
	if err == nil {
		t.Fatal("Run returned nil error for dirty tree, want error")
	}
	if errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatal("Run returned ErrPullRequestRequested for dirty tree, want ordinary error")
	}
	errMsg := err.Error()
	for _, wantPath := range []string{"f1.go", "f2.go", "f3.go", "f4.go", "f5.go"} {
		if !strings.Contains(errMsg, wantPath) {
			t.Errorf("err = %q, want it to contain path %q", errMsg, wantPath)
		}
	}
	for _, dontWantPath := range []string{"f6.go", "f7.go"} {
		if strings.Contains(errMsg, dontWantPath) {
			t.Errorf("err = %q, should not contain path %q (max 5 paths)", errMsg, dontWantPath)
		}
	}
}

func TestOpenPullRequestTool_Run_GitStatusFailure(t *testing.T) {
	ctx := t.Context()
	expected := OpenPullRequestExpectation{
		Repository: "https://github.com/example/repo",
		Branch:     "ao/goal/run-1",
	}
	statusErr := errors.New("sandbox exec failed")
	failSandbox := &fakeSandbox{err: statusErr}

	tool := NewOpenPullRequestTool(expected)
	tool.Repository = expected.Repository
	tool.Branch = expected.Branch
	tool.Title = "Some PR"
	tool.Body = "Some body"

	_, err := tool.Run(ctx, failSandbox)
	if err == nil {
		t.Fatal("Run returned nil error, want error")
	}
	if !errors.Is(err, statusErr) {
		t.Fatalf("err = %v, want wrapped statusErr %v", err, statusErr)
	}
	if errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatal("Run returned ErrPullRequestRequested on git status failure, want ordinary error")
	}

	exitCodeSandbox := &fakeSandbox{
		res: &tools.ExecCommandResult{
			ExitCode: 128,
			Stderr:   "fatal: not a git repository",
		},
	}
	_, err = tool.Run(ctx, exitCodeSandbox)
	if err == nil {
		t.Fatal("Run returned nil error for non-zero exit code, want error")
	}
	if errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatal("Run returned ErrPullRequestRequested on non-zero exit code, want ordinary error")
	}
	if !strings.Contains(err.Error(), "exit code 128") || !strings.Contains(err.Error(), "fatal: not a git repository") {
		t.Errorf("err = %q, want it to mention exit code and stderr", err.Error())
	}
}

func TestOpenPullRequestTool_Run_NilSandbox(t *testing.T) {
	expected := OpenPullRequestExpectation{
		Repository: "https://github.com/example/repo",
		Branch:     "ao/goal/run-1",
	}
	tool := NewOpenPullRequestTool(expected)
	tool.Repository = expected.Repository
	tool.Branch = expected.Branch
	tool.Title = "PR"
	tool.Body = "Body"
	if _, err := tool.Run(t.Context(), nil); err == nil {
		t.Fatal("Run with nil sandbox returned nil error, want error")
	}
}
