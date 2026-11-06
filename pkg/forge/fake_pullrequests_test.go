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

package forge

import (
	"errors"
	"testing"
)

func openPR(t *testing.T, f *FakeForge, repoURL, head, base string) *PullRequest {
	t.Helper()
	pr, err := f.OpenPullRequest(t.Context(), repoURL, head, base, head, "")
	if err != nil {
		t.Fatalf("OpenPullRequest(%q, %q): %v", head, base, err)
	}
	return pr
}

func TestFakeForge_PullRequests(t *testing.T) {
	ctx := t.Context()
	f := NewFake()
	repoURL := "https://github.com/gke-labs/test-repo"

	pr1 := openPR(t, f, repoURL, "ao/goal-1/run-1", "main")
	if pr1.Number != 1 || pr1.State != "open" || pr1.URL != repoURL+"/pull/1" {
		t.Errorf("unexpected PR 1: %+v", pr1)
	}

	openPR(t, f, repoURL, "ao/goal-2/run-1", "main")
	openPR(t, f, repoURL, "feature/foo", "main")

	// PullRequests records all opened PRs in order
	recorded := f.PullRequests(repoURL)
	if len(recorded) != 3 || recorded[0].Number != 1 || recorded[1].Number != 2 || recorded[2].Number != 3 {
		t.Errorf("unexpected recorded PR numbers: %v", recorded)
	}

	// List by prefix "ao/": newest first [PR 2, PR 1]
	aoPRs, err := f.ListPullRequests(ctx, repoURL, "ao/")
	if err != nil {
		t.Fatalf("ListPullRequests('ao/'): %v", err)
	}
	if len(aoPRs) != 2 || aoPRs[0].Number != 2 || aoPRs[1].Number != 1 {
		t.Errorf("expected newest first [PR 2, PR 1], got %v", aoPRs)
	}

	// List by specific prefix "ao/goal-1"
	goal1PRs, err := f.ListPullRequests(ctx, repoURL, "ao/goal-1")
	if err != nil {
		t.Fatalf("ListPullRequests('ao/goal-1'): %v", err)
	}
	if len(goal1PRs) != 1 || goal1PRs[0].Number != 1 {
		t.Errorf("expected [PR 1], got %v", goal1PRs)
	}

	// Empty prefix matches all, newest first
	allPRs, err := f.ListPullRequests(ctx, repoURL, "")
	if err != nil {
		t.Fatalf("ListPullRequests(''): %v", err)
	}
	if len(allPRs) != 3 || allPRs[0].Number != 3 || allPRs[1].Number != 2 || allPRs[2].Number != 1 {
		t.Errorf("expected newest first [PR 3, PR 2, PR 1], got %v", allPRs)
	}

	// Non-matching prefix returns empty slice
	nonePRs, err := f.ListPullRequests(ctx, repoURL, "nonexistent/")
	if err != nil {
		t.Fatalf("ListPullRequests('nonexistent/'): %v", err)
	}
	if len(nonePRs) != 0 {
		t.Errorf("expected 0 PRs for nonexistent prefix, got %d", len(nonePRs))
	}
}

func TestFakeForge_DuplicateReturnsPullRequestExistsError(t *testing.T) {
	ctx := t.Context()
	f := NewFake()
	repoURL := "https://github.com/gke-labs/test-repo"

	openPR(t, f, repoURL, "ao/my-branch", "main")

	// Duplicate head and base
	_, err := f.OpenPullRequest(ctx, repoURL, "ao/my-branch", "main", "duplicate", "")
	var existsErr *PullRequestExistsError
	if !errors.As(err, &existsErr) || !errors.Is(err, ErrPullRequestExists) {
		t.Fatalf("expected *PullRequestExistsError and ErrPullRequestExists, got %v", err)
	}
	if existsErr.Repo != repoURL || existsErr.Head != "ao/my-branch" || existsErr.Base != "main" {
		t.Errorf("unexpected existsErr fields: %+v", existsErr)
	}

	// Different base succeeds
	prDiffBase := openPR(t, f, repoURL, "ao/my-branch", "develop")
	if prDiffBase.Number != 2 {
		t.Errorf("expected PR number 2, got %d", prDiffBase.Number)
	}
}

func TestFakeForge_InjectedErrorReturnedOnce(t *testing.T) {
	ctx := t.Context()
	f := NewFake()
	repoURL := "https://github.com/gke-labs/test-repo"

	injected := errors.New("simulated network failure")
	f.SetOpenPullRequestError(injected)

	_, err := f.OpenPullRequest(ctx, repoURL, "branch-1", "main", "PR 1", "")
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	// Next call should succeed
	pr := openPR(t, f, repoURL, "branch-1", "main")
	if pr.Number != 1 {
		t.Errorf("expected PR number 1, got %d", pr.Number)
	}
}

func TestFakeForge_SetPullRequestsAndListError(t *testing.T) {
	ctx := t.Context()
	f := NewFake()
	repoURL := "https://github.com/gke-labs/test-repo"

	prs := []PullRequest{
		{Number: 1, Head: "ao/goal-1/run-1", Base: "main", State: "open"},
		{Number: 2, Head: "ao/goal-1/run-2", Base: "main", State: "closed"},
		{Number: 3, Head: "ao/goal-2/run-1", Base: "main", State: "merged"},
	}
	f.SetPullRequests(repoURL, prs)

	list, err := f.ListPullRequests(ctx, repoURL, "ao/goal-1/")
	if err != nil {
		t.Fatalf("ListPullRequests: %v", err)
	}
	if len(list) != 2 || list[0].Number != 2 || list[1].Number != 1 {
		t.Errorf("expected [PR 2 (closed), PR 1 (open)], got %+v", list)
	}

	injected := errors.New("simulated list failure")
	f.SetListPullRequestsError(injected)

	_, err = f.ListPullRequests(ctx, repoURL, "")
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	// Next call should succeed and return all PRs newest-first
	list2, err := f.ListPullRequests(ctx, repoURL, "")
	if err != nil {
		t.Fatalf("ListPullRequests after error: %v", err)
	}
	if len(list2) != 3 {
		t.Errorf("expected 3 PRs, got %d", len(list2))
	}
}
