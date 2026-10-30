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

package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/forge"
)

type fakeRemote struct {
	listRefsFunc      func(ctx context.Context, repoURL string) (map[string]string, error)
	fetchTreeFunc     func(ctx context.Context, repoURL, commit, prefix string) (fs.FS, error)
	defaultBranchFunc func(ctx context.Context, repoURL string) (string, error)
}

func (f *fakeRemote) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	if f.listRefsFunc != nil {
		return f.listRefsFunc(ctx, repoURL)
	}
	return nil, nil
}

func (f *fakeRemote) FetchTree(ctx context.Context, repoURL, commit, prefix string) (fs.FS, error) {
	if f.fetchTreeFunc != nil {
		return f.fetchTreeFunc(ctx, repoURL, commit, prefix)
	}
	return nil, nil
}

func (f *fakeRemote) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	if f.defaultBranchFunc != nil {
		return f.defaultBranchFunc(ctx, repoURL)
	}
	return "main", nil
}

func TestOpenPullRequest_Success(t *testing.T) {
	var receivedAuth, receivedVersion, receivedAccept string
	var receivedPayload map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/repos/testowner/testrepo/pulls" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		receivedAuth = r.Header.Get("Authorization")
		receivedVersion = r.Header.Get("X-GitHub-Api-Version")
		receivedAccept = r.Header.Get("Accept")

		if err := json.NewDecoder(r.Body).Decode(&receivedPayload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Remaining", "4900")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{
			"number": 42,
			"title": %q,
			"body": %q,
			"html_url": "https://github.com/testowner/testrepo/pull/42",
			"state": "open",
			"head": {"ref": %q, "sha": "headsha"},
			"base": {"ref": %q, "sha": "basesha"}
		}`, receivedPayload["title"], receivedPayload["body"], receivedPayload["head"], receivedPayload["base"])
	}))
	defer server.Close()

	client := New("secret-token",
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)

	ctx := t.Context()
	pr, err := client.OpenPullRequest(ctx, "https://github.com/testowner/testrepo", "ao/goal-1/run-1", "main", "Improve performance", "Automated summary")
	if err != nil {
		t.Fatalf("OpenPullRequest failed: %v", err)
	}

	if pr.Number != 42 {
		t.Errorf("pr.Number = %d; want 42", pr.Number)
	}
	if pr.Title != "Improve performance" {
		t.Errorf("pr.Title = %q; want %q", pr.Title, "Improve performance")
	}
	if pr.Head != "ao/goal-1/run-1" {
		t.Errorf("pr.Head = %q; want 'ao/goal-1/run-1'", pr.Head)
	}
	if pr.Base != "main" {
		t.Errorf("pr.Base = %q; want 'main'", pr.Base)
	}
	if pr.URL != "https://github.com/testowner/testrepo/pull/42" {
		t.Errorf("pr.URL = %q; want https://github.com/testowner/testrepo/pull/42", pr.URL)
	}

	if receivedAuth != "Bearer secret-token" {
		t.Errorf("receivedAuth = %q; want 'Bearer secret-token'", receivedAuth)
	}
	if receivedVersion != "2022-11-28" {
		t.Errorf("receivedVersion = %q; want '2022-11-28'", receivedVersion)
	}
	if receivedAccept != "application/vnd.github+json" {
		t.Errorf("receivedAccept = %q; want 'application/vnd.github+json'", receivedAccept)
	}
}

func TestOpenPullRequest_AlreadyExists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Remaining", "4800")
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprintln(w, `{
			"message": "Validation Failed",
			"errors": [
				{
					"resource": "PullRequest",
					"code": "custom",
					"message": "A pull request already exists for testowner:ao/goal-1."
				}
			]
		}`)
	}))
	defer server.Close()

	client := New("token",
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)

	ctx := t.Context()
	repoURL := "https://github.com/testowner/testrepo"
	_, err := client.OpenPullRequest(ctx, repoURL, "ao/goal-1", "main", "Title", "Body")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !errors.Is(err, forge.ErrPullRequestExists) {
		t.Errorf("expected errors.Is(err, ErrPullRequestExists), got: %v", err)
	}

	var typedErr *forge.PullRequestExistsError
	if !errors.As(err, &typedErr) {
		t.Errorf("expected *forge.PullRequestExistsError, got %T: %v", err, err)
	} else {
		if typedErr.Repo != repoURL || typedErr.Head != "ao/goal-1" || typedErr.Base != "main" {
			t.Errorf("unexpected typedErr details: %+v", typedErr)
		}
	}
}

func TestListPullRequests_PrefixFiltering(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/repos/testowner/testrepo/pulls" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Remaining", "4700")
		fmt.Fprintln(w, `[
			{
				"number": 1,
				"title": "PR 1",
				"body": "Body 1",
				"html_url": "https://github.com/testowner/testrepo/pull/1",
				"state": "open",
				"head": {"ref": "ao/goal-1/run-1"},
				"base": {"ref": "main"}
			},
			{
				"number": 2,
				"title": "PR 2",
				"body": "Body 2",
				"html_url": "https://github.com/testowner/testrepo/pull/2",
				"state": "open",
				"head": {"ref": "ao/goal-2/run-1"},
				"base": {"ref": "main"}
			},
			{
				"number": 3,
				"title": "Manual PR",
				"body": "Body 3",
				"html_url": "https://github.com/testowner/testrepo/pull/3",
				"state": "open",
				"head": {"ref": "feature/something-else"},
				"base": {"ref": "main"}
			}
		]`)
	}))
	defer server.Close()

	client := New("token",
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)

	ctx := t.Context()
	repoURL := "https://github.com/testowner/testrepo"

	// 1. Filter by prefix "ao/"
	aoPRs, err := client.ListPullRequests(ctx, repoURL, "ao/")
	if err != nil {
		t.Fatalf("ListPullRequests failed: %v", err)
	}
	if len(aoPRs) != 2 {
		t.Fatalf("expected 2 PRs matching 'ao/', got %d", len(aoPRs))
	}
	if aoPRs[0].Number != 1 || aoPRs[1].Number != 2 {
		t.Errorf("unexpected PR numbers: %v, %v", aoPRs[0].Number, aoPRs[1].Number)
	}

	// 2. Filter by specific prefix "ao/goal-1"
	goal1PRs, err := client.ListPullRequests(ctx, repoURL, "ao/goal-1")
	if err != nil {
		t.Fatalf("ListPullRequests failed: %v", err)
	}
	if len(goal1PRs) != 1 || goal1PRs[0].Number != 1 {
		t.Errorf("expected PR 1, got %v", goal1PRs)
	}

	// 3. No prefix returns all 3
	allPRs, err := client.ListPullRequests(ctx, repoURL, "")
	if err != nil {
		t.Fatalf("ListPullRequests without prefix failed: %v", err)
	}
	if len(allPRs) != 3 {
		t.Errorf("expected 3 PRs, got %d", len(allPRs))
	}
}

func TestConditionalRequests_304ReturnsCachedBody(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		w.Header().Set("X-RateLimit-Remaining", "4500")

		if count == 1 {
			w.Header().Set("ETag", `"etag-value-123"`)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `[
				{
					"number": 10,
					"title": "Cached PR",
					"head": {"ref": "ao/goal/run"},
					"base": {"ref": "main"}
				}
			]`)
			return
		}

		if r.Header.Get("If-None-Match") == `"etag-value-123"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		http.Error(w, "missing expected ETag", http.StatusBadRequest)
	}))
	defer server.Close()

	client := New("token",
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)

	ctx := t.Context()
	repoURL := "https://github.com/owner/repo"

	// First call: 200 OK with ETag
	prs1, err := client.ListPullRequests(ctx, repoURL, "")
	if err != nil {
		t.Fatalf("first ListPullRequests call failed: %v", err)
	}
	if len(prs1) != 1 || prs1[0].Number != 10 {
		t.Fatalf("unexpected prs1: %v", prs1)
	}

	// Second call: server returns 304 Not Modified; client should return cached body
	prs2, err := client.ListPullRequests(ctx, repoURL, "")
	if err != nil {
		t.Fatalf("second ListPullRequests call failed: %v", err)
	}
	if len(prs2) != 1 || prs2[0].Number != 10 {
		t.Fatalf("unexpected prs2 on 304 response: %v", prs2)
	}

	if atomic.LoadInt32(&requestCount) != 2 {
		t.Errorf("expected 2 server requests, got %d", requestCount)
	}
}

func TestRateLimitFloor_StopsReads(t *testing.T) {
	var serverRequests int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&serverRequests, 1)
		// Return remaining 50, which is below the default floor of 100
		w.Header().Set("X-RateLimit-Remaining", "50")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `[]`)
	}))
	defer server.Close()

	client := New("token",
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
		WithRateLimitFloor(100),
	)

	ctx := t.Context()
	repoURL := "https://github.com/owner/repo"

	// First read call: server returns 200 with X-RateLimit-Remaining: 50
	prs, err := client.ListPullRequests(ctx, repoURL, "")
	if err != nil {
		t.Fatalf("first read call failed: %v", err)
	}
	if len(prs) != 0 {
		t.Errorf("expected 0 PRs, got %d", len(prs))
	}
	if atomic.LoadInt32(&serverRequests) != 1 {
		t.Fatalf("expected 1 server request, got %d", serverRequests)
	}

	// Second read call: rate limit floor (100) must stop the read call before network request
	_, err = client.ListPullRequests(ctx, repoURL, "")
	if err == nil {
		t.Fatalf("second read call expected error due to rate limit floor, got nil")
	}

	if !errors.Is(err, forge.ErrRateLimitExceeded) {
		t.Errorf("expected errors.Is(err, ErrRateLimitExceeded), got: %v", err)
	}

	// Server should NOT have received a second request
	if atomic.LoadInt32(&serverRequests) != 1 {
		t.Errorf("server received %d requests; expected rate-limit floor to stop reads at 1", serverRequests)
	}

	// Third read call: still stopped, should not log multiple times
	_, err = client.ListPullRequests(ctx, repoURL, "")
	if !errors.Is(err, forge.ErrRateLimitExceeded) {
		t.Errorf("expected ErrRateLimitExceeded on third call, got: %v", err)
	}
	if atomic.LoadInt32(&serverRequests) != 1 {
		t.Errorf("server received %d requests; expected rate-limit floor to stop reads at 1", serverRequests)
	}
}

func TestPushCredentials(t *testing.T) {
	c1 := New("token123")
	u, p := c1.PushCredentials("https://github.com/owner/repo")
	if u != "x-access-token" || p != "token123" {
		t.Errorf("PushCredentials = (%q, %q); want ('x-access-token', 'token123')", u, p)
	}

	c2 := New("")
	u, p = c2.PushCredentials("https://github.com/owner/repo")
	if u != "" || p != "" {
		t.Errorf("PushCredentials on empty token = (%q, %q); want ('', '')", u, p)
	}
}

func TestForgeDelegation(t *testing.T) {
	var listRefsCalled, fetchTreeCalled, defaultBranchCalled bool
	remote := &fakeRemote{
		listRefsFunc: func(ctx context.Context, repoURL string) (map[string]string, error) {
			listRefsCalled = true
			return map[string]string{"refs/heads/main": "sha123"}, nil
		},
		fetchTreeFunc: func(ctx context.Context, repoURL, commit, prefix string) (fs.FS, error) {
			fetchTreeCalled = true
			if prefix != ".goals" {
				t.Errorf("expected prefix '.goals', got %q", prefix)
			}
			return forge.NewTreeFS(map[string][]byte{"file.txt": []byte("hi")}), nil
		},
		defaultBranchFunc: func(ctx context.Context, repoURL string) (string, error) {
			defaultBranchCalled = true
			return "trunk", nil
		},
	}

	client := New("token", WithRemote(remote))
	ctx := t.Context()

	refs, err := client.ListRefs(ctx, "https://github.com/foo/bar")
	if err != nil || !listRefsCalled || refs["refs/heads/main"] != "sha123" {
		t.Errorf("ListRefs delegation failed: err=%v, called=%v, refs=%v", err, listRefsCalled, refs)
	}

	tree, err := client.FetchTree(ctx, "https://github.com/foo/bar", "sha123", ".goals")
	if err != nil || !fetchTreeCalled {
		t.Errorf("FetchTree delegation failed: err=%v, called=%v", err, fetchTreeCalled)
	}
	data, err := fs.ReadFile(tree, "file.txt")
	if err != nil || string(data) != "hi" {
		t.Errorf("FetchTree tree read failed: %v, %q", err, string(data))
	}

	branch, err := client.DefaultBranch(ctx, "https://github.com/foo/bar")
	if err != nil || !defaultBranchCalled || branch != "trunk" {
		t.Errorf("DefaultBranch delegation failed: err=%v, called=%v, branch=%q", err, defaultBranchCalled, branch)
	}
}
