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
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"
)

func TestGitRemoteBareRepo(t *testing.T) {
	ctx := t.Context()
	t.Setenv("GIT_CONFIG_GLOBAL", "")
	t.Setenv("GIT_CONFIG_SYSTEM", "")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "")

	// Create a bare repository
	bareDir := filepath.Join(t.TempDir(), "repo.git")
	_, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("PlainInit bare repo: %v", err)
	}

	// Create a working repository to commit and push
	workDir := filepath.Join(t.TempDir(), "work")
	workRepo, err := git.PlainInit(workDir, false)
	if err != nil {
		t.Fatalf("PlainInit work repo: %v", err)
	}

	wt, err := workRepo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}

	goalsDir := filepath.Join(workDir, ".goals")
	if err := os.MkdirAll(goalsDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	goalContent := []byte("# Hello Goal\n\nTest goal description.\n")
	if err := os.WriteFile(filepath.Join(goalsDir, "hello.md"), goalContent, 0644); err != nil {
		t.Fatalf("WriteFile hello.md: %v", err)
	}

	if _, err := wt.Add(".goals/hello.md"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	commitHash, err := wt.Commit("add hello.md goal", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "Test Committer",
			Email: "test@example.com",
			When:  time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Push from work repository to bare repository
	_, err = workRepo.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{bareDir},
	})
	if err != nil {
		t.Fatalf("CreateRemote: %v", err)
	}

	if err := workRepo.Push(&git.PushOptions{RemoteName: "origin"}); err != nil {
		t.Fatalf("Push to bare repo: %v", err)
	}

	// Now exercise GitRemote against bareDir
	client := NewGitRemote()

	// 1. ListRefs
	refs, err := client.ListRefs(ctx, bareDir)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}

	commitHex := commitHash.String()
	foundBranch := false
	for refName, hash := range refs {
		if hash == commitHex {
			foundBranch = true
		}
		t.Logf("Found ref %s -> %s", refName, hash)
	}
	if !foundBranch {
		t.Errorf("commit %s not found in listed refs: %+v", commitHex, refs)
	}

	// 2. DefaultBranch
	branch, err := client.DefaultBranch(ctx, bareDir)
	if err != nil {
		t.Fatalf("DefaultBranch: %v", err)
	}
	if branch == "" {
		t.Error("expected non-empty default branch")
	}
	t.Logf("Default branch: %s", branch)

	// 3. FetchTree
	treeFS, err := client.FetchTree(ctx, bareDir, commitHex)
	if err != nil {
		t.Fatalf("FetchTree: %v", err)
	}

	readBytes, err := fs.ReadFile(treeFS, ".goals/hello.md")
	if err != nil {
		t.Fatalf("ReadFile from fetched tree: %v", err)
	}

	if string(readBytes) != string(goalContent) {
		t.Errorf("file content mismatch: got %q, want %q", string(readBytes), string(goalContent))
	}

	// Verify DefaultBranch fails when remote has no HEAD symref
	noHeadDir := filepath.Join(t.TempDir(), "nohead.git")
	if _, err := git.PlainInit(noHeadDir, true); err != nil {
		t.Fatalf("PlainInit: %v", err)
	}
	if err := os.Remove(filepath.Join(noHeadDir, "HEAD")); err != nil {
		t.Fatalf("Remove HEAD: %v", err)
	}
	if _, err := client.DefaultBranch(ctx, noHeadDir); err == nil {
		t.Error("expected error from DefaultBranch when HEAD symref is missing, got nil")
	}

	// 4. PushCredentials
	u, p := client.PushCredentials(bareDir)
	if u != "" || p != "" {
		t.Errorf("expected empty push credentials, got (%q, %q)", u, p)
	}

	credClient := NewGitRemoteWithCredentials("user", "pass")
	u, p = credClient.PushCredentials(bareDir)
	if u != "user" || p != "pass" {
		t.Errorf("expected ('user', 'pass'), got (%q, %q)", u, p)
	}
}

func TestSmartHTTP_ProtocolV2(t *testing.T) {
	ctx := t.Context()

	var getCalled, postCalled int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repo.git/info/refs":
			atomic.AddInt32(&getCalled, 1)
			if r.URL.Query().Get("service") != "git-upload-pack" {
				http.Error(w, "missing service query param", http.StatusBadRequest)
				return
			}
			if r.Header.Get("Git-Protocol") != "version=2" {
				http.Error(w, "missing Git-Protocol header", http.StatusBadRequest)
				return
			}
			if r.Header.Get("User-Agent") != "auto-optimizer" {
				http.Error(w, "unexpected User-Agent: "+r.Header.Get("User-Agent"), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			var buf bytes.Buffer
			_ = writePktLine(&buf, "# service=git-upload-pack\n")
			_ = writePktFlush(&buf)
			_ = writePktLine(&buf, "version 2\n")
			_ = writePktLine(&buf, "agent=git/test\n")
			_ = writePktLine(&buf, "ls-refs=unborn\n")
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())

		case r.Method == http.MethodPost && r.URL.Path == "/repo.git/git-upload-pack":
			atomic.AddInt32(&postCalled, 1)
			if r.Header.Get("Git-Protocol") != "version=2" {
				http.Error(w, "missing Git-Protocol header", http.StatusBadRequest)
				return
			}
			if r.Header.Get("Content-Type") != "application/x-git-upload-pack-request" {
				http.Error(w, "unexpected Content-Type", http.StatusBadRequest)
				return
			}
			if r.Header.Get("Accept") != "application/x-git-upload-pack-result" {
				http.Error(w, "unexpected Accept", http.StatusBadRequest)
				return
			}
			if r.Header.Get("User-Agent") != "auto-optimizer" {
				http.Error(w, "unexpected User-Agent", http.StatusBadRequest)
				return
			}

			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read error", http.StatusInternalServerError)
				return
			}
			lines, err := parsePktLines(bodyBytes)
			if err != nil {
				http.Error(w, "invalid pkt-lines: "+err.Error(), http.StatusBadRequest)
				return
			}

			// Validate expected commands/arguments in POST body
			var hasLsRefs, hasSymrefs, hasPeel, hasHeadPrefix, hasHeadsPrefix bool
			for _, l := range lines {
				if l.pType == pktData {
					switch string(l.payload) {
					case "command=ls-refs":
						hasLsRefs = true
					case "symrefs":
						hasSymrefs = true
					case "peel":
						hasPeel = true
					case "ref-prefix HEAD":
						hasHeadPrefix = true
					case "ref-prefix refs/heads/":
						hasHeadsPrefix = true
					}
				}
			}
			if !hasLsRefs || !hasSymrefs || !hasPeel || !hasHeadPrefix || !hasHeadsPrefix {
				http.Error(w, "missing expected ls-refs arguments", http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
			var buf bytes.Buffer
			_ = writePktLine(&buf, "1111111111111111111111111111111111111111 HEAD symref-target:refs/heads/main\n")
			_ = writePktLine(&buf, "1111111111111111111111111111111111111111 refs/heads/main\n")
			_ = writePktLine(&buf, "2222222222222222222222222222222222222222 refs/heads/feature\n")
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	repoURL := server.URL + "/repo.git"
	client := NewGitRemote(WithHTTPClient(server.Client()))

	// 1. ListRefs
	refs, err := client.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("ListRefs failed: %v", err)
	}

	if refs["HEAD"] != "1111111111111111111111111111111111111111" {
		t.Errorf("expected HEAD ref to be 1111..., got %q", refs["HEAD"])
	}
	if refs["refs/heads/main"] != "1111111111111111111111111111111111111111" {
		t.Errorf("expected refs/heads/main to be 1111..., got %q", refs["refs/heads/main"])
	}
	if refs["refs/heads/feature"] != "2222222222222222222222222222222222222222" {
		t.Errorf("expected refs/heads/feature to be 2222..., got %q", refs["refs/heads/feature"])
	}

	// 2. DefaultBranch - extracted from HEAD symref target in the same response
	branch, err := client.DefaultBranch(ctx, repoURL)
	if err != nil {
		t.Fatalf("DefaultBranch failed: %v", err)
	}
	if branch != "main" {
		t.Errorf("expected default branch 'main', got %q", branch)
	}

	if atomic.LoadInt32(&getCalled) == 0 || atomic.LoadInt32(&postCalled) == 0 {
		t.Errorf("expected GET and POST calls, got GET=%d POST=%d", getCalled, postCalled)
	}
}

func TestSmartHTTP_ProtocolV0(t *testing.T) {
	ctx := t.Context()

	var getCalled, postCalled int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repo.git/info/refs":
			atomic.AddInt32(&getCalled, 1)
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")

			var buf bytes.Buffer
			_ = writePktLine(&buf, "# service=git-upload-pack\n")
			_ = writePktFlush(&buf)
			// V0 advertisement: first ref carries capabilities after NUL
			_ = writePktLine(&buf, "3333333333333333333333333333333333333333 HEAD\x00multi_ack thin-pack symref=HEAD:refs/heads/master agent=git/test\n")
			_ = writePktLine(&buf, "3333333333333333333333333333333333333333 refs/heads/master\n")
			_ = writePktLine(&buf, "4444444444444444444444444444444444444444 refs/heads/dev\n")
			_ = writePktLine(&buf, "5555555555555555555555555555555555555555 refs/tags/v1.0^{}\n") // peeled tag, should be skipped
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())

		case r.Method == http.MethodPost:
			atomic.AddInt32(&postCalled, 1)
			http.Error(w, "protocol v0 should not trigger POST", http.StatusBadRequest)

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	repoURL := server.URL + "/repo.git"
	client := NewGitRemote(WithHTTPClient(server.Client()))

	refs, err := client.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("ListRefs failed for v0: %v", err)
	}

	if postCount := atomic.LoadInt32(&postCalled); postCount > 0 {
		t.Errorf("expected 0 POST calls for protocol v0, got %d", postCount)
	}

	if refs["HEAD"] != "3333333333333333333333333333333333333333" {
		t.Errorf("expected HEAD 3333..., got %q", refs["HEAD"])
	}
	if refs["refs/heads/master"] != "3333333333333333333333333333333333333333" {
		t.Errorf("expected master 3333..., got %q", refs["refs/heads/master"])
	}
	if refs["refs/heads/dev"] != "4444444444444444444444444444444444444444" {
		t.Errorf("expected dev 4444..., got %q", refs["refs/heads/dev"])
	}
	if _, ok := refs["refs/tags/v1.0^{}"]; ok {
		t.Errorf("peeled tag was not excluded from refs map")
	}

	branch, err := client.DefaultBranch(ctx, repoURL)
	if err != nil {
		t.Fatalf("DefaultBranch failed for v0: %v", err)
	}
	if branch != "master" {
		t.Errorf("expected default branch 'master', got %q", branch)
	}
}

func TestSmartHTTP_Conditional304(t *testing.T) {
	ctx := t.Context()

	var getCount int32
	etag := `"etag-abc-123"`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := atomic.AddInt32(&getCount, 1)

		if call == 1 {
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			var buf bytes.Buffer
			_ = writePktLine(&buf, "version 2\n")
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())
			return
		}

		if r.Method == http.MethodGet {
			if r.Header.Get("If-None-Match") != etag {
				http.Error(w, "missing or incorrect If-None-Match header", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNotModified)
			return
		}

		if r.Method == http.MethodPost {
			var buf bytes.Buffer
			_ = writePktLine(&buf, "9999999999999999999999999999999999999999 HEAD symref-target:refs/heads/main\n")
			_ = writePktLine(&buf, "9999999999999999999999999999999999999999 refs/heads/main\n")
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())
		}
	}))
	defer server.Close()

	repoURL := server.URL + "/repo.git"
	client := NewGitRemote(WithHTTPClient(server.Client()))

	// Initial request: 200 OK + cached
	refs1, err := client.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("Initial ListRefs failed: %v", err)
	}
	if refs1["HEAD"] != "9999999999999999999999999999999999999999" {
		t.Errorf("unexpected initial HEAD: %q", refs1["HEAD"])
	}

	// Second request: server returns 304 Not Modified
	refs2, err := client.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("Second ListRefs on 304 failed: %v", err)
	}
	if refs2["HEAD"] != "9999999999999999999999999999999999999999" {
		t.Errorf("unexpected cached HEAD after 304: %q", refs2["HEAD"])
	}

	branch, err := client.DefaultBranch(ctx, repoURL)
	if err != nil {
		t.Fatalf("DefaultBranch failed: %v", err)
	}
	if branch != "main" {
		t.Errorf("expected default branch 'main', got %q", branch)
	}
}

func TestSmartHTTP_401Unauthorized(t *testing.T) {
	ctx := t.Context()

	expectedUser := "testuser"
	expectedPass := "secrettoken"
	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(expectedUser+":"+expectedPass))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != expectedAuth {
			w.Header().Set("WWW-Authenticate", `Basic realm="Git Access"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		var buf bytes.Buffer
		_ = writePktLine(&buf, "version 2\n")
		_ = writePktFlush(&buf)
		_, _ = w.Write(buf.Bytes())
	}))
	defer server.Close()

	repoURL := server.URL + "/repo.git"

	// 1. Client without credentials should fail with 401
	unauthClient := NewGitRemote(WithHTTPClient(server.Client()))
	_, err := unauthClient.ListRefs(ctx, repoURL)
	if err == nil {
		t.Fatal("expected error from unauthenticated request, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected error to mention status 401, got: %v", err)
	}

	// 2. Client with credentials succeeds in passing Basic Auth
	authClient := NewGitRemoteWithCredentials(expectedUser, expectedPass, WithHTTPClient(server.Client()))
	// Also mock POST git-upload-pack handler
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != expectedAuth {
			w.Header().Set("WWW-Authenticate", `Basic realm="Git Access"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		if r.Method == http.MethodGet {
			var buf bytes.Buffer
			_ = writePktLine(&buf, "version 2\n")
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())
			return
		}

		if r.Method == http.MethodPost {
			var buf bytes.Buffer
			_ = writePktLine(&buf, "abcdef1234567890abcdef1234567890abcdef12 HEAD symref-target:refs/heads/main\n")
			_ = writePktLine(&buf, "abcdef1234567890abcdef1234567890abcdef12 refs/heads/main\n")
			_ = writePktFlush(&buf)
			_, _ = w.Write(buf.Bytes())
		}
	})

	refs, err := authClient.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("authenticated ListRefs failed: %v", err)
	}
	if refs["HEAD"] != "abcdef1234567890abcdef1234567890abcdef12" {
		t.Errorf("expected ref hash abcdef..., got %q", refs["HEAD"])
	}
}

func TestSmartHTTP_Timeout(t *testing.T) {
	ctx := t.Context()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(500 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	repoURL := server.URL + "/repo.git"
	client := NewGitRemote(
		WithTimeout(30*time.Millisecond),
		WithHTTPClient(server.Client()),
	)

	_, err := client.ListRefs(ctx, repoURL)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestSmartHTTP_LiveGitHub(t *testing.T) {
	if os.Getenv("FORGE_LIVE_TESTS") != "1" {
		t.Skip("skipping live github test: FORGE_LIVE_TESTS=1 not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	client := NewGitRemote()
	repoURL := "https://github.com/gke-labs/auto-optimizer.git"
	refs, err := client.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("ListRefs failed: %v", err)
	}
	if refs["HEAD"] == "" {
		t.Errorf("expected non-empty HEAD from live github")
	}
	if refs["refs/heads/main"] == "" {
		t.Errorf("expected refs/heads/main from live github")
	}
	branch, err := client.DefaultBranch(ctx, repoURL)
	if err != nil {
		t.Fatalf("DefaultBranch failed: %v", err)
	}
	if branch != "main" {
		t.Errorf("expected default branch 'main', got %q", branch)
	}
}
