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
	"io/fs"
	"os"
	"path/filepath"
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
