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
	"testing"
	"testing/fstest"
)

func TestFakeForge(t *testing.T) {
	ctx := t.Context()
	f := NewFake()

	repoURL := "https://github.com/gke-labs/test-repo"

	// Initial empty
	refs, err := f.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("expected 0 refs, got %d", len(refs))
	}

	defBranch, err := f.DefaultBranch(ctx, repoURL)
	if err != nil {
		t.Fatalf("DefaultBranch: %v", err)
	}
	if defBranch != "main" {
		t.Errorf("expected default branch 'main', got %q", defBranch)
	}

	// Set custom default branch
	f.SetDefaultBranch(repoURL, "master")
	defBranch, err = f.DefaultBranch(ctx, repoURL)
	if err != nil {
		t.Fatalf("DefaultBranch after set: %v", err)
	}
	if defBranch != "master" {
		t.Errorf("expected default branch 'master', got %q", defBranch)
	}

	// Set refs
	f.SetRefs(repoURL, map[string]string{
		"refs/heads/master": "commit-1",
	})

	refs, err = f.ListRefs(ctx, repoURL)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	if refs["refs/heads/master"] != "commit-1" || len(refs) != 1 {
		t.Errorf("unexpected refs: %+v", refs)
	}

	// Fetch nonexistent tree
	_, err = f.FetchTree(ctx, repoURL, "nonexistent")
	if err == nil {
		t.Fatal("expected error fetching nonexistent commit, got nil")
	}

	// Set tree and fetch
	f.SetTree(repoURL, "commit-1", map[string][]byte{
		".goals/hello.md": []byte("# Hello\n"),
		"README.md":       []byte("Welcome\n"),
	})

	treeFS, err := f.FetchTree(ctx, repoURL, "commit-1")
	if err != nil {
		t.Fatalf("FetchTree: %v", err)
	}

	data, err := fs.ReadFile(treeFS, ".goals/hello.md")
	if err != nil {
		t.Fatalf("ReadFile .goals/hello.md: %v", err)
	}
	if string(data) != "# Hello\n" {
		t.Errorf("unexpected content: %q", string(data))
	}

	// Run fstest.TestFS to verify TreeFS implements standard fs.FS
	if err := fstest.TestFS(treeFS, ".goals/hello.md", "README.md"); err != nil {
		t.Fatalf("TestFS: %v", err)
	}
}
