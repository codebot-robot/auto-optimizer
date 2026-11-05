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

package session

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseStatus(t *testing.T) {
	tests := []struct {
		name    string
		stdout  string
		want    []StatusEntry
		wantErr bool
	}{
		{
			name:   "empty clean tree",
			stdout: "",
			want:   nil,
		},
		{
			name:   "modified unstaged",
			stdout: "1 .M N... 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 modified.txt\x00",
			want: []StatusEntry{
				{Path: "modified.txt", Kind: StatusOrdinary, Staged: false, Unstaged: true},
			},
		},
		{
			name:   "added staged",
			stdout: "1 A. N... 000000 100644 100644 0000000000000000000000000000000000000000 3333333333333333333333333333333333333333 added.txt\x00",
			want: []StatusEntry{
				{Path: "added.txt", Kind: StatusOrdinary, Staged: true, Unstaged: false},
			},
		},
		{
			name:   "renamed",
			stdout: "2 R. N... 100644 100644 100644 4444444444444444444444444444444444444444 4444444444444444444444444444444444444444 R100 new.txt\x00old.txt\x00",
			want: []StatusEntry{
				{Path: "new.txt", Kind: StatusRenamed, Staged: true, Unstaged: false},
			},
		},
		{
			name:   "untracked",
			stdout: "? untracked.txt\x00",
			want: []StatusEntry{
				{Path: "untracked.txt", Kind: StatusUntracked, Staged: false, Unstaged: true},
			},
		},
		{
			name:   "unmerged",
			stdout: "u UU N... 100644 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 3333333333333333333333333333333333333333 conflict.txt\x00",
			want: []StatusEntry{
				{Path: "conflict.txt", Kind: StatusUnmerged, Staged: true, Unstaged: true},
			},
		},
		{
			name:   "path with space",
			stdout: "1 .M N... 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 path with space.txt\x00",
			want: []StatusEntry{
				{Path: "path with space.txt", Kind: StatusOrdinary, Staged: false, Unstaged: true},
			},
		},
		{
			name: "combined sample",
			stdout: "1 .M N... 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 modified.txt\x00" +
				"1 A. N... 000000 100644 100644 0000000000000000000000000000000000000000 3333333333333333333333333333333333333333 added.txt\x00" +
				"2 R. N... 100644 100644 100644 4444444444444444444444444444444444444444 4444444444444444444444444444444444444444 R100 new.txt\x00old.txt\x00" +
				"? untracked.txt\x00" +
				"u UU N... 100644 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 3333333333333333333333333333333333333333 conflict.txt\x00" +
				"1 .M N... 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 path with space.txt\x00",
			want: []StatusEntry{
				{Path: "modified.txt", Kind: StatusOrdinary, Staged: false, Unstaged: true},
				{Path: "added.txt", Kind: StatusOrdinary, Staged: true, Unstaged: false},
				{Path: "new.txt", Kind: StatusRenamed, Staged: true, Unstaged: false},
				{Path: "untracked.txt", Kind: StatusUntracked, Staged: false, Unstaged: true},
				{Path: "conflict.txt", Kind: StatusUnmerged, Staged: true, Unstaged: true},
				{Path: "path with space.txt", Kind: StatusOrdinary, Staged: false, Unstaged: true},
			},
		},
		{
			name:    "malformed ordinary line",
			stdout:  "1 short\x00",
			wantErr: true,
		},
		{
			name:    "malformed rename missing orig path",
			stdout:  "2 R. N... 100644 100644 100644 4444444444444444444444444444444444444444 4444444444444444444444444444444444444444 R100 new.txt\x00",
			wantErr: true,
		},
		{
			name:    "unrecognized entry format",
			stdout:  "x unknown format\x00",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseStatus(tc.stdout)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseStatus() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ParseStatus() diff (-want +got):\n%s", diff)
			}
			if tc.name == "empty clean tree" && !Clean(got) {
				t.Errorf("Clean(got) = false, want true for empty tree")
			}
		})
	}
}

func TestGit_RevParse(t *testing.T) {
	const tip = "abcdef1234567890abcdef1234567890abcdef12"
	srv := &scriptedProcessServer{
		commands: map[string]cmdResult{
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}": {stdout: tip + "\n"},
			"git -C /workspace/repo rev-parse --verify bad^{commit}":  {exitCode: 128, stderr: "fatal: Needed a single revision"},
		},
	}
	sb, cleanup := setupScriptedSandbox(t, srv)
	defer cleanup()

	git := sb.Git("/workspace/repo")

	// Success
	got, err := git.RevParse(t.Context(), "HEAD")
	if err != nil {
		t.Fatalf("RevParse(HEAD) err = %v", err)
	}
	if got != tip {
		t.Errorf("RevParse(HEAD) = %q, want %q", got, tip)
	}

	// Error on non-zero exit code
	_, err = git.RevParse(t.Context(), "bad")
	if err == nil {
		t.Fatal("RevParse(bad) returned nil error, want error")
	}
	if !strings.Contains(err.Error(), "fatal: Needed a single revision") {
		t.Errorf("RevParse(bad) err = %q, want stderr included", err)
	}
}

func TestGit_ChangedPaths(t *testing.T) {
	srv := &scriptedProcessServer{
		commands: map[string]cmdResult{
			"git -C /workspace/repo diff --name-only -z base HEAD":   {stdout: "file1.txt\x00path with space.txt\x00"},
			"git -C /workspace/repo diff --name-only -z same same":   {},
			"git -C /workspace/repo diff --name-only -z base badrev": {exitCode: 128, stderr: "fatal: bad revision"},
		},
	}
	sb, cleanup := setupScriptedSandbox(t, srv)
	defer cleanup()

	git := sb.Git("/workspace/repo")

	// Multiple changed paths
	paths, err := git.ChangedPaths(t.Context(), "base", "HEAD")
	if err != nil {
		t.Fatalf("ChangedPaths() err = %v", err)
	}
	want := []string{"file1.txt", "path with space.txt"}
	if diff := cmp.Diff(want, paths); diff != "" {
		t.Errorf("ChangedPaths() diff (-want +got):\n%s", diff)
	}

	// Empty diff
	emptyPaths, err := git.ChangedPaths(t.Context(), "same", "same")
	if err != nil {
		t.Fatalf("ChangedPaths() err = %v", err)
	}
	if len(emptyPaths) != 0 {
		t.Errorf("ChangedPaths() = %v, want empty", emptyPaths)
	}

	// Error
	_, err = git.ChangedPaths(t.Context(), "base", "badrev")
	if err == nil {
		t.Fatal("ChangedPaths(badrev) returned nil error, want error")
	}
	if !strings.Contains(err.Error(), "fatal: bad revision") {
		t.Errorf("ChangedPaths(badrev) err = %q, want stderr included", err)
	}
}

func TestGit_StatusAndClean(t *testing.T) {
	srv := &scriptedProcessServer{
		commands: map[string]cmdResult{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": {
				stdout: "1 .M N... 100644 100644 100644 1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 dirty.txt\x00",
			},
			"git -C /clean/repo status --porcelain=v2 -z --untracked-files=all": {},
		},
	}
	sb, cleanup := setupScriptedSandbox(t, srv)
	defer cleanup()

	// Dirty repository
	dirtyGit := sb.Git("/workspace/repo")
	clean, err := dirtyGit.Clean(t.Context())
	if err != nil {
		t.Fatalf("Clean() err = %v", err)
	}
	if clean {
		t.Errorf("Clean() = true, want false for dirty repo")
	}

	entries, err := dirtyGit.Status(t.Context())
	if err != nil {
		t.Fatalf("Status() err = %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "dirty.txt" {
		t.Errorf("Status() = %v, want 1 entry with Path 'dirty.txt'", entries)
	}

	// Clean repository
	cleanGit := sb.Git("/clean/repo")
	clean, err = cleanGit.Clean(t.Context())
	if err != nil {
		t.Fatalf("Clean() err = %v", err)
	}
	if !clean {
		t.Errorf("Clean() = false, want true for clean repo")
	}
}

func TestGit_NilSandbox(t *testing.T) {
	git := NewGit(nil, "/workspace/repo")
	if _, err := git.Run(t.Context(), "status"); !errors.Is(err, errors.New("sandbox is required")) && (err == nil || !strings.Contains(err.Error(), "sandbox is required")) {
		t.Errorf("Run with nil sandbox err = %v, want 'sandbox is required'", err)
	}
}
