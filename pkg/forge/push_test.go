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
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/revlist"
)

func commitAndPack(t *testing.T, repo *git.Repository, msg string) (plumbing.Hash, io.Reader) {
	t.Helper()
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	path := msg + ".txt"
	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), path), []byte(msg), 0644)
	_, _ = wt.Add(path)
	tip, err := wt.Commit(msg, &git.CommitOptions{
		Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	hashes, err := revlist.Objects(repo.Storer, []plumbing.Hash{tip}, nil)
	if err != nil {
		t.Fatalf("revlist.Objects: %v", err)
	}
	buf := new(bytes.Buffer)
	if _, err := packfile.NewEncoder(buf, repo.Storer, false).Encode(hashes, 10); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return tip, buf
}

func newRemoteWithBase(t *testing.T) (string, *git.Repository, *git.Repository, plumbing.Hash) {
	t.Helper()
	bareDir := filepath.Join(t.TempDir(), "remote.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("PlainInit bare: %v", err)
	}
	remoteURL := "file://" + bareDir
	work, err := git.PlainInit(filepath.Join(t.TempDir(), "work"), false)
	if err != nil {
		t.Fatalf("PlainInit work: %v", err)
	}
	base, _ := commitAndPack(t, work, "base")
	_, _ = work.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
	if err := work.Push(&git.PushOptions{RemoteName: "origin"}); err != nil {
		t.Fatalf("Push base: %v", err)
	}
	return remoteURL, bare, work, base
}

type hookReader struct {
	io.Reader
	fn func()
}

func (h *hookReader) Read(p []byte) (int, error) {
	if h.fn != nil {
		h.fn()
		h.fn = nil
	}
	return h.Reader.Read(p)
}

func TestPushPackfile(t *testing.T) {
	remoteURL, bare, work, base := newRemoteWithBase(t)
	branch := "ao/test-goal/run-1"
	refName := plumbing.ReferenceName("refs/heads/" + branch)

	// Step A: initial packfile push lands at branch tip.
	tip1, pack1 := commitAndPack(t, work, "first")
	if err := PushPackfile(t.Context(), PushPackfileOptions{
		RemoteURL: remoteURL, Branch: branch, Tip: tip1, Base: base, Packfile: pack1,
	}); err != nil {
		t.Fatalf("initial push: %v", err)
	}
	if ref, err := bare.Reference(refName, true); err != nil || ref.Hash() != tip1 {
		t.Fatalf("expected remote tip %s, got %v (err: %v)", tip1, ref, err)
	}

	// Step B: second call with further commit updates the branch.
	tip2, pack2 := commitAndPack(t, work, "second")
	if err := PushPackfile(t.Context(), PushPackfileOptions{
		RemoteURL: remoteURL, Branch: branch, Tip: tip2, Base: base, Packfile: pack2,
	}); err != nil {
		t.Fatalf("update push: %v", err)
	}
	if ref, err := bare.Reference(refName, true); err != nil || ref.Hash() != tip2 {
		t.Fatalf("expected updated remote tip %s, got %v (err: %v)", tip2, ref, err)
	}

	// Step C: stale lease returns ErrBranchMoved and leaves remote branch unchanged.
	tip3, pack3 := commitAndPack(t, work, "third")
	concurrentTip, _ := commitAndPack(t, work, "concurrent")
	r := &hookReader{
		Reader: pack3,
		fn: func() {
			_ = bare.Storer.SetReference(plumbing.NewHashReference(refName, concurrentTip))
		},
	}
	err := PushPackfile(t.Context(), PushPackfileOptions{
		RemoteURL: remoteURL, Branch: branch, Tip: tip3, Base: base, Packfile: r,
	})
	if !errors.Is(err, ErrBranchMoved) {
		t.Fatalf("expected ErrBranchMoved, got: %v", err)
	}
	if ref, err := bare.Reference(refName, true); err != nil || ref.Hash() != concurrentTip {
		t.Fatalf("expected remote to remain %s, got %v (err: %v)", concurrentTip, ref, err)
	}
}

func TestPushPackfile_Validation(t *testing.T) {
	remoteURL, _, work, base := newRemoteWithBase(t)
	_, pack := commitAndPack(t, work, "valid")
	unrelatedWork, _ := git.PlainInit(filepath.Join(t.TempDir(), "unrelated"), false)
	unrelatedTip, unrelatedPack := commitAndPack(t, unrelatedWork, "unrelated")

	for _, tc := range []struct {
		name, wantErr string
		tip, base     plumbing.Hash
		pack          io.Reader
	}{
		{name: "missing tip", tip: plumbing.NewHash("1111111111111111111111111111111111111111"), base: base, pack: pack, wantErr: "missing after import"},
		{name: "tip does not reach base", tip: unrelatedTip, base: base, pack: unrelatedPack, wantErr: "does not reach base commit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := PushPackfile(t.Context(), PushPackfileOptions{
				RemoteURL: remoteURL, Branch: "ao/val/r1", Tip: tc.tip, Base: tc.base, Packfile: tc.pack,
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

type fakeCredentialer struct {
	user, pass string
	calledWith string
}

func (c *fakeCredentialer) PushCredentials(repoURL string) (string, string) {
	c.calledWith = repoURL
	return c.user, c.pass
}

func TestPushPackfile_Credentials(t *testing.T) {
	remoteURL, _, work, base := newRemoteWithBase(t)
	tip, pack := commitAndPack(t, work, "auth")
	creds := &fakeCredentialer{user: "token-user", pass: "secret-token"}
	err := PushPackfile(t.Context(), PushPackfileOptions{
		RemoteURL:   remoteURL,
		Branch:      "ao/auth/r1",
		Tip:         tip,
		Base:        base,
		Packfile:    pack,
		Credentials: creds,
	})
	if err != nil {
		t.Fatalf("PushPackfile with credentials: %v", err)
	}
	if creds.calledWith != remoteURL {
		t.Fatalf("expected credentials called with %s, got %s", remoteURL, creds.calledWith)
	}
}

func TestBranchMovedError(t *testing.T) {
	inner := errors.New("lease expired")
	err := &BranchMovedError{Branch: "ao/g/r1", Remote: "github.com", Err: inner}
	if !errors.Is(err, ErrBranchMoved) || !errors.Is(err, inner) || err.Error() == "" {
		t.Fatalf("BranchMovedError unwrapping failed: %v", err)
	}
}
