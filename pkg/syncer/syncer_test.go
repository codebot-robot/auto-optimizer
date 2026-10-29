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

package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestSyncerGoalLifecycle(t *testing.T) {
	ctx := t.Context()

	store := state.NewFileStore(t.TempDir())
	fakeForge := forge.NewFake()

	repoURL := "https://github.com/gke-labs/example-repo"
	key := state.RepoKey{
		Host:  "github.com",
		Owner: "gke-labs",
		Name:  "example-repo",
	}
	repo := &state.Repo{
		URL:            repoURL,
		Host:           key.Host,
		Owner:          key.Owner,
		Name:           key.Name,
		DefaultBranch:  "main",
		ForgeKind:      "fake",
		RegisteredTime: time.Now(),
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	s, err := New(Options{
		Store:        store,
		Forge:        fakeForge,
		PollInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New syncer: %v", err)
	}

	// 1. Initial push adding .goals/hello.md
	commit1 := "commit-sha-1"
	helloV1 := []byte("# Hello World\n\nFirst version of the goal.\n")
	h1 := sha256.Sum256(helloV1)
	expectedHash1 := hex.EncodeToString(h1[:])

	fakeForge.SetRefs(repoURL, map[string]string{
		"HEAD":            commit1,
		"refs/heads/main": commit1,
	})
	fakeForge.SetTree(repoURL, commit1, map[string][]byte{
		".goals/hello.md": helloV1,
		"README.md":       []byte("# Readme\n"),
	})

	if err := s.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce (step 1): %v", err)
	}

	// Verify goal record was created
	rec1, err := store.GetGoal(ctx, key, "hello")
	if err != nil {
		t.Fatalf("GetGoal hello (step 1): %v", err)
	}
	if rec1 == nil || rec1.Goal == nil {
		t.Fatal("expected goal record for 'hello', got nil")
	}
	if rec1.BaseCommit != commit1 {
		t.Errorf("expected base commit %s, got %s", commit1, rec1.BaseCommit)
	}
	if rec1.Hash != expectedHash1 {
		t.Errorf("expected hash %s, got %s", expectedHash1, rec1.Hash)
	}

	storedRepo1, err := store.GetRepo(ctx, key)
	if err != nil {
		t.Fatalf("GetRepo: %v", err)
	}
	if storedRepo1.HeadCommit != commit1 {
		t.Errorf("expected repo head commit %s, got %s", commit1, storedRepo1.HeadCommit)
	}

	// 2. Editing the file changes the hash
	commit2 := "commit-sha-2"
	helloV2 := []byte("# Hello World (v2)\n\nUpdated goal instructions.\n")
	h2 := sha256.Sum256(helloV2)
	expectedHash2 := hex.EncodeToString(h2[:])

	fakeForge.SetRefs(repoURL, map[string]string{
		"HEAD":            commit2,
		"refs/heads/main": commit2,
	})
	fakeForge.SetTree(repoURL, commit2, map[string][]byte{
		".goals/hello.md": helloV2,
		"README.md":       []byte("# Readme\n"),
	})

	if err := s.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce (step 2): %v", err)
	}

	rec2, err := store.GetGoal(ctx, key, "hello")
	if err != nil {
		t.Fatalf("GetGoal hello (step 2): %v", err)
	}
	if rec2 == nil || rec2.Goal == nil {
		t.Fatal("expected goal record for 'hello' after update, got nil")
	}
	if rec2.BaseCommit != commit2 {
		t.Errorf("expected updated base commit %s, got %s", commit2, rec2.BaseCommit)
	}
	if rec2.Hash != expectedHash2 {
		t.Errorf("expected updated hash %s, got %s", expectedHash2, rec2.Hash)
	}

	// 3. Deleting it removes the record
	commit3 := "commit-sha-3"
	fakeForge.SetRefs(repoURL, map[string]string{
		"HEAD":            commit3,
		"refs/heads/main": commit3,
	})
	fakeForge.SetTree(repoURL, commit3, map[string][]byte{
		"README.md": []byte("# Readme\n"),
	})

	if err := s.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce (step 3): %v", err)
	}

	rec3, err := store.GetGoal(ctx, key, "hello")
	if err == nil {
		t.Errorf("expected goal 'hello' to be deleted, found: %+v", rec3)
	} else if !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("GetGoal hello (step 3): %v", err)
	}

	goalsList, err := store.ListGoals(ctx, key)
	if err != nil {
		t.Fatalf("ListGoals: %v", err)
	}
	if len(goalsList) != 0 {
		t.Errorf("expected 0 goals after deletion, got %d", len(goalsList))
	}
}
