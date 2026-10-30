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
	"context"
	"fmt"
	"io/fs"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
)

// GitRemote implements Forge using go-git for ref advertisement and depth-1 in-memory fetches.
type GitRemote struct{}

// NewGitRemote creates a new GitRemote forge instance.
func NewGitRemote() *GitRemote {
	return &GitRemote{}
}

// ListRefs queries the remote repository's ref advertisement.
func (g *GitRemote) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	rem := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	})

	refs, err := rem.ListContext(ctx, &git.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("listing remote refs for %s: %w", repoURL, err)
	}

	result := make(map[string]string, len(refs))
	var headTarget plumbing.ReferenceName
	for _, ref := range refs {
		if ref.Name() == plumbing.HEAD && ref.Type() == plumbing.SymbolicReference {
			headTarget = ref.Target()
		}
	}

	for _, ref := range refs {
		if ref.Type() == plumbing.HashReference {
			result[ref.Name().String()] = ref.Hash().String()
			if headTarget != "" && ref.Name() == headTarget {
				result["HEAD"] = ref.Hash().String()
			}
		}
	}

	return result, nil
}

// DefaultBranch queries the remote repository to determine its default branch from the HEAD symref.
func (g *GitRemote) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	rem := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	})

	refs, err := rem.ListContext(ctx, &git.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("querying default branch for %s: %w", repoURL, err)
	}

	for _, ref := range refs {
		if ref.Name() == plumbing.HEAD && ref.Type() == plumbing.SymbolicReference {
			target := ref.Target().Short()
			if target != "" {
				return target, nil
			}
		}
	}

	return "", fmt.Errorf("remote %s has no HEAD symref", repoURL)
}

// FetchTree performs a depth-1 in-memory fetch of the commit and returns its tree as an fs.FS.
func (g *GitRemote) FetchTree(ctx context.Context, repoURL string, commit string) (fs.FS, error) {
	storer := memory.NewStorage()
	r, err := git.CloneContext(ctx, storer, nil, &git.CloneOptions{
		URL:   repoURL,
		Depth: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("cloning %s: %w", repoURL, err)
	}

	commitHash := plumbing.NewHash(commit)
	cObj, err := r.CommitObject(commitHash)
	if err != nil {
		return nil, fmt.Errorf("resolving commit %s in %s: %w", commit, repoURL, err)
	}

	tree, err := cObj.Tree()
	if err != nil {
		return nil, fmt.Errorf("getting tree for commit %s: %w", commit, err)
	}

	files := make(map[string][]byte)
	err = tree.Files().ForEach(func(f *object.File) error {
		contents, err := f.Contents()
		if err != nil {
			return err
		}
		files[f.Name] = []byte(contents)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading tree files: %w", err)
	}

	return NewTreeFS(files), nil
}
