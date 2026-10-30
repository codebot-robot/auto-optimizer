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
	"sort"
	"sync"
)

// FakeForge is an in-memory implementation of Forge for testing.
type FakeForge struct {
	mu              sync.RWMutex
	refs            map[string]map[string]string            // repoURL -> ref -> commit
	trees           map[string]map[string]map[string][]byte // repoURL -> commit -> path -> content
	defaultBranches map[string]string                       // repoURL -> default branch
	requestedBlobs  []string
}

// NewFake creates a new FakeForge.
func NewFake() *FakeForge {
	return &FakeForge{
		refs:            make(map[string]map[string]string),
		trees:           make(map[string]map[string]map[string][]byte),
		defaultBranches: make(map[string]string),
	}
}

// SetRefs sets the advertised refs for a repository.
func (f *FakeForge) SetRefs(repoURL string, refs map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	cpy := make(map[string]string, len(refs))
	for k, v := range refs {
		cpy[k] = v
	}
	f.refs[repoURL] = cpy
}

// SetTree sets the files returned for a specific commit SHA in a repository.
func (f *FakeForge) SetTree(repoURL string, commit string, files map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.trees[repoURL] == nil {
		f.trees[repoURL] = make(map[string]map[string][]byte)
	}
	cpy := make(map[string][]byte, len(files))
	for k, v := range files {
		fileBytes := make([]byte, len(v))
		copy(fileBytes, v)
		cpy[k] = fileBytes
	}
	f.trees[repoURL][commit] = cpy
}

// SetDefaultBranch configures the default branch for a repository.
func (f *FakeForge) SetDefaultBranch(repoURL string, branch string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.defaultBranches[repoURL] = branch
}

// ListRefs returns the configured refs for the repository.
func (f *FakeForge) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	refs, ok := f.refs[repoURL]
	if !ok {
		return make(map[string]string), nil
	}
	cpy := make(map[string]string, len(refs))
	for k, v := range refs {
		cpy[k] = v
	}
	return cpy, nil
}

// FetchTree returns an fs.FS containing the files configured for the commit.
// If prefix is non-empty, only files under that path prefix are read;
// blobs outside the prefix are skipped without reading their contents.
// If prefix is empty, all files in the tree are fetched.
func (f *FakeForge) FetchTree(ctx context.Context, repoURL string, commit string, prefix string) (fs.FS, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	repoTrees := f.trees[repoURL]
	if repoTrees == nil || repoTrees[commit] == nil {
		return nil, fmt.Errorf("commit %s not found in repo %s", commit, repoURL)
	}

	commitFiles := repoTrees[commit]
	paths := make([]string, 0, len(commitFiles))
	for p := range commitFiles {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	filtered := make(map[string][]byte)
	for _, p := range paths {
		if !PathMatchesPrefix(p, prefix) {
			continue
		}
		f.requestedBlobs = append(f.requestedBlobs, p)
		data := commitFiles[p]
		fileBytes := make([]byte, len(data))
		copy(fileBytes, data)
		filtered[p] = fileBytes
	}

	return NewTreeFS(filtered), nil
}

// RequestedBlobs returns a slice of paths for all blobs requested via FetchTree.
func (f *FakeForge) RequestedBlobs() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	cpy := make([]string, len(f.requestedBlobs))
	copy(cpy, f.requestedBlobs)
	return cpy
}

// ResetRequestedBlobs clears the recorded list of requested blobs.
func (f *FakeForge) ResetRequestedBlobs() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requestedBlobs = nil
}

// DefaultBranch returns the configured default branch or defaults to "main".
func (f *FakeForge) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if branch, ok := f.defaultBranches[repoURL]; ok && branch != "" {
		return branch, nil
	}
	return "main", nil
}
