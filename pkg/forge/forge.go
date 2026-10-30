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

// Package forge defines the interface and implementations for interacting
// with remote git repositories and forge hosts.
//
// Ref Advertisement and Caching:
// GitRemote implements ListRefs and DefaultBranch for http:// and https:// remotes
// using net/http over smart HTTP (Git protocol v2 with fallback to v0).
//
// Observed caching behavior of ref advertisement endpoints:
// Neither GitHub nor a standard git http-backend returns an ETag header for
// /info/refs?service=git-upload-pack or /git-upload-pack endpoints. Both servers
// emit "Cache-Control: no-cache, max-age=0, must-revalidate" (along with
// "Expires: Fri, 01 Jan 1980 00:00:00 GMT" and "Pragma: no-cache"). Consequently,
// conditional requests (If-None-Match) cannot bypass ref listing on GitHub or
// plain git http-backend, so the syncer's 60-second poll makes an un-cached ref
// advertisement request every polling interval. However, if a server or proxy does
// provide an ETag, GitRemote caches it and sends If-None-Match, treating 304 Not
// Modified as "no change" without parsing.
//
// Non-HTTP schemes (such as file:// or local filesystem paths) continue to use
// go-git's Remote.ListContext so that local repositories in tests and offline
// environments keep working.
package forge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"
)

// Forge abstracts operations on git repositories such as listing refs and fetching file trees.
type Forge interface {
	// ListRefs queries the remote repository and returns a map of reference names to commit SHAs.
	ListRefs(ctx context.Context, repoURL string) (map[string]string, error)

	// FetchTree fetches the repository tree at a specific commit as an fs.FS.
	// If prefix is non-empty, only files under that path prefix are read;
	// blobs outside the prefix are skipped without reading their contents.
	// If prefix is empty, all files in the tree are fetched.
	FetchTree(ctx context.Context, repoURL string, commit string, prefix string) (fs.FS, error)

	// DefaultBranch returns the default branch name of the remote repository (e.g. "main").
	DefaultBranch(ctx context.Context, repoURL string) (string, error)
}

// PathMatchesPrefix reports whether filePath falls under prefix.
// An empty prefix, ".", or "/" matches all paths.
func PathMatchesPrefix(filePath, prefix string) bool {
	cleanPrefix := path.Clean(strings.TrimPrefix(prefix, "/"))
	if cleanPrefix == "." || cleanPrefix == "" {
		return true
	}
	cleanPath := path.Clean(strings.TrimPrefix(filePath, "/"))
	return cleanPath == cleanPrefix || strings.HasPrefix(cleanPath, cleanPrefix+"/")
}

// ChangeRequestCreator creates and lists change requests / pull requests.
type ChangeRequestCreator interface {
	// OpenPullRequest opens a pull request on the forge repository.
	OpenPullRequest(ctx context.Context, repoURL, head, base, title, body string) (*PullRequest, error)

	// ListPullRequests lists pull requests for the repository, optionally filtering by head branch prefix.
	ListPullRequests(ctx context.Context, repoURL, headPrefix string) ([]PullRequest, error)
}

// PushCredentialer provides credentials needed for control-plane git push operations.
type PushCredentialer interface {
	// PushCredentials returns username and password for pushing to repoURL.
	PushCredentials(repoURL string) (username, password string)
}

// PullRequest represents a pull request or change request on a forge.
type PullRequest struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	Head   string `json:"head"`
	Base   string `json:"base"`
	URL    string `json:"url"`
	State  string `json:"state"`
}

// ErrPullRequestExists is a sentinel error indicating that a pull request already exists.
var ErrPullRequestExists = errors.New("pull request already exists")

// PullRequestExistsError is returned when a pull request already exists for a head/base branch.
type PullRequestExistsError struct {
	Repo    string
	Head    string
	Base    string
	Message string
}

func (e *PullRequestExistsError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("pull request already exists for %s (%s -> %s): %s", e.Repo, e.Head, e.Base, e.Message)
	}
	return fmt.Sprintf("pull request already exists for %s (%s -> %s)", e.Repo, e.Head, e.Base)
}

func (e *PullRequestExistsError) Is(target error) bool {
	return target == ErrPullRequestExists
}

// ErrRateLimitExceeded is a sentinel error indicating that the forge rate limit floor was reached.
var ErrRateLimitExceeded = errors.New("rate limit floor reached")

// RateLimitFloorError is returned when remaining API rate limit is below the configured floor.
type RateLimitFloorError struct {
	Remaining int
	Floor     int
}

func (e *RateLimitFloorError) Error() string {
	return fmt.Sprintf("rate limit remaining (%d) is below floor (%d)", e.Remaining, e.Floor)
}

func (e *RateLimitFloorError) Is(target error) bool {
	return target == ErrRateLimitExceeded
}

// TreeFS is an in-memory implementation of fs.FS, fs.ReadDirFS, fs.ReadFileFS, and fs.StatFS.
type TreeFS struct {
	files map[string][]byte
	dirs  map[string]map[string]bool
}

// NewTreeFS creates a new TreeFS from a map of relative file paths to their byte contents.
func NewTreeFS(files map[string][]byte) *TreeFS {
	t := &TreeFS{
		files: make(map[string][]byte, len(files)),
		dirs:  make(map[string]map[string]bool),
	}

	for p, data := range files {
		clean := path.Clean(strings.TrimPrefix(p, "/"))
		if clean == "." || clean == "" {
			continue
		}
		t.files[clean] = data

		// Populate parent directories
		dir := path.Dir(clean)
		base := path.Base(clean)
		for {
			if t.dirs[dir] == nil {
				t.dirs[dir] = make(map[string]bool)
			}
			t.dirs[dir][base] = true
			if dir == "." {
				break
			}
			base = path.Base(dir)
			dir = path.Dir(dir)
		}
	}

	return t
}

// Open opens the named file or directory.
func (t *TreeFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	clean := name

	if data, ok := t.files[clean]; ok {
		return &memFile{
			name:   path.Base(clean),
			reader: bytes.NewReader(data),
			size:   int64(len(data)),
		}, nil
	}

	if clean == "." || t.dirs[clean] != nil {
		entries, err := t.ReadDir(clean)
		if err != nil {
			return nil, err
		}
		return &memDir{
			name:    path.Base(clean),
			entries: entries,
		}, nil
	}

	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

// ReadDir returns the contents of the directory.
func (t *TreeFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	clean := name
	children := t.dirs[clean]
	if children == nil && clean != "." {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}

	var names []string
	for child := range children {
		names = append(names, child)
	}
	sort.Strings(names)

	var entries []fs.DirEntry
	for _, n := range names {
		childPath := path.Join(clean, n)
		if _, isFile := t.files[childPath]; isFile {
			entries = append(entries, &memDirEntry{
				name:  n,
				isDir: false,
				size:  int64(len(t.files[childPath])),
			})
		} else {
			entries = append(entries, &memDirEntry{
				name:  n,
				isDir: true,
			})
		}
	}

	return entries, nil
}

// ReadFile returns the contents of the named file.
func (t *TreeFS) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrInvalid}
	}
	clean := name
	data, ok := t.files[clean]
	if !ok {
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrNotExist}
	}
	cpy := make([]byte, len(data))
	copy(cpy, data)
	return cpy, nil
}

// Stat returns the FileInfo for the named file or directory.
func (t *TreeFS) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	clean := name
	if data, ok := t.files[clean]; ok {
		return &memFileInfo{
			name: path.Base(clean),
			size: int64(len(data)),
			mode: 0644,
		}, nil
	}
	if clean == "." || t.dirs[clean] != nil {
		return &memFileInfo{
			name: path.Base(clean),
			mode: fs.ModeDir | 0755,
		}, nil
	}
	return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
}

type memFile struct {
	name   string
	reader *bytes.Reader
	size   int64
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	return &memFileInfo{name: f.name, size: f.size, mode: 0644}, nil
}

func (f *memFile) Read(b []byte) (int, error) {
	return f.reader.Read(b)
}

func (f *memFile) Close() error {
	return nil
}

type memDir struct {
	name    string
	entries []fs.DirEntry
	offset  int
}

func (d *memDir) Stat() (fs.FileInfo, error) {
	return &memFileInfo{name: d.name, mode: fs.ModeDir | 0755}, nil
}

func (d *memDir) Read([]byte) (int, error) {
	return 0, fmt.Errorf("cannot read a directory")
}

func (d *memDir) Close() error {
	return nil
}

func (d *memDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if n <= 0 {
		left := d.entries[d.offset:]
		d.offset = len(d.entries)
		return left, nil
	}

	if d.offset >= len(d.entries) {
		return nil, io.EOF
	}

	end := d.offset + n
	if end > len(d.entries) {
		end = len(d.entries)
	}
	slice := d.entries[d.offset:end]
	d.offset = end
	return slice, nil
}

type memDirEntry struct {
	name  string
	isDir bool
	size  int64
}

func (e *memDirEntry) Name() string      { return e.name }
func (e *memDirEntry) IsDir() bool       { return e.isDir }
func (e *memDirEntry) Type() fs.FileMode { return e.mode().Type() }
func (e *memDirEntry) Info() (fs.FileInfo, error) {
	return &memFileInfo{name: e.name, isDir: e.isDir, size: e.size, mode: e.mode()}, nil
}

func (e *memDirEntry) mode() fs.FileMode {
	if e.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}

type memFileInfo struct {
	name  string
	isDir bool
	size  int64
	mode  fs.FileMode
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *memFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *memFileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *memFileInfo) Sys() any           { return nil }
