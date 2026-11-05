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
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

// StatusKind identifies the category of entry in git status porcelain v2.
type StatusKind string

const (
	StatusOrdinary  StatusKind = "ordinary"
	StatusRenamed   StatusKind = "renamed"
	StatusUnmerged  StatusKind = "unmerged"
	StatusUntracked StatusKind = "untracked"
)

// StatusEntry represents a file in the working tree or index reported by git status.
type StatusEntry struct {
	Path     string
	Kind     StatusKind
	Staged   bool
	Unstaged bool
}

// Git runs git commands in a sandbox bound to a repository directory.
type Git struct {
	sandbox tools.Sandbox
	repoDir string
}

// NewGit creates a Git helper bound to sandbox and repoDir.
func NewGit(sandbox tools.Sandbox, repoDir string) *Git {
	return &Git{sandbox: sandbox, repoDir: repoDir}
}

// Git returns a Git helper bound to this sandbox and repoDir.
func (s *Sandbox) Git(repoDir string) *Git {
	return NewGit(s, repoDir)
}

// RunWithOptions executes a git command in the repository with the given options.
// It fails with an error carrying stderr on non-zero exit codes.
func (g *Git) RunWithOptions(ctx context.Context, opts tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	if g == nil || g.sandbox == nil {
		return nil, errors.New("sandbox is required")
	}
	opts.Command = append([]string{"git", "-C", g.repoDir}, opts.Command...)
	res, err := g.sandbox.ExecCommand(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(opts.Command[3:], " "), err)
	}
	if res.ExitCode != 0 {
		if stderr := strings.TrimSpace(res.Stderr); stderr != "" {
			return nil, fmt.Errorf("git %s failed with exit code %d: %s", strings.Join(opts.Command[3:], " "), res.ExitCode, stderr)
		}
		return nil, fmt.Errorf("git %s failed with exit code %d", strings.Join(opts.Command[3:], " "), res.ExitCode)
	}
	return res, nil
}

// Run executes a git command with args in the repository.
func (g *Git) Run(ctx context.Context, args ...string) (*tools.ExecCommandResult, error) {
	return g.RunWithOptions(ctx, tools.ExecCommandOptions{Command: args})
}

// Status runs git status in machine-readable porcelain v2 format and returns parsed entries.
func (g *Git) Status(ctx context.Context) ([]StatusEntry, error) {
	res, err := g.Run(ctx, "status", "--porcelain=v2", "-z", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	return ParseStatus(res.Stdout)
}

// Clean returns true if there are no status entries.
func Clean(entries []StatusEntry) bool {
	return len(entries) == 0
}

// Clean returns true if the repository working tree is clean.
func (g *Git) Clean(ctx context.Context) (bool, error) {
	entries, err := g.Status(ctx)
	return len(entries) == 0, err
}

// ChangedPaths returns file paths changed between from and to revisions.
func (g *Git) ChangedPaths(ctx context.Context, from, to string) ([]string, error) {
	res, err := g.Run(ctx, "diff", "--name-only", "-z", from, to)
	if err != nil {
		return nil, err
	}
	if res.Stdout == "" {
		return nil, nil
	}
	var paths []string
	for _, p := range strings.Split(res.Stdout, "\x00") {
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths, nil
}

// RevParse verifies a commit revision and returns the full commit SHA.
func (g *Git) RevParse(ctx context.Context, rev string) (string, error) {
	res, err := g.Run(ctx, "rev-parse", "--verify", rev+"^{commit}")
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(res.Stdout)
	if sha == "" {
		return "", fmt.Errorf("git rev-parse %s returned empty sha", rev)
	}
	return sha, nil
}

// ParseStatus parses git status --porcelain=v2 -z output into StatusEntry slices.
func ParseStatus(stdout string) ([]StatusEntry, error) {
	if stdout == "" {
		return nil, nil
	}
	parts := strings.Split(stdout, "\x00")
	var entries []StatusEntry
	for i := 0; i < len(parts); i++ {
		part := parts[i]
		if part == "" {
			continue
		}
		switch {
		case strings.HasPrefix(part, "1 "):
			tokens := strings.SplitN(part, " ", 9)
			if len(tokens) < 9 {
				return nil, fmt.Errorf("malformed ordinary status entry: %q", part)
			}
			xy := tokens[1]
			entries = append(entries, StatusEntry{
				Path:     tokens[8],
				Kind:     StatusOrdinary,
				Staged:   len(xy) > 0 && xy[0] != '.',
				Unstaged: len(xy) > 1 && xy[1] != '.',
			})
		case strings.HasPrefix(part, "2 "):
			tokens := strings.SplitN(part, " ", 10)
			if len(tokens) < 10 || i+1 >= len(parts) || parts[i+1] == "" {
				return nil, fmt.Errorf("malformed renamed status entry: %q", part)
			}
			i++
			xy := tokens[1]
			entries = append(entries, StatusEntry{
				Path:     tokens[9],
				Kind:     StatusRenamed,
				Staged:   len(xy) > 0 && xy[0] != '.',
				Unstaged: len(xy) > 1 && xy[1] != '.',
			})
		case strings.HasPrefix(part, "u "):
			tokens := strings.SplitN(part, " ", 11)
			if len(tokens) < 11 {
				return nil, fmt.Errorf("malformed unmerged status entry: %q", part)
			}
			entries = append(entries, StatusEntry{Path: tokens[10], Kind: StatusUnmerged, Staged: true, Unstaged: true})
		case strings.HasPrefix(part, "? "):
			entries = append(entries, StatusEntry{Path: strings.TrimPrefix(part, "? "), Kind: StatusUntracked, Unstaged: true})
		case strings.HasPrefix(part, "! ") || strings.HasPrefix(part, "#"):
			continue
		default:
			return nil, fmt.Errorf("unrecognized status entry: %q", part)
		}
	}
	return entries, nil
}
