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
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var validBaseSHA = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

// ExportOptions holds options for exporting commits from the sandbox.
type ExportOptions struct {
	RepoDir string // default /workspace/repo
	Base    string // base commit SHA
	DestDir string // local directory for the packfile
}

// ExportResult holds the result of exporting commits from the sandbox.
type ExportResult struct {
	Tip          string
	PackfilePath string
}

var (
	// ErrNoChanges is returned when the sandbox's git diff against base is empty.
	ErrNoChanges = errors.New("no changes against base commit")

	// ErrGoalsChanged is returned when the sandbox modified files under .goals/.
	ErrGoalsChanged = errors.New("changes under .goals/ are not allowed")

	// ErrUncommittedChanges is returned when the sandbox repository has uncommitted changes.
	ErrUncommittedChanges = errors.New("uncommitted changes in repository")
)

// ExportCommits verifies that there are no uncommitted changes and valid changes against
// base commit (and none under .goals/), and streams a git packfile containing all objects
// between Base and HEAD to a local file in DestDir.
func (s *Sandbox) ExportCommits(ctx context.Context, opts ExportOptions) (*ExportResult, error) {
	log := scoped.Log(ctx)
	repoDir := opts.RepoDir
	if repoDir == "" {
		repoDir = "/workspace/repo"
	}
	if !validBaseSHA.MatchString(opts.Base) {
		return nil, fmt.Errorf("invalid base commit SHA: %q", opts.Base)
	}
	if opts.DestDir == "" {
		return nil, errors.New("destination directory is required")
	}

	// 1. Verify there are no uncommitted changes.
	statusRes, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", repoDir, "status", "--porcelain"},
	})
	if err != nil {
		return nil, fmt.Errorf("git status: %w", err)
	}
	if statusRes.ExitCode != 0 {
		return nil, fmt.Errorf("git status failed with exit code %d: %s", statusRes.ExitCode, statusRes.Stderr)
	}
	if strings.TrimSpace(statusRes.Stdout) != "" {
		return nil, ErrUncommittedChanges
	}

	// 2. Check diff against base commit.
	diffQuietRes, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", repoDir, "diff", "--quiet", opts.Base, "HEAD"},
	})
	if err != nil {
		return nil, fmt.Errorf("git diff --quiet: %w", err)
	}
	if diffQuietRes.ExitCode == 0 {
		return nil, ErrNoChanges
	}
	if diffQuietRes.ExitCode != 1 {
		return nil, fmt.Errorf("git diff --quiet failed with exit code %d: %s", diffQuietRes.ExitCode, diffQuietRes.Stderr)
	}

	diffNamesRes, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", repoDir, "diff", "--name-only", opts.Base, "HEAD"},
	})
	if err != nil {
		return nil, fmt.Errorf("git diff --name-only: %w", err)
	}
	if diffNamesRes.ExitCode != 0 {
		return nil, fmt.Errorf("git diff --name-only failed with exit code %d: %s", diffNamesRes.ExitCode, diffNamesRes.Stderr)
	}

	for _, line := range strings.Split(diffNamesRes.Stdout, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		clean := path.Clean(strings.TrimPrefix(trimmed, "./"))
		if clean == ".goals" || strings.HasPrefix(clean, ".goals/") {
			return nil, ErrGoalsChanged
		}
	}

	// 3. Read Tip with git rev-parse HEAD.
	revParseRes, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", repoDir, "rev-parse", "HEAD"},
	})
	if err != nil {
		return nil, fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	if revParseRes.ExitCode != 0 {
		return nil, fmt.Errorf("git rev-parse HEAD failed with exit code %d: %s", revParseRes.ExitCode, revParseRes.Stderr)
	}
	tip := strings.TrimSpace(revParseRes.Stdout)
	if tip == "" {
		return nil, errors.New("git rev-parse HEAD returned empty tip")
	}

	// 4. Produce the packfile streaming stdout to a temporary file in DestDir.
	if err := os.MkdirAll(opts.DestDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create destination directory: %w", err)
	}

	packFilename := filepath.Join(opts.DestDir, fmt.Sprintf("pack-%s.pack", tip))
	tmpFilename := packFilename + ".tmp"

	tmpFile, err := os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary packfile %s: %w", tmpFilename, err)
	}

	shouldDeleteTempFile := true
	defer func() {
		if shouldDeleteTempFile {
			if removeErr := os.Remove(tmpFilename); removeErr != nil && !os.IsNotExist(removeErr) {
				log.V(2).Info("unable to remove temporary packfile", "error", removeErr)
			}
		}
	}()
	defer func() {
		if closeErr := tmpFile.Close(); closeErr != nil {
			log.V(2).Info("failed to close temporary packfile", "error", closeErr)
		}
	}()

	revListRes, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", repoDir, "rev-list", "--objects", opts.Base + "..HEAD"},
	})
	if err != nil {
		return nil, fmt.Errorf("git rev-list: %w", err)
	}
	if revListRes.ExitCode != 0 {
		return nil, fmt.Errorf("git rev-list failed with exit code %d: %s", revListRes.ExitCode, revListRes.Stderr)
	}

	packRes, err := s.ExecCommand(ctx, tools.ExecCommandOptions{
		Command: []string{"git", "-C", repoDir, "pack-objects", "--stdout", "--no-thin", "-q"},
		Stdin:   strings.NewReader(revListRes.Stdout),
		Stdout:  tmpFile,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to produce packfile: %w", err)
	}
	if packRes.ExitCode != 0 {
		return nil, fmt.Errorf("packfile generation failed with exit code %d: %s", packRes.ExitCode, packRes.Stderr)
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temporary packfile: %w", err)
	}
	if err := os.Rename(tmpFilename, packFilename); err != nil {
		return nil, fmt.Errorf("failed to rename temporary packfile to final path: %w", err)
	}
	shouldDeleteTempFile = false

	return &ExportResult{
		Tip:          tip,
		PackfilePath: packFilename,
	}, nil
}
