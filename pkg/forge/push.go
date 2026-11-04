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
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

// ErrBranchMoved is returned when pushing a branch fails because the remote branch moved.
var ErrBranchMoved = errors.New("remote branch moved")

// BranchMovedError is returned when a push fails because the remote branch moved.
type BranchMovedError struct {
	Branch string
	Remote string
	Err    error
}

func (e *BranchMovedError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("remote branch %s on %s moved: %v", e.Branch, e.Remote, e.Err)
	}
	return fmt.Sprintf("remote branch %s on %s moved", e.Branch, e.Remote)
}

func (e *BranchMovedError) Is(target error) bool {
	return target == ErrBranchMoved || (e.Err != nil && errors.Is(e.Err, target))
}

func (e *BranchMovedError) Unwrap() error {
	return e.Err
}

// PushPackfileOptions configures a packfile push operation.
type PushPackfileOptions struct {
	RemoteURL   string
	Branch      string        // for example ao/<goal>/<run-id>
	Tip, Base   plumbing.Hash // Tip must be reachable to Base through the packfile's commits
	Packfile    io.Reader
	Credentials PushCredentialer // may be nil for unauthenticated remotes
}

// PushPackfile imports a packfile into a temporary bare repository and pushes Tip to Branch on the remote.
func PushPackfile(ctx context.Context, opts PushPackfileOptions) error {
	remoteHost := "unknown"
	u, err := url.Parse(opts.RemoteURL)
	if err != nil {
		remoteHost = "invalid-url"
	} else if u.Host != "" {
		remoteHost = u.Host
	} else if u.Scheme != "" {
		remoteHost = u.Scheme
	} else {
		remoteHost = "local"
	}
	scoped.Log(ctx).Info("pushing packfile to remote", "host", remoteHost, "branch", opts.Branch)

	tmpDir, err := os.MkdirTemp("", "ao-push-*")
	if err != nil {
		return fmt.Errorf("creating temporary directory: %w", err)
	}
	defer func() {
		if rmErr := os.RemoveAll(tmpDir); rmErr != nil {
			scoped.Log(ctx).V(2).Info("failed to remove temporary directory", "path", tmpDir, "error", rmErr)
		}
	}()

	r, err := git.PlainInit(tmpDir, true)
	if err != nil {
		return fmt.Errorf("initializing bare repository: %w", err)
	}

	rem, err := r.CreateRemote(&config.RemoteConfig{
		Name: "origin",
		URLs: []string{opts.RemoteURL},
	})
	if err != nil {
		return fmt.Errorf("creating remote origin: %w", err)
	}

	var auth transport.AuthMethod
	if opts.Credentials != nil {
		username, password := opts.Credentials.PushCredentials(opts.RemoteURL)
		if username != "" || password != "" {
			auth = &githttp.BasicAuth{
				Username: username,
				Password: password,
			}
		}
	}

	branch := strings.TrimPrefix(opts.Branch, "refs/heads/")
	branchRef := plumbing.ReferenceName("refs/heads/" + branch)

	// List remote first to discover current branch hash for force-with-lease.
	remoteRefs, err := rem.ListContext(ctx, &git.ListOptions{
		Auth: auth,
	})
	if err != nil && !errors.Is(err, transport.ErrEmptyRemoteRepository) {
		return fmt.Errorf("listing remote %s refs: %w", remoteHost, err)
	}

	var requireRemoteRefs []config.RefSpec
	for _, ref := range remoteRefs {
		if ref.Name() == branchRef {
			requireRemoteRefs = []config.RefSpec{
				config.RefSpec(fmt.Sprintf("%s:%s", ref.Hash().String(), branchRef.String())),
			}
			break
		}
	}

	// Fetch remote's branches so Base and its history exist locally.
	err = r.FetchContext(ctx, &git.FetchOptions{
		RemoteName: "origin",
		Auth:       auth,
		RefSpecs:   []config.RefSpec{"+refs/heads/*:refs/remotes/origin/*"},
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) && !errors.Is(err, transport.ErrEmptyRemoteRepository) {
		return fmt.Errorf("fetching remote %s: %w", remoteHost, err)
	}

	if err := packfile.UpdateObjectStorage(r.Storer, opts.Packfile); err != nil {
		return fmt.Errorf("importing packfile: %w", err)
	}

	tipCommit, err := r.CommitObject(opts.Tip)
	if err != nil {
		return fmt.Errorf("tip commit %s is missing after import: %w", opts.Tip, err)
	}

	baseCommit, err := r.CommitObject(opts.Base)
	if err != nil {
		return fmt.Errorf("base commit %s is missing: %w", opts.Base, err)
	}

	isAncestor, err := baseCommit.IsAncestor(tipCommit)
	if err != nil {
		return fmt.Errorf("checking if tip %s reaches base %s: %w", opts.Tip, opts.Base, err)
	}
	if !isAncestor {
		return fmt.Errorf("tip commit %s does not reach base commit %s", opts.Tip, opts.Base)
	}

	if err := r.Storer.SetReference(plumbing.NewHashReference(branchRef, opts.Tip)); err != nil {
		return fmt.Errorf("setting reference %s: %w", branchRef, err)
	}

	pushOpts := &git.PushOptions{
		RemoteName:        "origin",
		Auth:              auth,
		RefSpecs:          []config.RefSpec{config.RefSpec(fmt.Sprintf("%s:%s", branchRef.String(), branchRef.String()))},
		RequireRemoteRefs: requireRemoteRefs,
		Force:             true,
	}

	if err := r.PushContext(ctx, pushOpts); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		if strings.Contains(err.Error(), "required to be") || strings.Contains(err.Error(), "non-fast-forward") {
			return &BranchMovedError{
				Branch: branch,
				Remote: remoteHost,
				Err:    err,
			}
		}
		return fmt.Errorf("pushing to %s: %w", remoteHost, err)
	}

	return nil
}
