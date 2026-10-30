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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"
)

const (
	defaultRequestTimeout = 30 * time.Second
	gitUserAgent          = "auto-optimizer"
)

type httpCacheEntry struct {
	etag          string
	refs          map[string]string
	defaultBranch string
}

// GitRemote implements Forge and PushCredentialer. For http:// and https:// remotes,
// ref advertisement (ListRefs and DefaultBranch) is executed over smart HTTP using net/http,
// supporting Git protocol v2 with v0 fallback, conditional requests with ETags, and
// single-response HEAD symref resolution for DefaultBranch. Non-HTTP schemes (such as file://
// or local filesystem paths) fall back to go-git's ListContext. Depth-1 in-memory fetches
// (FetchTree) use go-git.
type GitRemote struct {
	username   string
	password   string
	timeout    time.Duration
	httpClient *http.Client

	mu        sync.Mutex
	cache     map[string]httpCacheEntry
	v2Servers map[string]bool
}

var _ Forge = (*GitRemote)(nil)
var _ PushCredentialer = (*GitRemote)(nil)

// GitRemoteOption configures GitRemote options.
type GitRemoteOption func(*GitRemote)

// WithTimeout configures the per-request HTTP timeout for smart HTTP requests.
func WithTimeout(d time.Duration) GitRemoteOption {
	return func(g *GitRemote) {
		g.timeout = d
	}
}

// WithHTTPClient overrides the default HTTP client used for smart HTTP requests.
func WithHTTPClient(client *http.Client) GitRemoteOption {
	return func(g *GitRemote) {
		g.httpClient = client
	}
}

// NewGitRemote creates a new GitRemote forge instance.
func NewGitRemote(opts ...GitRemoteOption) *GitRemote {
	g := &GitRemote{
		timeout:   defaultRequestTimeout,
		cache:     make(map[string]httpCacheEntry),
		v2Servers: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// NewGitRemoteWithCredentials creates a GitRemote forge instance with HTTP basic auth credentials.
func NewGitRemoteWithCredentials(username, password string, opts ...GitRemoteOption) *GitRemote {
	g := &GitRemote{
		username:  username,
		password:  password,
		timeout:   defaultRequestTimeout,
		cache:     make(map[string]httpCacheEntry),
		v2Servers: make(map[string]bool),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// PushCredentials returns the HTTP basic auth credentials configured for git operations.
func (g *GitRemote) PushCredentials(repoURL string) (username, password string) {
	return g.username, g.password
}

func (g *GitRemote) auth() transport.AuthMethod {
	if g.username != "" || g.password != "" {
		return &githttp.BasicAuth{
			Username: g.username,
			Password: g.password,
		}
	}
	return nil
}

func isHTTPURL(repoURL string) bool {
	u, err := url.Parse(repoURL)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func copyRefs(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// ListRefs queries the remote repository's ref advertisement. For http:// and https://
// remotes, it queries smart HTTP using net/http (Git protocol v2 with v0 fallback).
// Non-HTTP schemes fall back to go-git's ListContext.
func (g *GitRemote) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	if isHTTPURL(repoURL) {
		refs, _, err := g.discoverSmartHTTP(ctx, repoURL)
		return refs, err
	}
	return g.listRefsGoGit(ctx, repoURL)
}

// DefaultBranch queries the remote repository to determine its default branch from the HEAD symref.
// For http:// and https:// remotes, the default branch is extracted directly from the HEAD
// symref target in the smart HTTP ref advertisement response without a second round trip.
// Non-HTTP schemes fall back to go-git's ListContext.
func (g *GitRemote) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	if isHTTPURL(repoURL) {
		_, defaultBranch, err := g.discoverSmartHTTP(ctx, repoURL)
		if err != nil {
			return "", err
		}
		if defaultBranch == "" {
			return "", fmt.Errorf("remote %s has no HEAD symref", repoURL)
		}
		return defaultBranch, nil
	}
	return g.defaultBranchGoGit(ctx, repoURL)
}

func (g *GitRemote) listRefsGoGit(ctx context.Context, repoURL string) (map[string]string, error) {
	rem := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	})

	refs, err := rem.ListContext(ctx, &git.ListOptions{
		Auth: g.auth(),
	})
	if err != nil {
		return nil, fmt.Errorf("listing remote refs for %s: %w", repoURL, err)
	}

	result := make(map[string]string, len(refs))
	for _, ref := range refs {
		if ref.Type() == plumbing.HashReference && ref.Name() != plumbing.HEAD {
			result[ref.Name().String()] = ref.Hash().String()
		}
	}

	return result, nil
}

func (g *GitRemote) defaultBranchGoGit(ctx context.Context, repoURL string) (string, error) {
	rem := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	})

	refs, err := rem.ListContext(ctx, &git.ListOptions{
		Auth: g.auth(),
	})
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
// If prefix is non-empty, only files under that path prefix are read by finding the subtree;
// blobs outside the prefix are skipped without reading their contents. If prefix is empty,
// all files in the tree are fetched.
func (g *GitRemote) FetchTree(ctx context.Context, repoURL string, commit string, prefix string) (fs.FS, error) {
	storer := memory.NewStorage()
	r, err := git.CloneContext(ctx, storer, nil, &git.CloneOptions{
		URL:   repoURL,
		Depth: 1,
		Auth:  g.auth(),
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
	cleanPrefix := path.Clean(strings.TrimPrefix(prefix, "/"))
	if cleanPrefix == "." || cleanPrefix == "" {
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
		return NewInMemoryFS(files), nil
	}

	subTree, err := tree.Tree(cleanPrefix)
	if err == nil {
		err = subTree.Files().ForEach(func(f *object.File) error {
			contents, err := f.Contents()
			if err != nil {
				return err
			}
			files[path.Join(cleanPrefix, f.Name)] = []byte(contents)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("reading tree files under %s: %w", cleanPrefix, err)
		}
		return NewInMemoryFS(files), nil
	}

	if !errors.Is(err, object.ErrDirectoryNotFound) && !errors.Is(err, object.ErrEntryNotFound) {
		return nil, fmt.Errorf("finding subtree %s: %w", cleanPrefix, err)
	}

	file, fileErr := tree.File(cleanPrefix)
	if fileErr == nil {
		contents, err := file.Contents()
		if err != nil {
			return nil, fmt.Errorf("reading file %s: %w", cleanPrefix, err)
		}
		files[cleanPrefix] = []byte(contents)
		return NewInMemoryFS(files), nil
	}
	if !errors.Is(fileErr, object.ErrFileNotFound) && !errors.Is(fileErr, object.ErrEntryNotFound) {
		return nil, fmt.Errorf("finding file %s: %w", cleanPrefix, fileErr)
	}

	return NewInMemoryFS(files), nil
}

type pktType int

const (
	pktData pktType = iota
	pktFlush
	pktDelim
	pktResponseEnd
)

type pktLine struct {
	pType   pktType
	payload []byte
}

func writePktLine(w io.Writer, s string) error {
	length := len(s) + 4
	if length > 65520 {
		return fmt.Errorf("pkt-line payload too large: %d", len(s))
	}
	_, err := fmt.Fprintf(w, "%04x%s", length, s)
	return err
}

func writePktDelim(w io.Writer) error {
	_, err := io.WriteString(w, "0001")
	return err
}

func writePktFlush(w io.Writer) error {
	_, err := io.WriteString(w, "0000")
	return err
}

func parsePktLines(data []byte) ([]pktLine, error) {
	var lines []pktLine
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("truncated pkt-line header: %q", data)
		}
		hexLen := string(data[:4])
		data = data[4:]
		switch hexLen {
		case "0000":
			lines = append(lines, pktLine{pType: pktFlush})
			continue
		case "0001":
			lines = append(lines, pktLine{pType: pktDelim})
			continue
		case "0002":
			lines = append(lines, pktLine{pType: pktResponseEnd})
			continue
		}

		length, err := strconv.ParseUint(hexLen, 16, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid pkt-line length %q: %w", hexLen, err)
		}
		if length < 4 {
			return nil, fmt.Errorf("invalid pkt-line length %d (must be >= 4)", length)
		}
		payloadLen := int(length) - 4
		if len(data) < payloadLen {
			return nil, fmt.Errorf("pkt-line length %d exceeds remaining data %d", length, len(data))
		}
		payload := data[:payloadLen]
		data = data[payloadLen:]
		payload = bytes.TrimRight(payload, "\r\n")
		lines = append(lines, pktLine{pType: pktData, payload: payload})
	}
	return lines, nil
}

func isV2UnsupportedStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusMethodNotAllowed,
		http.StatusUnsupportedMediaType,
		http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

// The Git smart HTTP specification describes discovery first (a GET to /info/refs
// followed by a POST to /git-upload-pack). While protocol v0 answers in a single GET,
// that response carries every ref unfiltered (including refs/pull/* and tags, which
// on hosts like GitHub can be massive). Protocol v2 allows ref-prefix filtering, but
// requires version negotiation on first contact. Once the server is known to support
// protocol v2, discovery is skipped after the first success and later polls go straight
// to the ls-refs POST, saving a round trip.
func (g *GitRemote) postLsRefs(ctx context.Context, client *http.Client, baseURL, repoURL string, timeout time.Duration, cached httpCacheEntry, hasCached bool) (map[string]string, string, int, error) {
	var postBuf bytes.Buffer
	if err := writePktLine(&postBuf, "command=ls-refs\n"); err != nil {
		return nil, "", 0, err
	}
	if err := writePktDelim(&postBuf); err != nil {
		return nil, "", 0, err
	}
	if err := writePktLine(&postBuf, "peel\n"); err != nil {
		return nil, "", 0, err
	}
	if err := writePktLine(&postBuf, "symrefs\n"); err != nil {
		return nil, "", 0, err
	}
	if err := writePktLine(&postBuf, "ref-prefix HEAD\n"); err != nil {
		return nil, "", 0, err
	}
	if err := writePktLine(&postBuf, "ref-prefix refs/heads/\n"); err != nil {
		return nil, "", 0, err
	}
	if err := writePktFlush(&postBuf); err != nil {
		return nil, "", 0, err
	}

	postURL := baseURL + "/git-upload-pack"
	postCtx, cancelPost := context.WithTimeout(ctx, timeout)
	defer cancelPost()

	postReq, err := http.NewRequestWithContext(postCtx, http.MethodPost, postURL, &postBuf)
	if err != nil {
		return nil, "", 0, fmt.Errorf("creating git smart HTTP POST request for %s: %w", repoURL, err)
	}
	postReq.Header.Set("Git-Protocol", "version=2")
	postReq.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	postReq.Header.Set("Accept", "application/x-git-upload-pack-result")
	postReq.Header.Set("User-Agent", gitUserAgent)
	if g.username != "" || g.password != "" {
		postReq.SetBasicAuth(g.username, g.password)
	}
	if hasCached && cached.etag != "" {
		postReq.Header.Set("If-None-Match", cached.etag)
	}

	postResp, err := client.Do(postReq)
	if err != nil {
		return nil, "", 0, fmt.Errorf("git smart HTTP POST request failed for %s: %w", repoURL, err)
	}
	defer func() {
		if closeErr := postResp.Body.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	if postResp.StatusCode == http.StatusNotModified {
		if hasCached {
			return copyRefs(cached.refs), cached.defaultBranch, postResp.StatusCode, nil
		}
		return nil, "", postResp.StatusCode, fmt.Errorf("git smart HTTP POST returned 304 without cached data for %s", repoURL)
	}

	postBody, err := io.ReadAll(postResp.Body)
	if err != nil {
		return nil, "", postResp.StatusCode, fmt.Errorf("reading git smart HTTP POST response for %s: %w", repoURL, err)
	}

	if postResp.StatusCode < 200 || postResp.StatusCode >= 300 {
		return nil, "", postResp.StatusCode, fmt.Errorf("git smart HTTP POST request for %s failed with status %d: %s", repoURL, postResp.StatusCode, string(postBody))
	}

	etag := cached.etag
	if postETag := postResp.Header.Get("ETag"); postETag != "" {
		etag = postETag
	}

	respLines, err := parsePktLines(postBody)
	if err != nil {
		return nil, "", postResp.StatusCode, fmt.Errorf("parsing git smart HTTP POST pkt-lines for %s: %w", repoURL, err)
	}

	refs := make(map[string]string)
	var defaultBranch string

	for _, line := range respLines {
		if line.pType != pktData {
			continue
		}
		fields := strings.Fields(string(line.payload))
		if len(fields) < 2 {
			continue
		}
		oid := fields[0]
		refname := fields[1]

		for _, attr := range fields[2:] {
			if strings.HasPrefix(attr, "symref-target:") {
				target := strings.TrimPrefix(attr, "symref-target:")
				if refname == "HEAD" {
					defaultBranch = strings.TrimPrefix(target, "refs/heads/")
				}
			}
		}

		if oid != "unborn" && refname != "HEAD" {
			refs[refname] = oid
		}
	}

	g.mu.Lock()
	g.v2Servers[baseURL] = true
	g.cache[baseURL] = httpCacheEntry{
		etag:          etag,
		refs:          refs,
		defaultBranch: defaultBranch,
	}
	g.mu.Unlock()

	return refs, defaultBranch, postResp.StatusCode, nil
}

func (g *GitRemote) discoverSmartHTTP(ctx context.Context, repoURL string) (map[string]string, string, error) {
	client := g.httpClient
	if client == nil {
		client = http.DefaultClient
	}

	timeout := g.timeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}

	baseURL := strings.TrimRight(repoURL, "/")
	infoURL := baseURL + "/info/refs?service=git-upload-pack"

	g.mu.Lock()
	isV2 := g.v2Servers[baseURL]
	cached, hasCached := g.cache[baseURL]
	g.mu.Unlock()

	if isV2 {
		refs, defaultBranch, status, err := g.postLsRefs(ctx, client, baseURL, repoURL, timeout, cached, hasCached)
		if err == nil {
			return copyRefs(refs), defaultBranch, nil
		}
		if !isV2UnsupportedStatus(status) {
			return nil, "", err
		}

		// The POST failed with a status suggesting the server no longer speaks v2 (e.g. 400 or 404).
		// Forget the entry and fall through to redo discovery.
		scoped.Log(ctx).V(2).Info("git protocol v2 POST failed, forgetting v2 support and redoing discovery", "repo", repoURL, "status", status, "error", err)
		g.mu.Lock()
		delete(g.v2Servers, baseURL)
		delete(g.cache, baseURL)
		g.mu.Unlock()
		hasCached = false
		cached = httpCacheEntry{}
	}

	getCtx, cancelGet := context.WithTimeout(ctx, timeout)
	defer cancelGet()

	getReq, err := http.NewRequestWithContext(getCtx, http.MethodGet, infoURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("creating git smart HTTP GET request for %s: %w", repoURL, err)
	}
	getReq.Header.Set("Git-Protocol", "version=2")
	getReq.Header.Set("User-Agent", gitUserAgent)
	if g.username != "" || g.password != "" {
		getReq.SetBasicAuth(g.username, g.password)
	}
	if hasCached && cached.etag != "" {
		getReq.Header.Set("If-None-Match", cached.etag)
	}

	getResp, err := client.Do(getReq)
	if err != nil {
		return nil, "", fmt.Errorf("git smart HTTP GET request failed for %s: %w", repoURL, err)
	}
	defer func() {
		if closeErr := getResp.Body.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	if getResp.StatusCode == http.StatusNotModified {
		if hasCached {
			return copyRefs(cached.refs), cached.defaultBranch, nil
		}
		return nil, "", fmt.Errorf("git smart HTTP GET returned 304 without cached data for %s", repoURL)
	}

	getBody, err := io.ReadAll(getResp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("reading git smart HTTP GET response for %s: %w", repoURL, err)
	}

	if getResp.StatusCode < 200 || getResp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("git smart HTTP GET request for %s failed with status %d: %s", repoURL, getResp.StatusCode, string(getBody))
	}
	etag := getResp.Header.Get("ETag")
	if etag != "" {
		cached.etag = etag
	}

	lines, err := parsePktLines(getBody)
	if err != nil {
		return nil, "", fmt.Errorf("parsing git smart HTTP GET response pkt-lines for %s: %w", repoURL, err)
	}

	discoveredV2 := false
	idx := 0
	if idx < len(lines) && lines[idx].pType == pktData && strings.HasPrefix(string(lines[idx].payload), "# service=") {
		idx++
		if idx < len(lines) && lines[idx].pType == pktFlush {
			idx++
		}
	}
	if idx < len(lines) && lines[idx].pType == pktData {
		payloadStr := string(lines[idx].payload)
		if payloadStr == "version 2" || strings.HasPrefix(payloadStr, "version 2") {
			discoveredV2 = true
		}
	}

	if discoveredV2 {
		refs, defaultBranch, _, err := g.postLsRefs(ctx, client, baseURL, repoURL, timeout, cached, hasCached)
		if err != nil {
			return nil, "", err
		}
		return copyRefs(refs), defaultBranch, nil
	}

	// Protocol v0 fallback
	g.mu.Lock()
	delete(g.v2Servers, baseURL)
	g.mu.Unlock()

	refs := make(map[string]string)
	var headTarget string
	var defaultBranch string

	firstRefLine := true
	for i := idx; i < len(lines); i++ {
		line := lines[i]
		if line.pType == pktFlush {
			break
		}
		if line.pType != pktData {
			continue
		}

		payload := line.payload
		if firstRefLine {
			firstRefLine = false
			if nulIdx := bytes.IndexByte(payload, 0); nulIdx != -1 {
				capsPart := string(payload[nulIdx+1:])
				payload = payload[:nulIdx]
				for _, cap := range strings.Fields(capsPart) {
					if strings.HasPrefix(cap, "symref=HEAD:") {
						headTarget = strings.TrimPrefix(cap, "symref=HEAD:")
						defaultBranch = strings.TrimPrefix(headTarget, "refs/heads/")
					}
				}
			}
		}

		fields := strings.Fields(string(payload))
		if len(fields) < 2 {
			continue
		}
		oid := fields[0]
		refname := fields[1]
		if refname == "HEAD" || refname == "capabilities^{}" || strings.HasSuffix(refname, "^{}") {
			continue
		}
		refs[refname] = oid
	}

	g.mu.Lock()
	g.cache[baseURL] = httpCacheEntry{
		etag:          etag,
		refs:          refs,
		defaultBranch: defaultBranch,
	}
	g.mu.Unlock()

	return copyRefs(refs), defaultBranch, nil
}
