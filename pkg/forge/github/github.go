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

// Package github implements the Forge and ChangeRequestCreator interfaces for GitHub.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

const (
	defaultBaseURL        = "https://api.github.com"
	defaultRateLimitFloor = 100
	githubAPIVersion      = "2022-11-28"
)

type cachedResponse struct {
	etag string
	body []byte
}

// Forge implements forge.Forge, forge.ChangeRequestCreator, and forge.PushCredentialer for GitHub.
type Forge struct {
	token          string
	app            *appAuth
	baseURL        string
	httpClient     *http.Client
	remote         forge.Forge
	rateLimitFloor int
	now            func() time.Time

	mu                 sync.Mutex
	cache              map[string]cachedResponse
	rateLimitRemaining *int
	rateLimitLogged    bool
}

var _ forge.Forge = (*Forge)(nil)
var _ forge.ChangeRequestCreator = (*Forge)(nil)
var _ forge.PushCredentialer = (*Forge)(nil)

// Option configures Forge settings.
type Option func(*Forge)

// WithBaseURL overrides the GitHub REST API base URL (useful for tests).
func WithBaseURL(baseURL string) Option {
	return func(f *Forge) {
		f.baseURL = strings.TrimRight(baseURL, "/")
	}
}

// WithHTTPClient overrides the default HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(f *Forge) {
		f.httpClient = client
	}
}

// WithRateLimitFloor sets the minimum rate limit floor before stopping read calls.
func WithRateLimitFloor(floor int) Option {
	return func(f *Forge) {
		f.rateLimitFloor = floor
	}
}

// WithRemote overrides the underlying forge used for ListRefs and FetchTree.
func WithRemote(remote forge.Forge) Option {
	return func(f *Forge) {
		f.remote = remote
	}
}

func newForge(opts ...Option) *Forge {
	f := &Forge{
		baseURL:        defaultBaseURL,
		httpClient:     http.DefaultClient,
		rateLimitFloor: defaultRateLimitFloor,
		cache:          make(map[string]cachedResponse),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// New creates a new GitHub Forge authenticated with the given personal access token.
func New(token string, opts ...Option) *Forge {
	f := newForge(opts...)
	f.token = token
	if f.remote == nil {
		if token != "" {
			f.remote = forge.NewGitRemoteWithCredentials("x-access-token", token)
		} else {
			f.remote = forge.NewGitRemote()
		}
	}
	return f
}

// NewFromEnv creates a GitHub Forge from environment variables.
// When GITHUB_APP_ID is set, it reads the private key from GITHUB_APP_PRIVATE_KEY
// or GITHUB_APP_PRIVATE_KEY_FILE and fails if the key is missing or invalid.
// Otherwise it falls back to GITHUB_TOKEN, returning (nil, nil) when unset.
func NewFromEnv(opts ...Option) (*Forge, error) {
	if appID := strings.TrimSpace(os.Getenv("GITHUB_APP_ID")); appID != "" {
		keyPEM := []byte(os.Getenv("GITHUB_APP_PRIVATE_KEY"))
		if len(bytes.TrimSpace(keyPEM)) == 0 {
			keyFile := strings.TrimSpace(os.Getenv("GITHUB_APP_PRIVATE_KEY_FILE"))
			if keyFile == "" {
				return nil, errors.New("GITHUB_APP_ID is set without GITHUB_APP_PRIVATE_KEY or GITHUB_APP_PRIVATE_KEY_FILE")
			}
			var err error
			keyPEM, err = os.ReadFile(keyFile)
			if err != nil {
				return nil, fmt.Errorf("reading GITHUB_APP_PRIVATE_KEY_FILE %q: %w", keyFile, err)
			}
		}
		return NewApp(appID, keyPEM, opts...)
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		return New(token, opts...), nil
	}
	return nil, nil
}

func (f *Forge) setAPIHeaders(ctx context.Context, req *http.Request, repoURL string) error {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	token := f.token
	if f.app != nil {
		tok, err := f.installationTokenForRepo(ctx, repoURL)
		if err != nil {
			return err
		}
		token = tok.token
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

func (f *Forge) remoteFor(ctx context.Context, repoURL string) (forge.Forge, error) {
	if f.remote != nil {
		return f.remote, nil
	}
	if f.app != nil {
		tok, err := f.installationTokenForRepo(ctx, repoURL)
		return tok.remote, err
	}
	return forge.NewGitRemote(), nil
}

// PushCredentials returns HTTP basic auth credentials for a control-plane git push.
func (f *Forge) PushCredentials(repoURL string) (username, password string) {
	if f.app != nil {
		ctx, cancel := context.WithTimeout(context.Background(), pushTokenTimeout)
		defer cancel()
		tok, err := f.installationTokenForRepo(ctx, repoURL)
		if err != nil {
			scoped.Log(ctx).Error(err, "failed to obtain GitHub App installation token for push")
			return "", ""
		}
		return "x-access-token", tok.token
	}
	if f.token == "" {
		return "", ""
	}
	return "x-access-token", f.token
}

// ListRefs queries references from the repository using the underlying gitremote forge.
func (f *Forge) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	r, err := f.remoteFor(ctx, repoURL)
	if err != nil {
		return nil, err
	}
	return r.ListRefs(ctx, repoURL)
}

// FetchTree retrieves the file tree at the given commit using the underlying gitremote forge.
// If prefix is non-empty, only files under that path prefix are read;
// blobs outside the prefix are skipped without reading their contents.
// If prefix is empty, all files in the tree are fetched.
func (f *Forge) FetchTree(ctx context.Context, repoURL string, commit string, prefix string) (fs.FS, error) {
	r, err := f.remoteFor(ctx, repoURL)
	if err != nil {
		return nil, err
	}
	return r.FetchTree(ctx, repoURL, commit, prefix)
}

// DefaultBranch queries the repository's default branch using the underlying gitremote forge.
func (f *Forge) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	r, err := f.remoteFor(ctx, repoURL)
	if err != nil {
		return "", err
	}
	return r.DefaultBranch(ctx, repoURL)
}

func parseRepo(repoURL string) (owner, name string, err error) {
	key, err := state.ParseRepoURL(repoURL)
	if err == nil && key.Owner != "" && key.Name != "" && key.Owner != "_" {
		return key.Owner, key.Name, nil
	}

	trimmed := strings.TrimSpace(repoURL)
	trimmed = strings.TrimPrefix(trimmed, "https://")
	trimmed = strings.TrimPrefix(trimmed, "http://")
	trimmed = strings.TrimPrefix(trimmed, "github.com/")
	trimmed = strings.TrimPrefix(trimmed, "git@github.com:")
	trimmed = strings.TrimSuffix(trimmed, ".git")
	trimmed = strings.Trim(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		return parts[0], parts[1], nil
	}
	if err != nil {
		return "", "", fmt.Errorf("parsing repository URL %q: %w", repoURL, err)
	}
	return "", "", fmt.Errorf("could not derive owner and name from repository URL %q", repoURL)
}

func (f *Forge) updateRateLimit(ctx context.Context, resp *http.Response) {
	headerVal := resp.Header.Get("X-RateLimit-Remaining")
	if headerVal == "" {
		return
	}
	val, err := strconv.Atoi(headerVal)
	if err != nil {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.rateLimitRemaining = &val
	if val >= f.rateLimitFloor {
		f.rateLimitLogged = false
	} else if !f.rateLimitLogged {
		f.rateLimitLogged = true
		scoped.Log(ctx).Info("github rate limit floor reached, stopping read calls",
			"remaining", val,
			"floor", f.rateLimitFloor)
	}
}

func (f *Forge) checkRateLimit(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rateLimitRemaining != nil && *f.rateLimitRemaining < f.rateLimitFloor {
		if !f.rateLimitLogged {
			f.rateLimitLogged = true
			scoped.Log(ctx).Info("github rate limit floor reached, stopping read calls",
				"remaining", *f.rateLimitRemaining,
				"floor", f.rateLimitFloor)
		}
		return &forge.RateLimitFloorError{
			Remaining: *f.rateLimitRemaining,
			Floor:     f.rateLimitFloor,
		}
	}
	return nil
}

func (f *Forge) doRead(ctx context.Context, req *http.Request) ([]byte, error) {
	if err := f.checkRateLimit(ctx); err != nil {
		return nil, err
	}

	cacheKey := req.URL.String()
	f.mu.Lock()
	cached, hasCached := f.cache[cacheKey]
	f.mu.Unlock()

	if hasCached && cached.etag != "" {
		req.Header.Set("If-None-Match", cached.etag)
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github API request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	f.updateRateLimit(ctx, resp)

	if resp.StatusCode == http.StatusNotModified {
		if hasCached {
			return cached.body, nil
		}
		return nil, fmt.Errorf("received 304 Not Modified without cached body for %s", cacheKey)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github API GET %s returned %d: %s", req.URL.Path, resp.StatusCode, string(body))
	}

	etag := resp.Header.Get("ETag")
	if etag != "" {
		f.mu.Lock()
		f.cache[cacheKey] = cachedResponse{
			etag: etag,
			body: body,
		}
		f.mu.Unlock()
	}

	return body, nil
}

// ListPullRequests lists pull requests for the repository URL, filtering by head branch prefix if provided.
func (f *Forge) ListPullRequests(ctx context.Context, repoURL, headPrefix string) ([]forge.PullRequest, error) {
	owner, name, err := parseRepo(repoURL)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls?state=all&per_page=100", f.baseURL, owner, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating list pull requests request: %w", err)
	}

	if err := f.setAPIHeaders(ctx, req, repoURL); err != nil {
		return nil, err
	}

	body, err := f.doRead(ctx, req)
	if err != nil {
		return nil, err
	}

	var ghPRs []githubPullRequest
	if err := json.Unmarshal(body, &ghPRs); err != nil {
		return nil, fmt.Errorf("parsing pull requests JSON: %w", err)
	}

	var result []forge.PullRequest
	for _, ghPR := range ghPRs {
		ref := ghPR.Head.Ref
		if headPrefix != "" && !strings.HasPrefix(ref, headPrefix) {
			continue
		}
		pr := forge.PullRequest{
			Number: ghPR.Number,
			Title:  ghPR.Title,
			Body:   ghPR.Body,
			Head:   ref,
			Base:   ghPR.Base.Ref,
			URL:    ghPR.HTMLURL,
			State:  ghPR.State,
		}
		if pr.URL == "" {
			pr.URL = ghPR.URL
		}
		result = append(result, pr)
	}

	return result, nil
}

// OpenPullRequest opens a new pull request on GitHub.
func (f *Forge) OpenPullRequest(ctx context.Context, repoURL, head, base, title, body string) (*forge.PullRequest, error) {
	owner, name, err := parseRepo(repoURL)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls", f.baseURL, owner, name)
	payload := map[string]string{
		"title": title,
		"body":  body,
		"head":  head,
		"base":  base,
	}
	reqData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling pull request payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqData))
	if err != nil {
		return nil, fmt.Errorf("creating open pull request request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if err := f.setAPIHeaders(ctx, req, repoURL); err != nil {
		return nil, err
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opening pull request on github: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close response body", "error", closeErr)
		}
	}()

	f.updateRateLimit(ctx, resp)

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		var ghPR githubPullRequest
		if err := json.Unmarshal(respBody, &ghPR); err != nil {
			return nil, fmt.Errorf("parsing pull request response JSON: %w", err)
		}
		pr := &forge.PullRequest{
			Number: ghPR.Number,
			Title:  ghPR.Title,
			Body:   ghPR.Body,
			Head:   ghPR.Head.Ref,
			Base:   ghPR.Base.Ref,
			URL:    ghPR.HTMLURL,
			State:  ghPR.State,
		}
		if pr.URL == "" {
			pr.URL = ghPR.URL
		}
		return pr, nil
	}

	if resp.StatusCode == http.StatusUnprocessableEntity {
		var ghErr githubErrorResponse
		if unmarshalErr := json.Unmarshal(respBody, &ghErr); unmarshalErr != nil {
			if strings.Contains(strings.ToLower(string(respBody)), "already exists") {
				return nil, &forge.PullRequestExistsError{
					Repo:    repoURL,
					Head:    head,
					Base:    base,
					Message: string(respBody),
				}
			}
			return nil, fmt.Errorf("github API POST %s returned 422: %s", req.URL.Path, string(respBody))
		}
		msgLower := strings.ToLower(ghErr.Message)
		if strings.Contains(msgLower, "already exists") {
			return nil, &forge.PullRequestExistsError{
				Repo:    repoURL,
				Head:    head,
				Base:    base,
				Message: ghErr.Message,
			}
		}
		for _, e := range ghErr.Errors {
			if strings.Contains(strings.ToLower(e.Message), "already exists") {
				return nil, &forge.PullRequestExistsError{
					Repo:    repoURL,
					Head:    head,
					Base:    base,
					Message: e.Message,
				}
			}
		}
		if strings.Contains(strings.ToLower(string(respBody)), "already exists") {
			return nil, &forge.PullRequestExistsError{
				Repo:    repoURL,
				Head:    head,
				Base:    base,
				Message: string(respBody),
			}
		}
	}

	return nil, fmt.Errorf("github API POST %s returned %d: %s", req.URL.Path, resp.StatusCode, string(respBody))
}

type githubPullRequest struct {
	Number  int    `json:"number"`
	Title   string `json:"title"`
	Body    string `json:"body"`
	HTMLURL string `json:"html_url"`
	URL     string `json:"url"`
	State   string `json:"state"`
	Head    struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"base"`
}

type githubErrorResponse struct {
	Message string `json:"message"`
	Errors  []struct {
		Resource string `json:"resource"`
		Code     string `json:"code"`
		Message  string `json:"message"`
		Field    string `json:"field"`
	} `json:"errors"`
}
