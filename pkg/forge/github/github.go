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
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

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
	baseURL        string
	httpClient     *http.Client
	remote         forge.Forge
	rateLimitFloor int

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

// New creates a new GitHub Forge authenticated with the given personal access token.
func New(token string, opts ...Option) *Forge {
	f := &Forge{
		token:          token,
		baseURL:        defaultBaseURL,
		httpClient:     http.DefaultClient,
		rateLimitFloor: defaultRateLimitFloor,
		cache:          make(map[string]cachedResponse),
	}
	for _, opt := range opts {
		opt(f)
	}
	if f.remote == nil {
		if token != "" {
			f.remote = forge.NewGitRemoteWithCredentials("x-access-token", token)
		} else {
			f.remote = forge.NewGitRemote()
		}
	}
	return f
}

// NewFromEnv creates a new GitHub Forge using the GITHUB_TOKEN environment variable.
func NewFromEnv(opts ...Option) *Forge {
	return New(os.Getenv("GITHUB_TOKEN"), opts...)
}

// PushCredentials returns HTTP basic auth credentials for a control-plane git push.
func (f *Forge) PushCredentials(repoURL string) (username, password string) {
	if f.token == "" {
		return "", ""
	}
	return "x-access-token", f.token
}

// ListRefs queries references from the repository using the underlying gitremote forge.
func (f *Forge) ListRefs(ctx context.Context, repoURL string) (map[string]string, error) {
	return f.remote.ListRefs(ctx, repoURL)
}

// FetchTree retrieves the file tree at the given commit using the underlying gitremote forge.
func (f *Forge) FetchTree(ctx context.Context, repoURL string, commit string) (fs.FS, error) {
	return f.remote.FetchTree(ctx, repoURL, commit)
}

// DefaultBranch queries the repository's default branch using the underlying gitremote forge.
func (f *Forge) DefaultBranch(ctx context.Context, repoURL string) (string, error) {
	return f.remote.DefaultBranch(ctx, repoURL)
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

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	if f.token != "" {
		req.Header.Set("Authorization", "Bearer "+f.token)
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
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	if f.token != "" {
		req.Header.Set("Authorization", "Bearer "+f.token)
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
