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
	"strings"
)

var _ ChangeRequestCreator = (*FakeForge)(nil)

// OpenPullRequest opens a pull request on the fake forge repository.
// If an error was set via SetOpenPullRequestError, that error is returned once.
// If an open pull request already exists with the same head and base, PullRequestExistsError is returned.
func (f *FakeForge) OpenPullRequest(ctx context.Context, repoURL, head, base, title, body string) (*PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.openPullRequestError != nil {
		err := f.openPullRequestError
		f.openPullRequestError = nil
		return nil, err
	}

	for _, existing := range f.pullRequests[repoURL] {
		if existing.State == "open" && existing.Head == head && existing.Base == base {
			return nil, &PullRequestExistsError{
				Repo: repoURL,
				Head: head,
				Base: base,
			}
		}
	}

	number := len(f.pullRequests[repoURL]) + 1
	pr := PullRequest{
		Number: number,
		Title:  title,
		Body:   body,
		Head:   head,
		Base:   base,
		URL:    fmt.Sprintf("%s/pull/%d", strings.TrimSuffix(repoURL, "/"), number),
		State:  "open",
	}

	if f.pullRequests == nil {
		f.pullRequests = make(map[string][]PullRequest)
	}
	f.pullRequests[repoURL] = append(f.pullRequests[repoURL], pr)

	cpy := pr
	return &cpy, nil
}

// ListPullRequests lists pull requests for repoURL whose head branch begins with headPrefix, newest first.
// An empty headPrefix matches all pull requests.
func (f *FakeForge) ListPullRequests(ctx context.Context, repoURL, headPrefix string) ([]PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listPullRequestsError != nil {
		err := f.listPullRequestsError
		f.listPullRequestsError = nil
		return nil, err
	}

	var result []PullRequest
	prs := f.pullRequests[repoURL]
	for i := len(prs) - 1; i >= 0; i-- {
		pr := prs[i]
		if strings.HasPrefix(pr.Head, headPrefix) {
			result = append(result, pr)
		}
	}
	return result, nil
}

// PullRequests returns all pull requests recorded for repoURL in the order they were opened.
func (f *FakeForge) PullRequests(repoURL string) []PullRequest {
	f.mu.RLock()
	defer f.mu.RUnlock()

	prs := f.pullRequests[repoURL]
	cpy := make([]PullRequest, len(prs))
	copy(cpy, prs)
	return cpy
}

// SetPullRequests sets the pull requests recorded for repoURL.
func (f *FakeForge) SetPullRequests(repoURL string, prs []PullRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.pullRequests == nil {
		f.pullRequests = make(map[string][]PullRequest)
	}
	cpy := make([]PullRequest, len(prs))
	copy(cpy, prs)
	f.pullRequests[repoURL] = cpy
}

// SetOpenPullRequestError sets an error to be returned once by the next OpenPullRequest call.
func (f *FakeForge) SetOpenPullRequestError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openPullRequestError = err
}

// SetListPullRequestsError sets an error to be returned once by the next ListPullRequests call.
func (f *FakeForge) SetListPullRequestsError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listPullRequestsError = err
}
