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

package web

import (
	"context"
	_ "embed"
	"html/template"
	"net/http"
	"sync"

	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// GoalEntry pairs a goal record with the phase of its latest run.
type GoalEntry struct {
	Record      *state.GoalRecord
	LatestPhase state.RunPhase
}

// RepoPageData contains data for rendering the repository detail page.
type RepoPageData struct {
	Repo       *state.Repo
	Goals      []GoalEntry
	RecentRuns []*state.Run
}

// Title returns the page title for the layout header.
func (d RepoPageData) Title() string {
	if d.Repo != nil {
		return d.Repo.Key().String()
	}
	return "Repository"
}

// InFlight reports whether an operation is in flight.
func (RepoPageData) InFlight() bool {
	return false
}

//go:embed repo_page.html
var repoPageHTML string

var repoPageTmpl = sync.OnceValue(func() *template.Template {
	return parsePageTemplate(repoPageHTML)
})

// RenderRepoPage renders the repository detail page.
func RenderRepoPage(ctx context.Context, w http.ResponseWriter, data RepoPageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return repoPageTmpl().Execute(w, data)
}
