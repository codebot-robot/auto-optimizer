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

	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// SessionListPageData contains data for rendering the session list page.
type SessionListPageData struct {
	Sessions []sessionapi.SessionSummary
	Runs     map[string]*state.Run
	User     string
}

// SessionRunPath returns the structured repo/goal/run path for a session's run, or an empty string.
func (d SessionListPageData) SessionRunPath(s sessionapi.SessionSummary) string {
	if s.RunID == "" || d.Runs == nil {
		return ""
	}
	return runPath(d.Runs[s.RunID])
}

// SessionLabel returns the display label for a session, using the structured run path when available.
func (d SessionListPageData) SessionLabel(s sessionapi.SessionSummary) string {
	if p := d.SessionRunPath(s); p != "" {
		return p
	}
	return s.Name
}

// Title returns the page title for the layout header.
func (SessionListPageData) Title() string {
	return "Sessions"
}

// InFlight reports whether an operation is in flight.
func (SessionListPageData) InFlight() bool {
	return false
}

//go:embed sessionlist_page.html
var sessionListPageHTML string

var sessionListPageTmpl = sync.OnceValue(func() *template.Template {
	return parsePageTemplate(sessionListPageHTML)
})

// RenderSessionListPage renders the session list page.
func RenderSessionListPage(ctx context.Context, w http.ResponseWriter, data SessionListPageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	populateUser(ctx, &data.User)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return executePageTemplate(ctx, sessionListPageTmpl(), w, data)
}
