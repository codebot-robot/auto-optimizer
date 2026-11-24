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

// SessionNode pairs a session summary with its direct children in a run's session tree.
type SessionNode struct {
	sessionapi.SessionSummary
	Children []*SessionNode
}

// RunPageData contains data for rendering the run detail page.
type RunPageData struct {
	Run         *state.Run
	Events      []state.Event
	Sessions    []sessionapi.SessionSummary
	ActionError string
	User        string
}

// SessionTree organizes the run's sessions into a forest reflecting their parent-child hierarchy.
func (d RunPageData) SessionTree() []*SessionNode {
	if len(d.Sessions) == 0 {
		return nil
	}
	byName := make(map[string]*SessionNode, len(d.Sessions))
	ordered := make([]*SessionNode, 0, len(d.Sessions))
	for _, s := range d.Sessions {
		if _, exists := byName[s.Name]; exists {
			continue
		}
		n := &SessionNode{SessionSummary: s}
		byName[s.Name] = n
		ordered = append(ordered, n)
	}

	parentOf := make(map[*SessionNode]*SessionNode, len(ordered))
	var roots []*SessionNode
	for _, n := range ordered {
		p := byName[n.Parent]
		if p != nil && !hasSessionAncestor(parentOf, p, n) {
			p.Children = append(p.Children, n)
			parentOf[n] = p
		} else {
			roots = append(roots, n)
		}
	}
	return roots
}

func hasSessionAncestor(parentOf map[*SessionNode]*SessionNode, node, target *SessionNode) bool {
	for cur := node; cur != nil; cur = parentOf[cur] {
		if cur == target {
			return true
		}
	}
	return false
}

// Title returns the page title for the layout header.
func (d RunPageData) Title() string {
	if d.Run != nil && d.Run.ID != "" {
		return "Run " + d.Run.ID
	}
	return "Run"
}

// InFlight reports whether the run is currently active (non-terminal).
func (d RunPageData) InFlight() bool {
	return d.Run != nil && !d.Run.Phase.IsTerminal()
}

// Finished reports whether the run is in a terminal phase.
func (d RunPageData) Finished() bool {
	return d.Run != nil && d.Run.Phase.IsTerminal()
}

//go:embed run_page.html
var runPageHTML string

var runPageTmpl = sync.OnceValue(func() *template.Template {
	return parsePageTemplate(runPageHTML)
})

// RenderRunPage renders the run detail page.
func RenderRunPage(ctx context.Context, w http.ResponseWriter, data RunPageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	populateUser(ctx, &data.User)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return executePageTemplate(ctx, runPageTmpl(), w, data)
}
