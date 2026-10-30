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

// RunPageData contains data for rendering the run detail page.
type RunPageData struct {
	Run    *state.Run
	Events []state.Event
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return runPageTmpl().Execute(w, data)
}
