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
)

// TranscriptPageData contains data for rendering the session transcript page.
type TranscriptPageData struct {
	Detail sessionapi.SessionDetail
	Full   bool
}

// Summary returns the session summary.
func (d TranscriptPageData) Summary() sessionapi.SessionSummary {
	return d.Detail.Summary
}

// Messages returns the transcript messages.
func (d TranscriptPageData) Messages() []sessionapi.Message {
	return d.Detail.Messages
}

// Title returns the page title for the layout header.
func (d TranscriptPageData) Title() string {
	return "Session " + d.Detail.Summary.Name
}

// InFlight reports whether a turn is currently in flight.
func (d TranscriptPageData) InFlight() bool {
	return d.Detail.Summary.InFlight
}

//go:embed transcript_page.html
var transcriptPageHTML string

var transcriptPageTmpl = sync.OnceValue(func() *template.Template {
	return parsePageTemplate(transcriptPageHTML)
})

// RenderTranscriptPage renders the transcript page for a session.
func RenderTranscriptPage(ctx context.Context, w http.ResponseWriter, data TranscriptPageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return transcriptPageTmpl().Execute(w, data)
}
