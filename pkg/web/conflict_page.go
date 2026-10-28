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
)

// ConflictPageData contains data for rendering the 409 Conflict page.
type ConflictPageData struct {
	Name string
}

// Title returns the page title for the layout header.
func (ConflictPageData) Title() string {
	return "409 Conflict"
}

// InFlight reports whether an operation is in flight.
func (ConflictPageData) InFlight() bool {
	return false
}

//go:embed conflict_page.html
var conflictPageHTML string

var conflictPageTmpl = sync.OnceValue(func() *template.Template {
	return parsePageTemplate(conflictPageHTML)
})

// RenderConflictPage renders the 409 Conflict error page.
func RenderConflictPage(ctx context.Context, w http.ResponseWriter, data ConflictPageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusConflict)
	return conflictPageTmpl().Execute(w, data)
}
