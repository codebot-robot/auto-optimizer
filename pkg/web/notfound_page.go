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

// NotFoundPageData contains data for rendering the 404 Not Found page.
type NotFoundPageData struct {
	Message string
}

// Title returns the page title for the layout header.
func (NotFoundPageData) Title() string {
	return "404 Not Found"
}

// InFlight reports whether an operation is in flight.
func (NotFoundPageData) InFlight() bool {
	return false
}

//go:embed notfound_page.html
var notFoundPageHTML string

var notFoundPageTmpl = sync.OnceValue(func() *template.Template {
	return parsePageTemplate(notFoundPageHTML)
})

// RenderNotFoundPage renders the 404 Not Found error page.
func RenderNotFoundPage(ctx context.Context, w http.ResponseWriter, data NotFoundPageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	return notFoundPageTmpl().Execute(w, data)
}
