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
	_ "embed"
	"html/template"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
)

//go:embed layout.html
var layoutHTML string

const maxTruncateLen = 500

var templateFuncs = template.FuncMap{
	"formatTime":  formatTime,
	"truncate":    truncate,
	"canTruncate": canTruncate,
	"isTool":      isTool,
}

func parsePageTemplate(pageHTML string) *template.Template {
	tmpl := template.Must(template.New("layout.html").Funcs(templateFuncs).Parse(layoutHTML))
	return template.Must(tmpl.Parse(pageHTML))
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

func truncate(s string) string {
	if prefix, ok := truncateRunes(s, maxTruncateLen); ok {
		return prefix + "..."
	}
	return s
}

func canTruncate(s string) bool {
	_, ok := truncateRunes(s, maxTruncateLen)
	return ok
}

func isTool(m sessionapi.Message) bool {
	return m.Role == "tool" || m.ToolCallID != ""
}

// truncateRunes returns the prefix of s up to maxRunes, and whether s exceeded maxRunes.
// It is rune-aware and does not split multi-byte UTF-8 sequences.
func truncateRunes(s string, maxRunes int) (string, bool) {
	var count int
	for i := range s {
		if count == maxRunes {
			return s[:i], true
		}
		count++
	}
	return s, false
}
