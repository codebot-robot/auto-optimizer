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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestRenderTranscriptPage(t *testing.T) {
	rec := httptest.NewRecorder()
	data := TranscriptPageData{
		Detail: sessionapi.SessionDetail{
			Summary: sessionapi.SessionSummary{
				Name:         "transcript-session",
				Toolset:      "basic",
				CreatedAt:    time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
				MessageCount: 1,
				InFlight:     false,
			},
			Messages: []sessionapi.Message{
				{
					Role:    "user",
					Content: "hello world",
				},
			},
		},
		Full: false,
	}

	err := RenderTranscriptPage(t.Context(), rec, data)
	if err != nil {
		t.Fatalf("RenderTranscriptPage returned unexpected error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Session: transcript-session") {
		t.Errorf("expected body to contain session name %q, got:\n%s", "Session: transcript-session", body)
	}
	if !strings.Contains(body, "hello world") {
		t.Errorf("expected body to contain message content %q, got:\n%s", "hello world", body)
	}
}

func TestRenderTranscriptPage_WithRun(t *testing.T) {
	rec := httptest.NewRecorder()
	data := TranscriptPageData{
		Detail: sessionapi.SessionDetail{
			Summary: sessionapi.SessionSummary{
				Name:      "run20261126t231827z0e08",
				RunID:     "20261126T231827Z-0e08",
				Toolset:   "basic",
				CreatedAt: time.Date(2026, 11, 26, 23, 18, 27, 0, time.UTC),
			},
		},
		Run: &state.Run{
			ID:       "20261126T231827Z-0e08",
			Repo:     state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
			GoalName: "fix-broken-links",
		},
	}

	if err := RenderTranscriptPage(t.Context(), rec, data); err != nil {
		t.Fatalf("RenderTranscriptPage: %v", err)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"<title>github.com/gke-labs/auto-optimizer/fix-broken-links/20261126T231827Z-0e08 · auto-optimizer</title>",
		`<a href="/repos/github.com/gke-labs/auto-optimizer">github.com/gke-labs/auto-optimizer</a>/fix-broken-links/<a href="/runs/20261126T231827Z-0e08">20261126T231827Z-0e08</a>`,
		`<span class="meta-badge">session: run20261126t231827z0e08</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected body to contain %q, got:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Session: run20261126t231827z0e08") {
		t.Errorf("expected run session header not to use raw session prefix, got:\n%s", body)
	}
}

func TestRenderTranscriptPage_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := RenderTranscriptPage(ctx, rec, TranscriptPageData{})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
