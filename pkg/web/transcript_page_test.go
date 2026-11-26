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

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
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
	if !strings.Contains(body, `<div class="message-content">hello world</div>`) {
		t.Errorf("expected .message-content with no surrounding template whitespace, got:\n%s", body)
	}
}

func TestRenderTranscriptPage_MessageContentNoTemplateWhitespace(t *testing.T) {
	long := strings.Repeat("a", maxTruncateLen+10)
	truncated := strings.Repeat("a", maxTruncateLen) + "..."

	cases := []struct {
		name    string
		content string
		full    bool
		want    string
	}{
		{"short", "line one\nline two", false, `<div class="message-content">line one` + "\n" + `line two</div>`},
		{"truncated", long, false, `<div class="message-content">` + truncated + `<div><a href="/sessions/s?full=1" class="show-all-link">show all</a></div></div>`},
		{"full", long, true, `<div class="message-content">` + long + `<div><a href="/sessions/s" class="show-all-link">show less</a></div></div>`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			data := TranscriptPageData{
				Detail: sessionapi.SessionDetail{
					Summary:  sessionapi.SessionSummary{Name: "s"},
					Messages: []sessionapi.Message{{Role: "assistant", Content: tc.content}},
				},
				Full: tc.full,
			}
			if err := RenderTranscriptPage(t.Context(), rec, data); err != nil {
				t.Fatalf("RenderTranscriptPage: %v", err)
			}
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Errorf("body missing expected .message-content %q\ngot:\n%s", tc.want, rec.Body.String())
			}
		})
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

func TestRenderTranscriptPage_TerminalAndWaitingStates(t *testing.T) {
	tests := []struct {
		name      string
		phase     herorunv1alpha1.SessionPhase
		outcome   herorunv1alpha1.SessionOutcome
		wantBadge string
		terminal  bool
	}{
		{"finished", herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomeFinished, `<span class="badge badge-succeeded">finished</span>`, true},
		{"pull_request", herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomePullRequest, `<span class="badge badge-succeeded">succeeded</span>`, true},
		{"max_turns", herorunv1alpha1.SessionPhaseSucceeded, herorunv1alpha1.SessionOutcomeMaxTurns, `<span class="badge badge-max-turns">max turns</span>`, true},
		{"failed", herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeFailed, `<span class="badge badge-failed">failed</span>`, true},
		{"deadline", herorunv1alpha1.SessionPhaseFailed, herorunv1alpha1.SessionOutcomeDeadlineExceeded, `<span class="badge badge-failed">deadline exceeded</span>`, true},
		{"cancelled", herorunv1alpha1.SessionPhaseCanceled, herorunv1alpha1.SessionOutcomeCanceled, `<span class="badge badge-cancelled">cancelled</span>`, true},
		{"waiting", herorunv1alpha1.SessionPhaseWaiting, "", `<span class="badge badge-in-flight">waiting</span>`, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			data := TranscriptPageData{
				Detail: sessionapi.SessionDetail{
					Summary: sessionapi.SessionSummary{Name: "s-" + tc.name, Toolset: "basic", Phase: tc.phase, Outcome: tc.outcome},
				},
			}
			if err := RenderTranscriptPage(t.Context(), rec, data); err != nil {
				t.Fatalf("RenderTranscriptPage: %v", err)
			}
			body := rec.Body.String()
			if !strings.Contains(body, tc.wantBadge) {
				t.Errorf("missing badge %q in:\n%s", tc.wantBadge, body)
			}
			if strings.Contains(body, "badge-idle") {
				t.Errorf("unexpected idle badge in:\n%s", body)
			}
			hasForm := strings.Contains(body, `class="message-form"`)
			if tc.terminal && hasForm {
				t.Errorf("expected Send Message form to be hidden for terminal session")
			}
			if tc.terminal && !strings.Contains(body, "This session has ended") {
				t.Errorf("expected ended note for terminal session")
			}
			if !tc.terminal && !hasForm {
				t.Errorf("expected Send Message form to remain visible for non-terminal session")
			}
			hasRefresh := strings.Contains(body, `http-equiv="refresh"`)
			if tc.terminal && hasRefresh {
				t.Errorf("unexpected auto-refresh for terminal session")
			}
			if !tc.terminal && !hasRefresh {
				t.Errorf("expected auto-refresh for waiting session")
			}
		})
	}
}
