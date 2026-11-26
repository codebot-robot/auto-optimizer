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

func TestRenderSessionListPage(t *testing.T) {
	rec := httptest.NewRecorder()
	data := SessionListPageData{
		Sessions: []sessionapi.SessionSummary{
			{
				Name:         "test-session",
				Toolset:      "basic",
				CreatedAt:    time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
				MessageCount: 3,
				InFlight:     false,
			},
		},
	}

	err := RenderSessionListPage(t.Context(), rec, data)
	if err != nil {
		t.Fatalf("RenderSessionListPage returned unexpected error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}

	body := rec.Body.String()
	if !strings.Contains(body, "test-session") {
		t.Errorf("expected body to contain session name %q, got:\n%s", "test-session", body)
	}
	if !strings.Contains(body, "2026-10-28 12:00:00 UTC") {
		t.Errorf("expected body to contain formatted time, got:\n%s", body)
	}
}

func TestRenderSessionListPage_RunAndStandalone(t *testing.T) {
	rec := httptest.NewRecorder()
	runID := "20261126T231827Z-0e08"
	ts := time.Date(2026, 11, 26, 23, 18, 27, 0, time.UTC)
	data := SessionListPageData{
		Sessions: []sessionapi.SessionSummary{
			{Name: "run20261126t231827z0e08", RunID: runID, Toolset: "basic", CreatedAt: ts},
			{Name: "orphanrun", RunID: "20261126T000000Z-9999", Toolset: "basic", CreatedAt: ts},
			{Name: "interactive", Toolset: "basic", CreatedAt: ts},
		},
		Runs: map[string]*state.Run{
			runID: {
				ID:       runID,
				Repo:     state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
				GoalName: "fix-broken-links",
			},
		},
	}

	if err := RenderSessionListPage(t.Context(), rec, data); err != nil {
		t.Fatalf("RenderSessionListPage: %v", err)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`<a href="/sessions/run20261126t231827z0e08"><strong>github.com/gke-labs/auto-optimizer/fix-broken-links/20261126T231827Z-0e08</strong></a><div class="session-meta">run20261126t231827z0e08</div>`,
		`<a href="/sessions/orphanrun"><strong>orphanrun</strong></a></td>`,
		`<a href="/sessions/interactive"><strong>interactive</strong></a></td>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected body to contain %q, got:\n%s", want, body)
		}
	}
}

func TestRenderSessionListPage_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := RenderSessionListPage(ctx, rec, SessionListPageData{})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
