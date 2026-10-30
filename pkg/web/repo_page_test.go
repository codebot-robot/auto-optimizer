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

	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestRenderRepoPage(t *testing.T) {
	rec := httptest.NewRecorder()
	repo := &state.Repo{
		URL:            "https://github.com/gke-labs/auto-optimizer",
		Host:           "github.com",
		Owner:          "gke-labs",
		Name:           "auto-optimizer",
		DefaultBranch:  "main",
		HeadCommit:     "abcdef1234567890",
		ForgeKind:      "github",
		RegisteredTime: time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC),
		LastChangeTime: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
	}

	data := RepoPageData{
		Repo: repo,
		Goals: []GoalEntry{
			{
				Record: &state.GoalRecord{
					Goal: &goals.Goal{
						Name: "latency",
					},
					Hash:       "hash1234567890",
					BaseCommit: "commit1234567890",
					LastRunID:  "20261028T120000Z-abcd",
				},
				LatestPhase: state.PhaseWorking,
			},
		},
		RecentRuns: []*state.Run{
			{
				ID:         "20261028T120000Z-abcd",
				Repo:       repo.Key(),
				GoalName:   "latency",
				Phase:      state.PhaseWorking,
				BaseCommit: "commit1234567890",
				Branch:     "ao/latency/run-1",
				PRURL:      "https://github.com/gke-labs/auto-optimizer/pull/1",
				SessionID:  "sess-latency-1",
			},
		},
	}

	err := RenderRepoPage(t.Context(), rec, data)
	if err != nil {
		t.Fatalf("RenderRepoPage returned unexpected error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"github.com/gke-labs/auto-optimizer",
		"latency",
		"Working",
		"20261028T120000Z-abcd",
		"ao/latency/run-1",
		"sess-latency-1",
		"https://github.com/gke-labs/auto-optimizer/pull/1",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("expected body to contain %q, got:\n%s", expected, body)
		}
	}
}

func TestRenderRepoPage_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := RenderRepoPage(ctx, rec, RepoPageData{})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
