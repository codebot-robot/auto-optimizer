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

func TestRenderRunPage(t *testing.T) {
	rec := httptest.NewRecorder()
	baseline := 120.5
	candidate := 95.2

	data := RunPageData{
		Run: &state.Run{
			ID: "20261028T120000Z-abcd",
			Repo: state.RepoKey{
				Host:  "github.com",
				Owner: "gke-labs",
				Name:  "auto-optimizer",
			},
			GoalName:   "optimize-throughput",
			Phase:      state.PhaseWorking,
			BaseCommit: "commit-base-sha-1234",
			Branch:     "ao/optimize-throughput/run-1",
			PRURL:      "https://github.com/gke-labs/auto-optimizer/pull/42",
			SessionID:  "session-opt-42",
			Baseline:   &baseline,
			Candidate:  &candidate,
			CostUSD:    0.0452,
			Goal: &goals.Goal{
				Name:     "optimize-throughput",
				Schedule: "on-change",
				Body:     "# Optimize throughput\nEnsure throughput exceeds 1000 req/s.\n",
			},
		},
		Events: []state.Event{
			{
				Time:    time.Date(2026, 10, 28, 12, 0, 1, 0, time.UTC),
				Phase:   state.PhasePreparing,
				Kind:    "clone",
				Message: "Cloning repository at base commit",
			},
			{
				Time:    time.Date(2026, 10, 28, 12, 1, 0, 0, time.UTC),
				Phase:   state.PhaseWorking,
				Kind:    "session_start",
				Message: "Started agent session",
			},
		},
	}

	err := RenderRunPage(t.Context(), rec, data)
	if err != nil {
		t.Fatalf("RenderRunPage returned unexpected error: %v", err)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"Run: 20261028T120000Z-abcd",
		"github.com/gke-labs/auto-optimizer",
		"optimize-throughput",
		"Working",
		"active",
		"<meta http-equiv=\"refresh\" content=\"2\">",
		"commit-base-sha-1234",
		"ao/optimize-throughput/run-1",
		"https://github.com/gke-labs/auto-optimizer/pull/42",
		"/sessions/session-opt-42",
		"Cloning repository at base commit",
		"Started agent session",
		"Ensure throughput exceeds 1000 req/s.",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("expected body to contain %q, got:\n%s", expected, body)
		}
	}
}

func TestRenderRunPage_FailedRun(t *testing.T) {
	rec := httptest.NewRecorder()
	data := RunPageData{
		Run: &state.Run{
			ID: "20261028T130000Z-fail",
			Repo: state.RepoKey{
				Host:  "github.com",
				Owner: "gke-labs",
				Name:  "auto-optimizer",
			},
			GoalName: "failing-goal",
			Phase:    state.PhaseFailed,
			Error:    "failed to clone repository: connection refused",
		},
	}

	err := RenderRunPage(t.Context(), rec, data)
	if err != nil {
		t.Fatalf("RenderRunPage returned unexpected error: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Run Failed") {
		t.Error("expected body to contain 'Run Failed'")
	}
	if !strings.Contains(body, "failed to clone repository: connection refused") {
		t.Error("expected body to contain error message")
	}
	if strings.Contains(body, "<meta http-equiv=\"refresh\" content=\"2\">") {
		t.Error("expected terminal run NOT to have refresh meta tag")
	}
}

func TestRenderRunPage_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := RenderRunPage(ctx, rec, RunPageData{})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
