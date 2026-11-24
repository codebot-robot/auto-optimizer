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
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
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
	if !strings.Contains(body, "Run again") {
		t.Error("expected terminal run to contain 'Run again' button")
	}
}

func TestRenderRunPage_ActionError(t *testing.T) {
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
		},
		ActionError: "goal already has an active run",
	}

	if err := RenderRunPage(t.Context(), rec, data); err != nil {
		t.Fatalf("RenderRunPage: %v", err)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Cannot Start Run") {
		t.Error("expected body to contain 'Cannot Start Run'")
	}
	if !strings.Contains(body, "goal already has an active run") {
		t.Error("expected body to contain action error message")
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

func TestRunPageData_SessionTree(t *testing.T) {
	t.Run("hierarchy", func(t *testing.T) {
		data := RunPageData{
			Sessions: []sessionapi.SessionSummary{
				{Name: "root", InFlight: true},
				{Name: "child-a", Parent: "root"},
				{Name: "child-b", Parent: "root", InFlight: true},
				{Name: "grandchild", Parent: "child-a"},
			},
		}

		roots := data.SessionTree()
		if len(roots) != 1 || roots[0].Name != "root" {
			t.Fatalf("roots = %v, want [root]", roots)
		}
		if len(roots[0].Children) != 2 || roots[0].Children[0].Name != "child-a" || roots[0].Children[1].Name != "child-b" {
			t.Fatalf("root children = %v, want [child-a, child-b]", roots[0].Children)
		}
		gc := roots[0].Children[0].Children
		if len(gc) != 1 || gc[0].Name != "grandchild" {
			t.Fatalf("child-a children = %v, want [grandchild]", gc)
		}
	})

	t.Run("orphan_and_cycle_safe", func(t *testing.T) {
		data := RunPageData{
			Sessions: []sessionapi.SessionSummary{
				{Name: "orphan", Parent: "missing"},
				{Name: "self", Parent: "self"},
				{Name: "cyc-a", Parent: "cyc-b"},
				{Name: "cyc-b", Parent: "cyc-a"},
				{Name: "orphan", Parent: "missing"}, // duplicate
			},
		}

		roots := data.SessionTree()
		if len(roots) != 3 {
			t.Fatalf("len(roots) = %d, want 3 (orphan, self, cyc-b)", len(roots))
		}
	})
}

func TestRenderRunPage_SessionTree(t *testing.T) {
	rec := httptest.NewRecorder()
	ts := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	data := RunPageData{
		Run: &state.Run{
			ID:       "20261124T100000Z-tree",
			Repo:     state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
			GoalName: "tree-goal",
			Phase:    state.PhaseWorking,
		},
		Sessions: []sessionapi.SessionSummary{
			{Name: "root-s", Toolset: "basic", CreatedAt: ts, MessageCount: 5, InFlight: true},
			{Name: "child-1", Parent: "root-s", Toolset: "basic", CreatedAt: ts.Add(time.Minute), MessageCount: 1, InFlight: false},
			{Name: "grandchild-1", Parent: "child-1", Toolset: "basic", CreatedAt: ts.Add(2 * time.Minute), MessageCount: 2, InFlight: true},
		},
	}

	if err := RenderRunPage(t.Context(), rec, data); err != nil {
		t.Fatalf("RenderRunPage: %v", err)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`/sessions/root-s`,
		`/sessions/child-1`,
		`/sessions/grandchild-1`,
		`<span class="badge badge-in-flight">running</span>`,
		`<span class="badge badge-idle">stopped</span>`,
		`1 message`,
		`5 messages`,
		`<ul class="session-tree session-tree-root">`,
		`<ul class="session-tree">`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("rendered page missing %q\nbody:\n%s", want, body)
		}
	}

	// Verify nested ordering: root-s appears before child-1, which appears before grandchild-1.
	rootIdx := strings.Index(body, "/sessions/root-s")
	childIdx := strings.Index(body, "/sessions/child-1")
	grandIdx := strings.Index(body, "/sessions/grandchild-1")
	if !(rootIdx < childIdx && childIdx < grandIdx) {
		t.Errorf("unexpected session order in HTML: root=%d child=%d grandchild=%d", rootIdx, childIdx, grandIdx)
	}
}
