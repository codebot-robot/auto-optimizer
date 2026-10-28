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

func TestRenderSessionListPage_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := RenderSessionListPage(ctx, rec, SessionListPageData{})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
