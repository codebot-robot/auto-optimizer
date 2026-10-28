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
)

func TestRenderNotFoundPage(t *testing.T) {
	rec := httptest.NewRecorder()
	data := NotFoundPageData{
		Message: "Session not found",
	}

	err := RenderNotFoundPage(t.Context(), rec, data)
	if err != nil {
		t.Fatalf("RenderNotFoundPage returned unexpected error: %v", err)
	}

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}

	body := rec.Body.String()
	if !strings.Contains(body, "404 Not Found") {
		t.Errorf("expected body to contain %q, got:\n%s", "404 Not Found", body)
	}
	if !strings.Contains(body, "Session not found") {
		t.Errorf("expected body to contain message %q, got:\n%s", "Session not found", body)
	}
}

func TestRenderNotFoundPage_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := RenderNotFoundPage(ctx, rec, NotFoundPageData{Message: "foo"})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
