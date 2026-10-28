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
	"flag"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
)

var update = flag.Bool("update", false, "update golden files")

func assertGolden(t *testing.T, filename string, actual string) {
	t.Helper()
	goldenPath := filepath.Join("testdata", filename)
	if *update {
		if err := os.MkdirAll("testdata", 0755); err != nil {
			t.Fatalf("failed to create testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(actual), 0644); err != nil {
			t.Fatalf("failed to update golden file %s: %v", goldenPath, err)
		}
		return
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file %s: %v (run with -update to generate)", goldenPath, err)
	}

	actualClean := strings.ReplaceAll(string(actual), "\r\n", "\n")
	expectedClean := strings.ReplaceAll(string(expected), "\r\n", "\n")
	if actualClean != expectedClean {
		t.Errorf("golden mismatch for %s\nwant:\n%s\ngot:\n%s", filename, expectedClean, actualClean)
	}
}

type fakeSessionService struct {
	summaries []sessionapi.SessionSummary
	details   map[string]*sessionapi.SessionDetail
}

func (f *fakeSessionService) ListSessionSummaries(_ context.Context) ([]sessionapi.SessionSummary, error) {
	return f.summaries, nil
}

func (f *fakeSessionService) GetSessionDetail(_ context.Context, name string) (*sessionapi.SessionDetail, error) {
	if d, ok := f.details[name]; ok {
		return d, nil
	}
	return nil, os.ErrNotExist
}

func (f *fakeSessionService) CreateNewSession(_ context.Context, _, _ string) error {
	return nil
}

func (f *fakeSessionService) StartTurn(_ context.Context, _, _ string) error {
	return nil
}

func TestGoldenFiles(t *testing.T) {
	ctx := t.Context()
	fixedTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

	t.Run("list_zero", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := SessionListPageData{Sessions: nil}
		if err := RenderSessionListPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderSessionListPage: %v", err)
		}
		assertGolden(t, "list_zero.golden.html", rec.Body.String())
	})

	t.Run("list_one", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := SessionListPageData{
			Sessions: []sessionapi.SessionSummary{
				{
					Name:         "alpha",
					Toolset:      "basic",
					Parent:       "",
					CreatedAt:    fixedTime,
					MessageCount: 3,
					InFlight:     false,
				},
			},
		}
		if err := RenderSessionListPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderSessionListPage: %v", err)
		}
		assertGolden(t, "list_one.golden.html", rec.Body.String())
	})

	t.Run("list_several", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := SessionListPageData{
			Sessions: []sessionapi.SessionSummary{
				{
					Name:         "gamma",
					Toolset:      "geminicli",
					Parent:       "beta",
					CreatedAt:    fixedTime.Add(2 * time.Hour),
					MessageCount: 5,
					InFlight:     true,
				},
				{
					Name:         "beta",
					Toolset:      "basic",
					Parent:       "",
					CreatedAt:    fixedTime.Add(1 * time.Hour),
					MessageCount: 8,
					InFlight:     false,
				},
				{
					Name:         "alpha",
					Toolset:      "basic",
					Parent:       "",
					CreatedAt:    fixedTime,
					MessageCount: 2,
					InFlight:     false,
				},
			},
		}
		if err := RenderSessionListPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderSessionListPage: %v", err)
		}
		assertGolden(t, "list_several.golden.html", rec.Body.String())
	})

	t.Run("transcript_empty", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := TranscriptPageData{
			Detail: sessionapi.SessionDetail{
				Summary: sessionapi.SessionSummary{
					Name:         "empty-session",
					Toolset:      "basic",
					CreatedAt:    fixedTime,
					MessageCount: 0,
					InFlight:     false,
				},
			},
		}
		if err := RenderTranscriptPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderTranscriptPage: %v", err)
		}
		assertGolden(t, "transcript_empty.golden.html", rec.Body.String())
	})

	t.Run("transcript_tool_call", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := TranscriptPageData{
			Detail: sessionapi.SessionDetail{
				Summary: sessionapi.SessionSummary{
					Name:         "tool-session",
					Toolset:      "basic",
					CreatedAt:    fixedTime,
					MessageCount: 4,
					InFlight:     false,
				},
				Messages: []sessionapi.Message{
					{
						Role:    "user",
						Content: "run: uname -a",
					},
					{
						Role: "assistant",
						ToolCalls: []sessionapi.ToolCall{
							{
								ID:        "call_123",
								Name:      "run_command",
								Arguments: `{"command":"uname -a"}`,
							},
						},
					},
					{
						Role:       "tool",
						Content:    "Linux test-node 6.6.0 #1 SMP\n",
						ToolCallID: "call_123",
					},
					{
						Role:    "assistant",
						Content: "The command returned Linux test-node 6.6.0.",
					},
				},
			},
		}
		if err := RenderTranscriptPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderTranscriptPage: %v", err)
		}
		assertGolden(t, "transcript_tool_call.golden.html", rec.Body.String())
	})

	t.Run("transcript_busy", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := TranscriptPageData{
			Detail: sessionapi.SessionDetail{
				Summary: sessionapi.SessionSummary{
					Name:         "busy-session",
					Toolset:      "basic",
					CreatedAt:    fixedTime,
					MessageCount: 1,
					InFlight:     true,
				},
				Messages: []sessionapi.Message{
					{
						Role:    "user",
						Content: "run: sleep 10",
					},
				},
			},
		}
		if err := RenderTranscriptPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderTranscriptPage: %v", err)
		}
		assertGolden(t, "transcript_busy.golden.html", rec.Body.String())
	})

	t.Run("transcript_idle", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := TranscriptPageData{
			Detail: sessionapi.SessionDetail{
				Summary: sessionapi.SessionSummary{
					Name:         "idle-session",
					Toolset:      "basic",
					CreatedAt:    fixedTime,
					MessageCount: 2,
					InFlight:     false,
				},
				Messages: []sessionapi.Message{
					{
						Role:    "user",
						Content: "hello",
					},
					{
						Role:    "assistant",
						Content: "Hello! How can I help you today?",
					},
				},
			},
		}
		if err := RenderTranscriptPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderTranscriptPage: %v", err)
		}
		assertGolden(t, "transcript_idle.golden.html", rec.Body.String())
	})

	t.Run("not_found", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := NotFoundPageData{
			Message: `Session "unknown" not found`,
		}
		if err := RenderNotFoundPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderNotFoundPage: %v", err)
		}
		assertGolden(t, "not_found.golden.html", rec.Body.String())
	})

	t.Run("conflict", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := ConflictPageData{
			Name: "busy-sess",
		}
		if err := RenderConflictPage(ctx, rec, data); err != nil {
			t.Fatalf("RenderConflictPage: %v", err)
		}
		assertGolden(t, "conflict.golden.html", rec.Body.String())
	})
}
