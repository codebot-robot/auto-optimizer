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

package ao

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/session"
)

func TestOpenPullRequestTool_Schema(t *testing.T) {
	tool := NewOpenPullRequestTool()
	schema := tool.Schema()

	if schema.Function.Name != "open_pull_request" {
		t.Errorf("schema function name = %q, want open_pull_request", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	req, ok := params["required"].([]string)
	if !ok || !slices.Equal(req, []string{"title", "body"}) {
		t.Errorf("required parameters = %v, want ['title', 'body']", req)
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	titleProp, ok := props["title"].(map[string]any)
	if !ok || titleProp["type"] != "string" {
		t.Errorf("title property = %v, want type: string", titleProp)
	}
	bodyProp, ok := props["body"].(map[string]any)
	if !ok || bodyProp["type"] != "string" {
		t.Errorf("body property = %v, want type: string", bodyProp)
	}
}

func TestOpenPullRequestTool_Run(t *testing.T) {
	ctx := t.Context()

	// 1. Run with Title and Body populated
	tool := NewOpenPullRequestTool()
	tool.Title = "Optimize database index queries"
	tool.Body = "Adds compound index on (repo, goal, run_id)."

	msg, err := tool.Run(ctx, nil)
	if !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run err = %v, want ErrPullRequestRequested", err)
	}
	if msg.Content == nil || !strings.Contains(*msg.Content, "recorded") {
		t.Errorf("msg.Content = %v, want non-nil containing 'recorded'", msg.Content)
	}
	if msg.Role != "tool" {
		t.Errorf("msg.Role = %q, want 'tool'", msg.Role)
	}
	if tool.Title != "Optimize database index queries" {
		t.Errorf("tool.Title = %q, want 'Optimize database index queries'", tool.Title)
	}
	if tool.Body != "Adds compound index on (repo, goal, run_id)." {
		t.Errorf("tool.Body = %q, want 'Adds compound index on (repo, goal, run_id).'", tool.Body)
	}

	// 2. Run with empty or whitespace title returns tool error without sentinel
	for _, badTitle := range []string{"", "   "} {
		badTool := NewOpenPullRequestTool()
		badTool.Title = badTitle
		badTool.Body = "Some body"
		if _, err := badTool.Run(ctx, nil); err == nil {
			t.Errorf("Run with title %q returned nil error, want error", badTitle)
		} else if errors.Is(err, session.ErrPullRequestRequested) {
			t.Errorf("Run with title %q returned ErrPullRequestRequested, want ordinary error", badTitle)
		}
	}

	// 3. Unmarshal JSON into tool, keeps Title and Body and returns sentinel
	unmarshalTool := NewOpenPullRequestTool()
	jsonInput := `{"title":"Refactor cache layer","body":"Replaces map with sync.Map."}`
	if err := json.Unmarshal([]byte(jsonInput), unmarshalTool); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if unmarshalTool.Title != "Refactor cache layer" {
		t.Errorf("Title = %q, want 'Refactor cache layer'", unmarshalTool.Title)
	}
	if unmarshalTool.Body != "Replaces map with sync.Map." {
		t.Errorf("Body = %q, want 'Replaces map with sync.Map.'", unmarshalTool.Body)
	}

	jsonMsg, err := unmarshalTool.Run(ctx, nil)
	if !errors.Is(err, session.ErrPullRequestRequested) {
		t.Fatalf("Run err = %v, want ErrPullRequestRequested", err)
	}
	if jsonMsg.Content == nil || !strings.Contains(*jsonMsg.Content, "recorded") {
		t.Errorf("jsonMsg.Content = %v, want non-nil containing 'recorded'", jsonMsg.Content)
	}
	if unmarshalTool.Title != "Refactor cache layer" {
		t.Errorf("unmarshalTool.Title = %q, want 'Refactor cache layer'", unmarshalTool.Title)
	}
	if unmarshalTool.Body != "Replaces map with sync.Map." {
		t.Errorf("unmarshalTool.Body = %q, want 'Replaces map with sync.Map.'", unmarshalTool.Body)
	}
}
