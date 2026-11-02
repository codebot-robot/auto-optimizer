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
	"testing"

	"github.com/gke-labs/auto-optimizer/pkg/session"
)

func TestFinishTool_Schema(t *testing.T) {
	tool := NewFinishTool()
	schema := tool.Schema()

	if schema.Function.Name != "finish" {
		t.Errorf("schema function name = %q, want finish", schema.Function.Name)
	}
	if schema.Function.Description == "" {
		t.Error("schema function description is empty")
	}

	params, ok := schema.Function.Parameters.(map[string]any)
	if !ok {
		t.Fatalf("parameters is %T, want map[string]any", schema.Function.Parameters)
	}
	req, ok := params["required"].([]string)
	if !ok || len(req) != 1 || req[0] != "summary" {
		t.Errorf("required parameters = %v, want ['summary']", req)
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties is %T, want map[string]any", params["properties"])
	}
	summaryProp, ok := props["summary"].(map[string]any)
	if !ok || summaryProp["type"] != "string" {
		t.Errorf("summary property = %v, want type: string", summaryProp)
	}
}

func TestFinishTool_Run(t *testing.T) {
	ctx := t.Context()

	// 1. Run with Summary populated
	tool := NewFinishTool()
	tool.Summary = "Implemented cache optimization"

	msg, err := tool.Run(ctx, nil)
	if !errors.Is(err, session.ErrSessionFinished) {
		t.Fatalf("Run err = %v, want ErrSessionFinished", err)
	}
	if msg.Content == nil || *msg.Content != "Implemented cache optimization" {
		t.Errorf("msg.Content = %v, want 'Implemented cache optimization'", msg.Content)
	}
	if msg.Role != "tool" {
		t.Errorf("msg.Role = %q, want 'tool'", msg.Role)
	}

	// 2. Run with empty Summary defaults content
	emptyTool := NewFinishTool()
	emptyMsg, err := emptyTool.Run(ctx, nil)
	if !errors.Is(err, session.ErrSessionFinished) {
		t.Fatalf("Run err = %v, want ErrSessionFinished", err)
	}
	if emptyMsg.Content == nil || *emptyMsg.Content != "Session finished." {
		t.Errorf("emptyMsg.Content = %v, want 'Session finished.'", emptyMsg.Content)
	}

	// 3. Unmarshal JSON into tool
	unmarshalTool := NewFinishTool()
	if err := json.Unmarshal([]byte(`{"summary":"Completed benchmark refactoring"}`), unmarshalTool); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if unmarshalTool.Summary != "Completed benchmark refactoring" {
		t.Errorf("Summary = %q, want 'Completed benchmark refactoring'", unmarshalTool.Summary)
	}

	jsonMsg, err := unmarshalTool.Run(ctx, nil)
	if !errors.Is(err, session.ErrSessionFinished) {
		t.Fatalf("Run err = %v, want ErrSessionFinished", err)
	}
	if jsonMsg.Content == nil || *jsonMsg.Content != "Completed benchmark refactoring" {
		t.Errorf("jsonMsg.Content = %v, want 'Completed benchmark refactoring'", jsonMsg.Content)
	}
}
