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

// Package ao provides control-plane tools for auto-optimizer sessions.
package ao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var _ tools.Tool = &ListSessionsTool{}

// SessionInfo describes a session of a run.
type SessionInfo struct {
	Name   string `json:"name"`
	Parent string `json:"parent,omitempty"`
	State  string `json:"state"`
}

// SessionLister is the callback invoked to list sessions of the calling run.
type SessionLister func(ctx context.Context) ([]SessionInfo, error)

// ListSessionsTool is a control-plane tool that lists the sessions of the
// calling run.
type ListSessionsTool struct {
	lister SessionLister
}

// NewListSessionsTool returns a new ListSessionsTool instance.
func NewListSessionsTool(lister SessionLister) *ListSessionsTool {
	return &ListSessionsTool{
		lister: lister,
	}
}

// Schema implements tools.Tool.
func (t *ListSessionsTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "list_sessions",
			Description: "Call this tool to list the sessions of the calling run. " +
				"Returns each session's name, its parent, and whether it is running or stopped.",
			Parameters: map[string]any{
				"type": "object",
			},
		},
	}
}

// Run implements tools.Tool.
func (t *ListSessionsTool) Run(ctx context.Context, _ tools.Sandbox) (llm.Message, error) {
	if t.lister == nil {
		return llm.Message{}, errors.New("cannot call list_sessions: session belongs to no run")
	}

	sessions, err := t.lister(ctx)
	if err != nil {
		return llm.Message{}, err
	}
	if sessions == nil {
		sessions = []SessionInfo{}
	}

	data, err := json.MarshalIndent(sessions, "", "  ")
	if err != nil {
		return llm.Message{}, fmt.Errorf("marshalling sessions: %w", err)
	}

	content := string(data)
	return llm.Message{
		Role:    "tool",
		Content: &content,
	}, nil
}
