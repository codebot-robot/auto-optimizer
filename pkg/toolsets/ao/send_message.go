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
	"errors"
	"fmt"
	"strings"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

var _ tools.Tool = &SendMessageTool{}

// MessageSender is the callback invoked to send a message to another session.
type MessageSender func(ctx context.Context, recipient, message string) error

// SendMessageTool is a control-plane tool that sends a text message to another
// session in the same run.
type SendMessageTool struct {
	sender MessageSender

	Recipient string `json:"recipient"`
	Message   string `json:"message"`
}

// NewSendMessageTool returns a new SendMessageTool instance.
func NewSendMessageTool(sender MessageSender) *SendMessageTool {
	return &SendMessageTool{sender: sender}
}

// Schema implements tools.Tool.
func (t *SendMessageTool) Schema() llm.Tool {
	return llm.Tool{
		Type: "function",
		Function: llm.ToolFunction{
			Name: "send_message",
			Description: "Call this tool to send a text message to another session in the same run by name. " +
				"Sessions share the workspace, so a message can name a file instead of carrying its content.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"recipient": map[string]any{
						"type":        "string",
						"description": "Name of the recipient session.",
					},
					"message": map[string]any{
						"type":        "string",
						"description": "Text message to send to the recipient session. Sessions share the workspace, so a message can name a file instead of carrying its content.",
					},
				},
				"required": []string{"recipient", "message"},
			},
		},
	}
}

// Run implements tools.Tool.
func (t *SendMessageTool) Run(ctx context.Context, _ tools.Sandbox) (llm.Message, error) {
	if t.sender == nil {
		return llm.Message{}, errors.New("cannot call send_message: session belongs to no run")
	}
	recipient := strings.TrimSpace(t.Recipient)
	if recipient == "" {
		return llm.Message{}, errors.New("recipient is required")
	}
	msg := strings.TrimSpace(t.Message)
	if msg == "" {
		return llm.Message{}, errors.New("message is required")
	}

	if err := t.sender(ctx, recipient, msg); err != nil {
		return llm.Message{}, err
	}

	content := fmt.Sprintf("Message sent to %s.", recipient)
	return llm.Message{
		Role:    "tool",
		Content: &content,
	}, nil
}
