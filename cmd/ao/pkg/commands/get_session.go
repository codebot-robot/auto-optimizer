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

package commands

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
)

// GetSessionOptions specifies options for retrieving and printing a session's messages.
type GetSessionOptions struct {
	Server string
	Name   string
}

// InitDefaults sets default values for GetSessionOptions.
func (o *GetSessionOptions) InitDefaults() {
	if o.Server == "" {
		o.Server = DefaultServerAddress()
	}
}

// GetSession retrieves the message history of a session and prints them to stdout.
func GetSession(ctx context.Context, opt *GetSessionOptions) error {
	if opt.Name == "" {
		return errors.New("session name is required")
	}

	conn, err := dialGRPC(opt.Server)
	if err != nil {
		return err
	}
	defer closeGRPCConn(ctx, conn)

	client := sessionv1.NewSessionServiceClient(conn)
	resp, err := client.ListMessages(ctx, &sessionv1.ListMessagesRequest{Name: opt.Name})
	if err != nil {
		return fmt.Errorf("getting session messages: %w", err)
	}

	for _, msg := range resp.Messages {
		content := msg.Content
		if content == "" && len(msg.ToolCalls) > 0 {
			var calls []string
			for _, tc := range msg.ToolCalls {
				calls = append(calls, fmt.Sprintf("%s(%s)", tc.Function.Name, tc.Function.Arguments))
			}
			content = fmt.Sprintf("call: %s", strings.Join(calls, ", "))
		}
		fmt.Printf("%s: %s\n", msg.Role, strings.TrimSpace(content))
	}

	return nil
}
