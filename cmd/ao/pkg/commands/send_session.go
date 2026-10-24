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
	"io"
	"os"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
)

// SendSessionOptions specifies options for sending a message to a session.
type SendSessionOptions struct {
	Server string
	Name   string
	Text   string
}

// InitDefaults sets default values for SendSessionOptions.
func (o *SendSessionOptions) InitDefaults() {
	if o.Server == "" {
		o.Server = DefaultServerAddress()
	}
}

// SendSession sends a message to an agent session and streams turn events.
func SendSession(ctx context.Context, opt *SendSessionOptions) error {
	if opt.Name == "" {
		return errors.New("session name is required")
	}
	if opt.Text == "" {
		return errors.New("message text is required")
	}

	conn, err := dialGRPC(opt.Server)
	if err != nil {
		return err
	}
	defer closeGRPCConn(ctx, conn)

	client := sessionv1.NewSessionServiceClient(conn)
	stream, err := client.SendMessage(ctx, &sessionv1.SendMessageRequest{
		Name: opt.Name,
		Text: opt.Text,
	})
	if err != nil {
		return fmt.Errorf("sending message: %w", err)
	}

	var hadError bool
	var errorMsg string
	var finalAssistantText string

	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving event: %w", err)
		}

		switch e := event.Event.(type) {
		case *sessionv1.SessionEvent_MessageAppended:
			// Message appended to session history.
		case *sessionv1.SessionEvent_ToolCallStarted:
			tc := e.ToolCallStarted.ToolCall
			fmt.Fprintf(os.Stderr, "==> Tool call: %s %s\n", tc.Function.Name, tc.Function.Arguments)
		case *sessionv1.SessionEvent_ToolCallFinished:
			tc := e.ToolCallFinished.ToolCall
			if e.ToolCallFinished.Error != "" {
				fmt.Fprintf(os.Stderr, "==> Tool failed: %s: %s\n", tc.Function.Name, e.ToolCallFinished.Error)
			} else {
				fmt.Fprintf(os.Stderr, "==> Tool finished: %s\n", tc.Function.Name)
			}
		case *sessionv1.SessionEvent_TurnFinished:
			finalAssistantText = e.TurnFinished.Text
		case *sessionv1.SessionEvent_Error:
			hadError = true
			errorMsg = e.Error.Message
			fmt.Fprintf(os.Stderr, "Error: %s\n", errorMsg)
		}
	}

	if finalAssistantText != "" {
		fmt.Println(finalAssistantText)
	}

	if hadError {
		return fmt.Errorf("turn failed: %s", errorMsg)
	}

	return nil
}
