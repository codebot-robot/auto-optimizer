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
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
)

// ListSessionsOptions specifies options for listing sessions.
type ListSessionsOptions struct {
	Server string
}

// InitDefaults sets default values for ListSessionsOptions.
func (o *ListSessionsOptions) InitDefaults() {
	if o.Server == "" {
		o.Server = DefaultServerAddress()
	}
}

// ListSessions queries the server for all sessions and prints them in tabular form.
func ListSessions(ctx context.Context, opt *ListSessionsOptions) error {
	conn, err := dialGRPC(opt.Server)
	if err != nil {
		return err
	}
	defer closeGRPCConn(ctx, conn)

	client := sessionv1.NewSessionServiceClient(conn)
	resp, err := client.ListSessions(ctx, &sessionv1.ListSessionsRequest{})
	if err != nil {
		return fmt.Errorf("listing sessions: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "NAME\tTOOLSET\tPARENT\tCREATED\tMESSAGES"); err != nil {
		return err
	}
	for _, s := range resp.Sessions {
		created := ""
		if s.CreationTime != nil {
			created = s.CreationTime.AsTime().Format(time.RFC3339)
		}
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n", s.Name, s.Toolset, s.Parent, created, s.MessageCount); err != nil {
			return err
		}
	}
	return w.Flush()
}
