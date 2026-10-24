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

package main

import (
	"github.com/spf13/cobra"

	"github.com/gke-labs/auto-optimizer/cmd/ao/pkg/commands"
)

func newSessionListCommand() *cobra.Command {
	var opts commands.ListSessionsOptions
	opts.InitDefaults()

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all agent sessions on the server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.ListSessions(cmd.Context(), &opts)
		},
	}

	cmd.Flags().StringVar(&opts.Server, "server", opts.Server, "gRPC server address")
	return cmd
}

func newSessionGetCommand() *cobra.Command {
	var opts commands.GetSessionOptions
	opts.InitDefaults()

	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get and print messages in an agent session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Name = args[0]
			return commands.GetSession(cmd.Context(), &opts)
		},
	}

	cmd.Flags().StringVar(&opts.Server, "server", opts.Server, "gRPC server address")
	return cmd
}

func newSessionSendCommand() *cobra.Command {
	var opts commands.SendSessionOptions
	opts.InitDefaults()

	cmd := &cobra.Command{
		Use:   "send <name> <text>",
		Short: "Send a message to an agent session and stream the turn",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Name = args[0]
			opts.Text = args[1]
			return commands.SendSession(cmd.Context(), &opts)
		},
	}

	cmd.Flags().StringVar(&opts.Server, "server", opts.Server, "gRPC server address")
	return cmd
}
