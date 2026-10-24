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
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gke-labs/auto-optimizer/cmd/ao/pkg/commands"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

func newServeCommand() *cobra.Command {
	var opts commands.ServeOptions
	opts.InitDefaults()

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the session API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.Serve(cmd.Context(), &opts)
		},
	}

	cmd.Flags().StringVar(&opts.Listen, "listen", opts.Listen, "Address and port to listen on; must not be exposed to untrusted networks")
	cmd.Flags().StringVar(&opts.Namespace, "namespace", opts.Namespace, "Kubernetes namespace where sandbox pods are created")
	cmd.Flags().StringVar(&opts.Toolset, "toolset", opts.Toolset, fmt.Sprintf("Default toolset to expose to the LLM (%s)", strings.Join(toolsets.Names(), ", ")))
	cmd.Flags().StringVar(&opts.Image, "image", opts.Image, "Default sandbox container image (overrides toolset default)")
	cmd.Flags().StringVar(&opts.HomeDir, "homedir", opts.HomeDir, "Home directory inside the sandbox persisted via snapshot/restore")
	cmd.Flags().DurationVar(&opts.ToolTimeout, "tool-timeout", opts.ToolTimeout, "Maximum duration for a single tool execution")
	cmd.Flags().StringVar(&opts.StateDir, "state-dir", opts.StateDir, "Directory where session state and snapshots are saved")

	return cmd
}
