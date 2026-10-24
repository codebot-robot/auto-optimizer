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
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/klog/v2"

	"github.com/gke-labs/auto-optimizer/cmd/ao/pkg/commands"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets"
)

func main() {
	rootCmd := newRootCommand()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "ao: %v\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "ao",
		Short:        "auto-optimizer control plane and CLI",
		SilenceUsage: true,
	}

	klogFlagSet := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(klogFlagSet)
	rootCmd.PersistentFlags().AddGoFlagSet(klogFlagSet)

	rootCmd.AddCommand(newServeCommand())
	rootCmd.AddCommand(newSessionCommand())
	return rootCmd
}

func newSessionCommand() *cobra.Command {
	sessionCmd := &cobra.Command{
		Use:   "session",
		Short: "Manage and run agent sessions",
	}

	sessionCmd.AddCommand(newSessionRunCommand())
	sessionCmd.AddCommand(newSessionListCommand())
	sessionCmd.AddCommand(newSessionGetCommand())
	sessionCmd.AddCommand(newSessionSendCommand())
	return sessionCmd
}

func newSessionRunCommand() *cobra.Command {
	var opts commands.RunSessionOptions
	opts.InitDefaults()

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run an agent session in a sandboxed environment",
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.RunSession(cmd.Context(), &opts)
		},
	}

	cmd.Flags().StringVar(&opts.SessionName, "session", opts.SessionName, "Session name")
	cmd.Flags().StringVar(&opts.Namespace, "namespace", opts.Namespace, "Kubernetes namespace where sandbox pods are created")
	cmd.Flags().StringVar(&opts.Toolset, "toolset", opts.Toolset, fmt.Sprintf("Toolset to expose to the LLM (%s)", strings.Join(toolsets.Names(), ", ")))
	cmd.Flags().StringVar(&opts.Image, "image", opts.Image, "Sandbox container image (overrides toolset default)")
	cmd.Flags().StringVar(&opts.HomeDir, "homedir", opts.HomeDir, "Home directory inside the sandbox persisted via snapshot/restore")
	cmd.Flags().DurationVar(&opts.ToolTimeout, "tool-timeout", opts.ToolTimeout, "Maximum duration for a single tool execution")
	cmd.Flags().StringVar(&opts.StateDir, "state-dir", opts.StateDir, "Directory where session state and snapshots are saved")
	cmd.Flags().StringVar(&opts.Prompt, "prompt", "", "Non-interactive single-turn prompt")
	cmd.Flags().BoolVar(&opts.Yolo, "yolo", false, "Auto-approve tool calls without prompting")

	return cmd
}
