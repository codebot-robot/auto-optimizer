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
	"testing"
)

func TestCommandHierarchy(t *testing.T) {
	root := newRootCommand()

	expectedSubcommands := []string{"serve", "session", "repo", "goal"}
	for _, name := range expectedSubcommands {
		cmd, _, err := root.Find([]string{name})
		if err != nil || cmd == nil || cmd.Name() != name {
			t.Errorf("expected subcommand %q on root command", name)
		}
	}
}

func TestRepoCommand(t *testing.T) {
	cmd := newRepoCommand()
	listCmd, _, err := cmd.Find([]string{"list"})
	if err != nil || listCmd == nil || listCmd.Name() != "list" {
		t.Fatalf("expected list subcommand on repo command")
	}

	if listCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao repo list")
	}
	if listCmd.Flag("json") == nil {
		t.Error("expected --json flag on ao repo list")
	}
}

func TestGoalCommand(t *testing.T) {
	cmd := newGoalCommand()
	listCmd, _, err := cmd.Find([]string{"list"})
	if err != nil || listCmd == nil || listCmd.Name() != "list" {
		t.Fatalf("expected list subcommand on goal command")
	}

	if listCmd.Flag("repo") == nil {
		t.Error("expected --repo flag on ao goal list")
	}
	if listCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao goal list")
	}
}

func TestServeFlags(t *testing.T) {
	cmd := newServeCommand()
	if cmd.Flag("repo") == nil {
		t.Error("expected --repo flag on ao serve")
	}
}

func TestSelectForge(t *testing.T) {
	tests := []struct {
		repoURL     string
		githubToken string
		wantForge   string
	}{
		{
			repoURL:     "https://github.com/gke-labs/auto-optimizer",
			githubToken: "ghp_secret",
			wantForge:   "github",
		},
		{
			repoURL:     "https://github.com/gke-labs/auto-optimizer",
			githubToken: "",
			wantForge:   "gitremote",
		},
		{
			repoURL:     "https://gitlab.com/example/repo",
			githubToken: "ghp_secret",
			wantForge:   "gitremote",
		},
		{
			repoURL:     "/path/to/bare/repo.git",
			githubToken: "ghp_secret",
			wantForge:   "gitremote",
		},
	}

	for _, tt := range tests {
		_, name := selectForge(tt.repoURL, tt.githubToken)
		if name != tt.wantForge {
			t.Errorf("selectForge(%q, token=%q) = %q; want %q", tt.repoURL, tt.githubToken, name, tt.wantForge)
		}
	}
}
