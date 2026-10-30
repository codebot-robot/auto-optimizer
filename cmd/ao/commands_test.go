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
	"bytes"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"

	sessionv1 "github.com/gke-labs/auto-optimizer/api/session/v1"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/run"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessionapi"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

func TestCommandHierarchy(t *testing.T) {
	root := newRootCommand()

	expectedSubcommands := []string{"serve", "session", "repo", "goal", "run"}
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

func TestRunCommand(t *testing.T) {
	cmd := newRunCommand()
	listCmd, _, err := cmd.Find([]string{"list"})
	if err != nil || listCmd == nil || listCmd.Name() != "list" {
		t.Fatalf("expected list subcommand on run command")
	}

	if listCmd.Flag("repo") == nil {
		t.Error("expected --repo flag on ao run list")
	}
	if listCmd.Flag("server") == nil {
		t.Error("expected --server flag on ao run list")
	}
	if listCmd.Flag("json") == nil {
		t.Error("expected --json flag on ao run list")
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

func TestRunListCommand_Execution(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	stateStore := state.NewFileStore(dir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/example-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "example-repo",
		DefaultBranch: "main",
	}
	if err := stateStore.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		Goal: &goals.Goal{
			Name:     "latency-reduction",
			Body:     "Reduce latency",
			Hash:     "hash-1",
			Schedule: "on-change",
		},
		Hash:       "hash-1",
		BaseCommit: "commit-abc1234",
	}
	if err := stateStore.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	// Run scheduler tick to produce a Pending run
	scheduler, err := run.NewScheduler(run.SchedulerOptions{Store: stateStore})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	// Start test gRPC server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer listener.Close()

	sessionStore := session.NewFileStore(dir)
	grpcServer := grpc.NewServer()
	srv, err := sessionapi.NewServer(sessionapi.ServerOptions{
		Store:          sessionStore,
		StateStore:     stateStore,
		DefaultToolset: "basic",
		HarnessFactory: func(_ string) (*session.Harness, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	sessionv1.RegisterSessionServiceServer(grpcServer, srv)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	// Execute `ao run list --repo <url> --server <addr>`
	cmd := newRunCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"list", "--repo", repo.URL, "--server", listener.Addr().String()})

	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatalf("cmd.ExecuteContext: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "latency-reduction") {
		t.Errorf("expected output to contain goal name 'latency-reduction', got:\n%s", output)
	}
	if !strings.Contains(output, "Pending") {
		t.Errorf("expected output to contain phase 'Pending', got:\n%s", output)
	}
	if !strings.Contains(output, "commit-abc1234") {
		t.Errorf("expected output to contain base commit 'commit-abc1234', got:\n%s", output)
	}
}
