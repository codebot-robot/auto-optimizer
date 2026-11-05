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

package session

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
)

type cmdResult struct {
	exitCode int32
	stdout   string
	stderr   string
}

type scriptedProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	mu sync.Mutex

	commands      map[string]cmdResult
	startStdout   []byte
	startExitCode int32
	stdinCh       chan struct{}
	capturedStdin []byte
	lastCall      string
	calls         []string
}

func (s *scriptedProcessServer) Execute(_ context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.Join(req.GetConfig().GetCommand(), " ")
	s.lastCall = key
	s.calls = append(s.calls, key)
	res, ok := s.commands[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unhandled command: %s", key)
	}
	return &processv1.ExecuteResponse{ExitCode: res.exitCode, Stdout: []byte(res.stdout), Stderr: []byte(res.stderr)}, nil
}

func (s *scriptedProcessServer) Start(req *processv1.StartRequest, stream processv1.ProcessService_StartServer) error {
	s.mu.Lock()
	s.lastCall = strings.Join(req.GetConfig().GetCommand(), " ")
	ch, stdout, code := s.stdinCh, s.startStdout, s.startExitCode
	s.mu.Unlock()

	if err := stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 1}}}); err != nil {
		return err
	}
	if ch != nil {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
		}
	}
	if len(stdout) > 0 {
		if err := stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: stdout}}); err != nil {
			return err
		}
	}
	return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: code}}})
}

func (s *scriptedProcessServer) WriteStdin(_ context.Context, req *processv1.WriteStdinRequest) (*processv1.WriteStdinResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capturedStdin = append(s.capturedStdin, req.GetInput()...)
	if s.stdinCh != nil {
		select {
		case <-s.stdinCh:
		default:
			close(s.stdinCh)
		}
	}
	return &processv1.WriteStdinResponse{}, nil
}

func setupScriptedSandbox(t *testing.T, srv *scriptedProcessServer) (*Sandbox, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	processv1.RegisterProcessServiceServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	sb := &Sandbox{grpcConn: conn, processClient: processv1.NewProcessServiceClient(conn)}
	return sb, func() { _ = sb.Close(); grpcServer.Stop(); _ = lis.Close() }
}

func TestExportCommits(t *testing.T) {
	const (
		base = "1234567890abcdef1234567890abcdef12345678"
		tip  = "abcdef1234567890abcdef1234567890abcdef12"
	)

	tests := []struct {
		name        string
		opts        ExportOptions
		commands    map[string]cmdResult
		startStdout []byte
		startExit   int32
		wantErr     error
		errSub      string
		wantTip     string
		wantPack    []byte
		checkStdin  string
	}{
		{
			name: "success without pending changes",
			opts: ExportOptions{Base: base},
			commands: map[string]cmdResult{
				"git -C /workspace/repo status --porcelain":                    {},
				"git -C /workspace/repo diff --quiet " + base + " HEAD":        {exitCode: 1},
				"git -C /workspace/repo diff --name-only " + base + " HEAD":    {stdout: "pkg/session/export.go\n"},
				"git -C /workspace/repo rev-parse HEAD":                        {stdout: tip + "\n"},
				"git -C /workspace/repo rev-list --objects " + base + "..HEAD": {stdout: "obj1\nobj2\n"},
				"git -C /workspace/repo pack-objects --stdout --no-thin -q":    {},
			},
			startStdout: []byte("PACK-STREAM"),
			wantTip:     tip,
			wantPack:    []byte("PACK-STREAM"),
			checkStdin:  "obj1\nobj2\n",
		},
		{
			name: "pending changes return ErrUncommittedChanges",
			opts: ExportOptions{RepoDir: "/custom", Base: base},
			commands: map[string]cmdResult{
				"git -C /custom status --porcelain": {stdout: " M file.go\n"},
			},
			wantErr: ErrUncommittedChanges,
		},
		{
			name: "empty diff returns ErrNoChanges",
			opts: ExportOptions{Base: base},
			commands: map[string]cmdResult{
				"git -C /workspace/repo status --porcelain":             {},
				"git -C /workspace/repo diff --quiet " + base + " HEAD": {exitCode: 0},
			},
			wantErr: ErrNoChanges,
		},
		{
			name: "goals changes return ErrGoalsChanged",
			opts: ExportOptions{Base: base},
			commands: map[string]cmdResult{
				"git -C /workspace/repo status --porcelain":                 {},
				"git -C /workspace/repo diff --quiet " + base + " HEAD":     {exitCode: 1},
				"git -C /workspace/repo diff --name-only " + base + " HEAD": {stdout: "main.go\n.goals/goal.md\n"},
			},
			wantErr: ErrGoalsChanged,
		},
		{
			name:   "invalid base SHA rejected up front",
			opts:   ExportOptions{Base: "invalid;injection"},
			errSub: "invalid base commit SHA",
		},
		{
			name: "pack-objects failure cleans up temp file",
			opts: ExportOptions{Base: base},
			commands: map[string]cmdResult{
				"git -C /workspace/repo status --porcelain":                    {},
				"git -C /workspace/repo diff --quiet " + base + " HEAD":        {exitCode: 1},
				"git -C /workspace/repo diff --name-only " + base + " HEAD":    {stdout: "file.txt\n"},
				"git -C /workspace/repo rev-parse HEAD":                        {stdout: tip + "\n"},
				"git -C /workspace/repo rev-list --objects " + base + "..HEAD": {stdout: "obj\n"},
				"git -C /workspace/repo pack-objects --stdout --no-thin -q":    {},
			},
			startExit: 128,
			errSub:    "packfile generation failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &scriptedProcessServer{
				commands:      tc.commands,
				startStdout:   tc.startStdout,
				startExitCode: tc.startExit,
			}
			if tc.checkStdin != "" {
				srv.stdinCh = make(chan struct{})
			}
			sb, cleanup := setupScriptedSandbox(t, srv)
			defer cleanup()

			destDir := t.TempDir()
			opts := tc.opts
			if opts.DestDir == "" {
				opts.DestDir = destDir
			}

			res, err := sb.ExportCommits(t.Context(), opts)
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.errSub != "" && (err == nil || !strings.Contains(err.Error(), tc.errSub)) {
				t.Fatalf("err = %v, want substring %q", err, tc.errSub)
			}
			if tc.wantErr == nil && tc.errSub == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if res.Tip != tc.wantTip {
					t.Errorf("res.Tip = %q, want %q", res.Tip, tc.wantTip)
				}
				data, err := os.ReadFile(res.PackfilePath)
				if err != nil {
					t.Fatalf("failed to read packfile: %v", err)
				}
				if !bytes.Equal(data, tc.wantPack) {
					t.Errorf("packfile data = %q, want %q", data, tc.wantPack)
				}
				if _, err := os.Stat(res.PackfilePath + ".tmp"); !os.IsNotExist(err) {
					t.Errorf("temp packfile still exists on success")
				}
			}
			if tc.errSub != "" {
				finalPath := filepath.Join(opts.DestDir, "pack-"+tip+".pack")
				if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
					t.Errorf("final packfile exists despite error")
				}
				if _, err := os.Stat(finalPath + ".tmp"); !os.IsNotExist(err) {
					t.Errorf("temp packfile exists despite error")
				}
			}
			if tc.checkStdin != "" && string(srv.capturedStdin) != tc.checkStdin {
				t.Errorf("stdin = %q, want %q", srv.capturedStdin, tc.checkStdin)
			}
		})
	}
}
