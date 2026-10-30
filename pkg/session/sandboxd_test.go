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
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/gke-labs/auto-optimizer/pkg/tools"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
)

type fakeProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	mu sync.Mutex

	executeCalls []*processv1.ExecuteRequest
	startCalls   []*processv1.StartRequest
	stdinCalls   []*processv1.WriteStdinRequest
	signalCalls  []*processv1.SendSignalRequest

	executeResp *processv1.ExecuteResponse
	executeErr  error

	startPID        int32
	startStdout     [][]byte
	startStderr     [][]byte
	startExit       int32
	startBlockCh    chan struct{}
	stdinReceivedCh chan struct{}

	capturedMD metadata.MD
}

func (f *fakeProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	f.mu.Lock()
	f.executeCalls = append(f.executeCalls, req)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.capturedMD = md
	}
	resp, err := f.executeResp, f.executeErr
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if resp != nil {
		return resp, nil
	}
	return &processv1.ExecuteResponse{
		ExitCode: 0,
		Stdout:   []byte("default execute stdout\n"),
		Stderr:   nil,
	}, nil
}

func (f *fakeProcessServer) Start(req *processv1.StartRequest, stream processv1.ProcessService_StartServer) error {
	f.mu.Lock()
	f.startCalls = append(f.startCalls, req)
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		f.capturedMD = md
	}
	pid := f.startPID
	if pid == 0 {
		pid = 1234
	}
	stdoutChunks := f.startStdout
	stderrChunks := f.startStderr
	exitCode := f.startExit
	blockCh := f.startBlockCh
	stdinCh := f.stdinReceivedCh
	f.mu.Unlock()

	// 1. Send InitEvent
	if err := stream.Send(&processv1.StartResponse{
		Event: &processv1.StartResponse_Init{
			Init: &processv1.InitEvent{ProcessId: pid},
		},
	}); err != nil {
		return err
	}

	// 2. Stream stdout chunks
	for _, chunk := range stdoutChunks {
		if err := stream.Send(&processv1.StartResponse{
			Event: &processv1.StartResponse_Stdout{Stdout: chunk},
		}); err != nil {
			return err
		}
	}

	// 3. Stream stderr chunks
	for _, chunk := range stderrChunks {
		if err := stream.Send(&processv1.StartResponse{
			Event: &processv1.StartResponse_Stderr{Stderr: chunk},
		}); err != nil {
			return err
		}
	}

	if stdinCh != nil {
		select {
		case <-stdinCh:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	if blockCh != nil {
		select {
		case <-blockCh:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	// 4. Send ExitEvent
	return stream.Send(&processv1.StartResponse{
		Event: &processv1.StartResponse_Exit{
			Exit: &processv1.ExitEvent{ExitCode: exitCode},
		},
	})
}

func (f *fakeProcessServer) WriteStdin(ctx context.Context, req *processv1.WriteStdinRequest) (*processv1.WriteStdinResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stdinCalls = append(f.stdinCalls, req)
	if f.stdinReceivedCh != nil {
		select {
		case <-f.stdinReceivedCh:
		default:
			close(f.stdinReceivedCh)
		}
	}
	return &processv1.WriteStdinResponse{}, nil
}

func (f *fakeProcessServer) SendSignal(ctx context.Context, req *processv1.SendSignalRequest) (*processv1.SendSignalResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signalCalls = append(f.signalCalls, req)
	if f.startBlockCh != nil {
		select {
		case <-f.startBlockCh:
		default:
			close(f.startBlockCh)
		}
	}
	return &processv1.SendSignalResponse{}, nil
}

func setupTestProcessClient(t *testing.T, srv *fakeProcessServer, authToken string) (*Sandbox, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	processv1.RegisterProcessServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if authToken != "" {
		dialOpts = append(dialOpts,
			grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+authToken)
				return invoker(ctx, method, req, reply, cc, callOpts...)
			}),
			grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+authToken)
				return streamer(ctx, desc, cc, method, callOpts...)
			}),
		)
	}

	conn, err := grpc.NewClient("passthrough:///bufnet", dialOpts...)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	sb := &Sandbox{
		authToken:     authToken,
		grpcConn:      conn,
		processClient: processv1.NewProcessServiceClient(conn),
	}

	cleanup := func() {
		_ = sb.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}

	return sb, cleanup
}

func TestExecCommand_Execute_ShortCommand(t *testing.T) {
	srv := &fakeProcessServer{
		executeResp: &processv1.ExecuteResponse{
			ExitCode: 0,
			Stdout:   []byte("Linux test 6.6\n"),
			Stderr:   nil,
		},
	}
	sb, cleanup := setupTestProcessClient(t, srv, "test-secret-token")
	defer cleanup()

	ctx := t.Context()
	opts := tools.ExecCommandOptions{
		Command: []string{"uname", "-a"},
	}

	res, err := sb.ExecCommand(ctx, opts)
	if err != nil {
		t.Fatalf("ExecCommand() error = %v, want nil", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
	if res.Stdout != "Linux test 6.6\n" {
		t.Errorf("Stdout = %q, want %q", res.Stdout, "Linux test 6.6\n")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.executeCalls) != 1 {
		t.Fatalf("Execute calls = %d, want 1", len(srv.executeCalls))
	}
	if diff := cmp.Diff([]string{"uname", "-a"}, srv.executeCalls[0].GetConfig().GetCommand()); diff != "" {
		t.Errorf("Execute command mismatch (-want +got):\n%s", diff)
	}
	// Verify bearer token was sent in authorization metadata header
	authHeader := srv.capturedMD.Get("authorization")
	if len(authHeader) == 0 || authHeader[0] != "Bearer test-secret-token" {
		t.Errorf("authorization header = %v, want [Bearer test-secret-token]", authHeader)
	}
}

func TestExecCommand_Execute_NonZeroExit(t *testing.T) {
	srv := &fakeProcessServer{
		executeResp: &processv1.ExecuteResponse{
			ExitCode: 42,
			Stdout:   []byte("failed job\n"),
			Stderr:   []byte("exit status 42\n"),
		},
	}
	sb, cleanup := setupTestProcessClient(t, srv, "")
	defer cleanup()

	res, err := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
		Command: []string{"sh", "-c", "exit 42"},
	})
	if err != nil {
		t.Fatalf("ExecCommand() returned unexpected error for non-zero exit: %v", err)
	}
	if res.ExitCode != 42 {
		t.Errorf("ExitCode = %d, want 42", res.ExitCode)
	}
	if res.Stdout != "failed job\n" {
		t.Errorf("Stdout = %q, want %q", res.Stdout, "failed job\n")
	}
	if res.Stderr != "exit status 42\n" {
		t.Errorf("Stderr = %q, want %q", res.Stderr, "exit status 42\n")
	}
}

func TestExecCommand_Start_StreamedOutput(t *testing.T) {
	srv := &fakeProcessServer{
		startPID: 4321,
		startStdout: [][]byte{
			[]byte("chunk1-"),
			[]byte("chunk2\n"),
		},
		startStderr: [][]byte{
			[]byte("warn: stderr output\n"),
		},
		startExit: 0,
	}
	sb, cleanup := setupTestProcessClient(t, srv, "token-stream")
	defer cleanup()

	var stdoutBuf, stderrBuf bytes.Buffer
	opts := tools.ExecCommandOptions{
		Command: []string{"bash", "-c", "echo chunk1 && echo chunk2"},
		Stdout:  &stdoutBuf,
		Stderr:  &stderrBuf,
	}

	res, err := sb.ExecCommand(t.Context(), opts)
	if err != nil {
		t.Fatalf("ExecCommand() error = %v, want nil", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
	if stdoutBuf.String() != "chunk1-chunk2\n" {
		t.Errorf("Streamed Stdout = %q, want %q", stdoutBuf.String(), "chunk1-chunk2\n")
	}
	if stderrBuf.String() != "warn: stderr output\n" {
		t.Errorf("Streamed Stderr = %q, want %q", stderrBuf.String(), "warn: stderr output\n")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.startCalls) != 1 {
		t.Fatalf("Start calls = %d, want 1", len(srv.startCalls))
	}
}

func TestExecCommand_Start_WithStdin(t *testing.T) {
	srv := &fakeProcessServer{
		startPID: 5555,
		startStdout: [][]byte{
			[]byte("received stdin\n"),
		},
		startExit:       0,
		stdinReceivedCh: make(chan struct{}),
	}
	sb, cleanup := setupTestProcessClient(t, srv, "")
	defer cleanup()

	stdinData := strings.NewReader("hello from stdin")
	opts := tools.ExecCommandOptions{
		Command: []string{"cat"},
		Stdin:   stdinData,
	}

	res, err := sb.ExecCommand(t.Context(), opts)
	if err != nil {
		t.Fatalf("ExecCommand() error = %v, want nil", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
	if res.Stdout != "received stdin\n" {
		t.Errorf("Stdout = %q, want %q", res.Stdout, "received stdin\n")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.stdinCalls) == 0 {
		t.Errorf("expected WriteStdin calls, got 0")
	}
}

func TestExecCommand_Start_ContextCancellationDeliversSignal(t *testing.T) {
	srv := &fakeProcessServer{
		startPID:     9999,
		startBlockCh: make(chan struct{}),
		startExit:    137, // SIGKILL
	}
	sb, cleanup := setupTestProcessClient(t, srv, "")
	defer cleanup()

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		_, err := sb.ExecCommand(ctx, tools.ExecCommandOptions{
			Command: []string{"sleep", "999"},
			Stream:  true,
		})
		errCh <- err
	}()

	// Wait for process to start and receive InitEvent
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error on context cancellation, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ExecCommand to abort on cancellation")
	}

	// Verify SendSignal was called with process ID 9999 and SIGKILL
	var signalCalls []*processv1.SendSignalRequest
	for range 100 {
		srv.mu.Lock()
		signalCalls = srv.signalCalls
		srv.mu.Unlock()
		if len(signalCalls) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(signalCalls) != 1 {
		t.Fatalf("SendSignal calls = %d, want 1", len(signalCalls))
	}
	if signalCalls[0].GetProcessId() != 9999 {
		t.Errorf("SendSignal ProcessId = %d, want 9999", signalCalls[0].GetProcessId())
	}
	if signalCalls[0].GetSignal() != processv1.Signal_SIGNAL_SIGKILL {
		t.Errorf("SendSignal Signal = %v, want SIGKILL", signalCalls[0].GetSignal())
	}
}
