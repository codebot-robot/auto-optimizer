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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestRunVerify_Passing(t *testing.T) {
	srv := &fakeProcessServer{
		startStdout: [][]byte{
			[]byte("=== RUN   TestSample\n--- PASS: TestSample (0.01s)\nPASS\nok  \tpkg/sample\t0.02s\n"),
		},
		startExit: 0,
	}
	sb, cleanup := setupTestProcessClient(t, srv, nil, "")
	defer cleanup()

	res, err := sb.RunVerify(t.Context(), "/workspace/repo", "go test ./...", 50)
	if err != nil {
		t.Fatalf("RunVerify() error = %v, want nil", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", res.ExitCode)
	}
	wantTail := "=== RUN   TestSample\n--- PASS: TestSample (0.01s)\nPASS\nok  \tpkg/sample\t0.02s\n"
	if res.Tail != wantTail {
		t.Errorf("Tail = %q, want %q", res.Tail, wantTail)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.startCalls) != 1 {
		t.Fatalf("startCalls count = %d, want 1", len(srv.startCalls))
	}
	wantCmd := []string{"sh", "-c", "cd '/workspace/repo' && go test ./..."}
	if diff := cmp.Diff(wantCmd, srv.startCalls[0].Config.Command); diff != "" {
		t.Errorf("command mismatch (-want +got):\n%s", diff)
	}
}

func TestRunVerify_FailingExitCode(t *testing.T) {
	srv := &fakeProcessServer{
		startStderr: [][]byte{
			[]byte("--- FAIL: TestFail (0.01s)\nFAIL\nexit status 1\n"),
		},
		startExit: 1,
	}
	sb, cleanup := setupTestProcessClient(t, srv, nil, "")
	defer cleanup()

	res, err := sb.RunVerify(t.Context(), "/workspace/repo", "go test ./...", 50)
	if err != nil {
		t.Fatalf("RunVerify() error = %v, want nil (non-zero exit is not an error)", err)
	}

	if res.ExitCode != 1 {
		t.Errorf("ExitCode = %d, want 1", res.ExitCode)
	}
	wantTail := "--- FAIL: TestFail (0.01s)\nFAIL\nexit status 1\n"
	if res.Tail != wantTail {
		t.Errorf("Tail = %q, want %q", res.Tail, wantTail)
	}
}

func TestRunVerify_TailLines_KeepsLastNLinesAcrossStdoutAndStderr(t *testing.T) {
	srv := &fakeProcessServer{
		startStdout: [][]byte{
			[]byte("out 1\nout 2\n"),
			[]byte("out 3\n"),
		},
		startStderr: [][]byte{
			[]byte("err 1\nerr 2\n"),
			[]byte("err 3\n"),
		},
		startExit: 0,
	}
	sb, cleanup := setupTestProcessClient(t, srv, nil, "")
	defer cleanup()

	// 6 lines in total across stdout and stderr. With tailLines = 3, only the last 3 lines should remain.
	res, err := sb.RunVerify(t.Context(), "/workspace/repo", "verify.sh", 3)
	if err != nil {
		t.Fatalf("RunVerify() error = %v", err)
	}

	wantTail := "err 1\nerr 2\nerr 3\n"
	if res.Tail != wantTail {
		t.Errorf("Tail = %q, want %q", res.Tail, wantTail)
	}
}

func TestRunVerify_TailLines_Default50WhenZero(t *testing.T) {
	var lines []string
	var chunks [][]byte
	for i := 1; i <= 60; i++ {
		line := fmt.Sprintf("line %d\n", i)
		lines = append(lines, line)
		chunks = append(chunks, []byte(line))
	}

	srv := &fakeProcessServer{
		startStdout: chunks,
		startExit:   0,
	}
	sb, cleanup := setupTestProcessClient(t, srv, nil, "")
	defer cleanup()

	// tailLines = 0 should default to 50
	res, err := sb.RunVerify(t.Context(), "/workspace/repo", "test.sh", 0)
	if err != nil {
		t.Fatalf("RunVerify() error = %v", err)
	}

	wantTail := strings.Join(lines[10:], "")
	if res.Tail != wantTail {
		t.Errorf("Tail mismatch: got %d lines, want 50 lines", len(strings.Split(res.Tail, "\n"))-1)
	}
}

func TestRunVerify_ExecutionFailure(t *testing.T) {
	srv := &fakeProcessServer{
		startErr: errors.New("sandboxd daemon unavailable"),
	}
	sb, cleanup := setupTestProcessClient(t, srv, nil, "")
	defer cleanup()

	res, err := sb.RunVerify(t.Context(), "/workspace/repo", "go test ./...", 50)
	if err == nil {
		t.Fatal("RunVerify() error = nil, want execution failure error")
	}
	if res != nil {
		t.Errorf("RunVerify() result = %v, want nil", res)
	}
}

func TestRunVerify_ShellQuoting(t *testing.T) {
	srv := &fakeProcessServer{
		startExit: 0,
	}
	sb, cleanup := setupTestProcessClient(t, srv, nil, "")
	defer cleanup()

	repoDir := "/workspace/dir with spaces/and 'single quotes'"
	cmd := "go test -v ./... && echo \"done\""
	_, err := sb.RunVerify(t.Context(), repoDir, cmd, 10)
	if err != nil {
		t.Fatalf("RunVerify() error = %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.startCalls) != 1 {
		t.Fatalf("startCalls count = %d, want 1", len(srv.startCalls))
	}
	wantCmd := []string{
		"sh",
		"-c",
		"cd '/workspace/dir with spaces/and '\\''single quotes'\\''' && go test -v ./... && echo \"done\"",
	}
	if diff := cmp.Diff(wantCmd, srv.startCalls[0].Config.Command); diff != "" {
		t.Errorf("command mismatch (-want +got):\n%s", diff)
	}
}

func TestTailWriter_ChunkedAndIncompleteLines(t *testing.T) {
	tw := newTailWriter(3)
	// Write chunked without trailing newline at first
	_, _ = tw.Write([]byte("part1-"))
	_, _ = tw.Write([]byte("part2\n"))
	_, _ = tw.Write([]byte("line2\nline3\nline4\nfinal-no-newline"))

	// Total lines:
	// 1: part1-part2\n
	// 2: line2\n
	// 3: line3\n
	// 4: line4\n
	// 5: final-no-newline
	// With tailLines = 3, should keep lines 3, 4, 5:
	want := "line3\nline4\nfinal-no-newline"
	if got := tw.String(); got != want {
		t.Errorf("tw.String() = %q, want %q", got, want)
	}
}
