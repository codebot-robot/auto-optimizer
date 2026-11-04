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
	"fmt"
	"strings"
	"sync"

	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

// VerifyResult contains the exit code and the output tail of a verify command.
type VerifyResult struct {
	ExitCode int
	Tail     string // last TailLines lines of combined stdout and stderr
}

// RunVerify runs a goal's verify command in the sandbox at repoDir and keeps
// the last tailLines lines of combined stdout and stderr.
// When tailLines is zero (or negative), it defaults to 50 lines.
func (s *Sandbox) RunVerify(ctx context.Context, repoDir, command string, tailLines int) (*VerifyResult, error) {
	if s == nil {
		return nil, errors.New("sandbox is nil")
	}

	fullCmd := command
	if repoDir != "" {
		fullCmd = fmt.Sprintf("cd %s && %s", shellQuote(repoDir), command)
	}

	tw := newTailWriter(tailLines)
	opts := tools.ExecCommandOptions{
		Command: []string{"sh", "-c", fullCmd},
		Stdout:  tw,
		Stderr:  tw,
	}

	res, err := s.ExecCommand(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &VerifyResult{
		ExitCode: res.ExitCode,
		Tail:     tw.String(),
	}, nil
}

// shellQuote wraps s in single quotes and escapes any embedded single quotes.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

type tailWriter struct {
	mu        sync.Mutex
	tailLines int
	lines     []string
	pending   bytes.Buffer
}

func newTailWriter(tailLines int) *tailWriter {
	if tailLines <= 0 {
		tailLines = 50
	}
	return &tailWriter{
		tailLines: tailLines,
	}
}

func (w *tailWriter) pushLine(line string) {
	if len(w.lines) < w.tailLines {
		w.lines = append(w.lines, line)
	} else {
		copy(w.lines, w.lines[1:])
		w.lines[len(w.lines)-1] = line
	}
}

func (w *tailWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n := len(p)
	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		if idx == -1 {
			w.pending.Write(p)
			break
		}
		if w.pending.Len() > 0 {
			w.pending.Write(p[:idx+1])
			w.pushLine(w.pending.String())
			w.pending.Reset()
		} else {
			w.pushLine(string(p[:idx+1]))
		}
		p = p[idx+1:]
	}
	return n, nil
}

func (w *tailWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	var result strings.Builder
	if w.pending.Len() > 0 {
		pendingStr := w.pending.String()
		if len(w.lines) < w.tailLines {
			for _, l := range w.lines {
				result.WriteString(l)
			}
			result.WriteString(pendingStr)
		} else {
			for _, l := range w.lines[1:] {
				result.WriteString(l)
			}
			result.WriteString(pendingStr)
		}
	} else {
		for _, l := range w.lines {
			result.WriteString(l)
		}
	}
	return result.String()
}
