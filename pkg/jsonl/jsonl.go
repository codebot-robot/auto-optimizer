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

// Package jsonl provides helpers for reading JSON Lines files where records
// are separated by newlines, blank lines are allowed, and fragments from
// interrupted writes are skipped.
package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

// Read reads all records of type T from a JSONL file in order.
// Records are separated by newlines, and blank lines are ignored.
// Any line that does not parse as a record or has no trailing newline is
// treated as a fragment from an interrupted write, logged with the file path
// and byte offset, and skipped.
func Read[T any](ctx context.Context, path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			scoped.Log(ctx).V(2).Info("failed to close file", "path", path, "error", closeErr)
		}
	}()

	r := bufio.NewReader(f)
	var records []T
	offset := int64(0)

	for {
		chunk, err := r.ReadBytes('\n')
		chunkLen := int64(len(chunk))
		if chunkLen > 0 {
			lineStart := offset
			offset += chunkLen
			hasNL := chunk[chunkLen-1] == '\n'
			trimmed := bytes.TrimSpace(chunk)

			if len(trimmed) > 0 {
				var item T
				if !hasNL || json.Unmarshal(trimmed, &item) != nil {
					scoped.Log(ctx).Info("ignoring partial record in JSONL file", "path", path, "offset", lineStart)
				} else {
					records = append(records, item)
				}
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
	}

	return records, nil
}
