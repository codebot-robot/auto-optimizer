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

package jsonl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRead_LargeRecord(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "large.jsonl")

	type record struct {
		Data string `json:"data"`
	}

	want := record{
		Data: strings.Repeat("x", 2*1024*1024),
	}

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	data = append(data, '\n')

	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := Read[record](ctx, path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Read returned %d records, want 1", len(got))
	}
	if got[0] != want {
		t.Fatalf("got record with len %d, want %d", len(got[0].Data), len(want.Data))
	}
}
