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

package state

import (
	"testing"
)

func TestParseRepoKey_RoundTrip(t *testing.T) {
	tests := []struct {
		input string
		want  RepoKey
	}{
		{
			input: "github.com/owner/name",
			want:  RepoKey{Host: "github.com", Owner: "owner", Name: "name"},
		},
		{
			input: "local/_/name",
			want:  RepoKey{Host: "local", Owner: "_", Name: "name"},
		},
		{
			input: "local/_/hello-repo",
			want:  RepoKey{Host: "local", Owner: "_", Name: "hello-repo"},
		},
		{
			input: "gitlab.example.com/team/project",
			want:  RepoKey{Host: "gitlab.example.com", Owner: "team", Name: "project"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseRepoKey(tt.input)
			if err != nil {
				t.Fatalf("ParseRepoKey(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseRepoKey(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
			if gotStr := got.String(); gotStr != tt.input {
				t.Errorf("Round-trip mismatch: got.String() = %q, want %q", gotStr, tt.input)
			}
		})
	}
}

func TestParseRepoKey_Errors(t *testing.T) {
	tests := []string{
		"",
		"repo",
		"owner/name",
		"host/owner/name/extra",
		"/owner/name",
		"host//name",
		"host/owner/",
		"host/owner/..",
		"host/../name",
		"../owner/name",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if _, err := ParseRepoKey(input); err == nil {
				t.Errorf("ParseRepoKey(%q) expected error, got nil", input)
			}
		})
	}
}
