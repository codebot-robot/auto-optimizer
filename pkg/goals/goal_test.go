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

package goals_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/google/go-cmp/cmp"
)

func TestParse_DesignExample(t *testing.T) {
	path := filepath.Join("testdata", "sort-benchmark.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	got, err := goals.Parse("sort-benchmark", data)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	h := sha256.Sum256(data)
	wantHash := hex.EncodeToString(h[:])

	wantBody := `# Make the sort benchmark faster

Do not change the public API of ` + "`pkg/sort`" + `. The hot path is ` + "`quick.go`" + `.
Measure with ` + "`./hack/bench.sh`" + `.
`

	want := &goals.Goal{
		Name:     "sort-benchmark",
		Body:     wantBody,
		Hash:     wantHash,
		Schedule: "daily",
		Verify:   "go test ./...",
		Agents:   []string{"gemini", "claude"},
		Budget: &goals.Budget{
			MaxDuration: "2h",
			MaxCostUSD:  20,
		},
		Metric: &goals.Metric{
			Command:        "./hack/bench.sh",
			Direction:      "lower",
			Unit:           "ns/op",
			MinImprovement: "1%",
			Runs:           3,
			Timeout:        "10m",
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Goal mismatch (-want +got):\n%s", diff)
	}

	if got.Kind() != goals.KindMetric {
		t.Errorf("Kind() = %q, want %q", got.Kind(), goals.KindMetric)
	}
}

func TestParse_BodyOnly(t *testing.T) {
	path := filepath.Join("testdata", "body-only.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	got, err := goals.Parse("body-only", data)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	h := sha256.Sum256(data)
	wantHash := hex.EncodeToString(h[:])

	want := &goals.Goal{
		Name:     "body-only",
		Body:     string(data),
		Hash:     wantHash,
		Schedule: "on-change",
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Goal mismatch (-want +got):\n%s", diff)
	}

	if got.Kind() != goals.KindJudged {
		t.Errorf("Kind() = %q, want %q", got.Kind(), goals.KindJudged)
	}
}

func TestParse_Judged(t *testing.T) {
	path := filepath.Join("testdata", "judged.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	got, err := goals.Parse("judged", data)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	h := sha256.Sum256(data)
	wantHash := hex.EncodeToString(h[:])

	want := &goals.Goal{
		Name: "judged",
		Body: `# Judged Goal With Front Matter

This goal has front matter but no metric section.
`,
		Hash:     wantHash,
		Schedule: "weekly",
		Verify:   "go test ./...",
		Agents:   []string{"gemini"},
		Budget: &goals.Budget{
			MaxDuration: "1h",
			MaxCostUSD:  5,
		},
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Goal mismatch (-want +got):\n%s", diff)
	}

	if got.Kind() != goals.KindJudged {
		t.Errorf("Kind() = %q, want %q", got.Kind(), goals.KindJudged)
	}
}

func TestParse_Defaults(t *testing.T) {
	path := filepath.Join("testdata", "defaults.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	got, err := goals.Parse("defaults", data)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if got.Schedule != "on-change" {
		t.Errorf("Schedule = %q, want %q", got.Schedule, "on-change")
	}
	if got.Metric == nil {
		t.Fatal("expected Metric to be non-nil")
	}
	if got.Metric.Runs != 1 {
		t.Errorf("Metric.Runs = %d, want 1", got.Metric.Runs)
	}
	if got.Metric.Direction != "higher" {
		t.Errorf("Metric.Direction = %q, want 'higher'", got.Metric.Direction)
	}
	if got.Kind() != goals.KindMetric {
		t.Errorf("Kind() = %q, want %q", got.Kind(), goals.KindMetric)
	}
}

func TestParse_ValidationErrors(t *testing.T) {
	tests := []struct {
		file      string
		errSubstr string
	}{
		{
			file:      "invalid-unknown-field.md",
			errSubstr: "unknown field",
		},
		{
			file:      "invalid-unknown-metric-field.md",
			errSubstr: "unknown field",
		},
		{
			file:      "invalid-unknown-budget-field.md",
			errSubstr: "unknown field",
		},
		{
			file:      "invalid-direction.md",
			errSubstr: "direction",
		},
		{
			file:      "invalid-min-improvement.md",
			errSubstr: "minImprovement",
		},
		{
			file:      "invalid-runs-zero.md",
			errSubstr: "runs",
		},
		{
			file:      "invalid-runs-negative.md",
			errSubstr: "runs",
		},
		{
			file:      "invalid-schedule.md",
			errSubstr: "schedule",
		},
		{
			file:      "invalid-metric-no-command.md",
			errSubstr: "command",
		},
		{
			file:      "invalid-unclosed-front-matter.md",
			errSubstr: "unclosed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join("testdata", tc.file)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile failed: %v", err)
			}

			name := strings.TrimSuffix(tc.file, ".md")
			_, err = goals.Parse(name, data)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.errSubstr)) {
				t.Errorf("error %q does not contain expected substring %q", err.Error(), tc.errSubstr)
			}
		})
	}
}

func TestParse_InvalidGoalNames(t *testing.T) {
	dummyData := []byte("# Just a body\n")

	invalidNames := []string{
		"",
		"UPPERCASE",
		"Sort-Benchmark",
		"has space",
		"has/slash",
		"has:colon",
		"has@at",
		"-leading-dash",
		".leading-dot",
		"trailing-dot.",
		"name.lock",
		"contains..dotdot",
	}

	for _, name := range invalidNames {
		t.Run(name, func(t *testing.T) {
			_, err := goals.Parse(name, dummyData)
			if err == nil {
				t.Fatalf("Parse(%q) expected error for invalid goal name, got nil", name)
			}
			if !strings.Contains(err.Error(), "invalid goal name") {
				t.Errorf("error %q does not contain 'invalid goal name'", err.Error())
			}
		})
	}
}

func TestParse_ValidGoalNames(t *testing.T) {
	dummyData := []byte("# Just a body\n")

	validNames := []string{
		"sort-benchmark",
		"quick_sort",
		"v1.2",
		"bench-1_2.3",
		"a",
		"0",
	}

	for _, name := range validNames {
		t.Run(name, func(t *testing.T) {
			g, err := goals.Parse(name, dummyData)
			if err != nil {
				t.Fatalf("Parse(%q) returned unexpected error: %v", name, err)
			}
			if g.Name != name {
				t.Errorf("g.Name = %q, want %q", g.Name, name)
			}
		})
	}
}

func TestParse_MinImprovementFormats(t *testing.T) {
	tests := []struct {
		name           string
		minImprovement string
		wantErr        bool
	}{
		{name: "relative-percent", minImprovement: "1%", wantErr: false},
		{name: "relative-decimal-percent", minImprovement: "0.5%", wantErr: false},
		{name: "absolute-integer", minImprovement: "5", wantErr: false},
		{name: "absolute-decimal", minImprovement: "12.34", wantErr: false},
		{name: "empty-omitted", minImprovement: "", wantErr: false},
		{name: "word-invalid", minImprovement: "fast", wantErr: true},
		{name: "unit-invalid", minImprovement: "10px", wantErr: true},
		{name: "bare-percent-invalid", minImprovement: "%", wantErr: true},
		{name: "double-percent-invalid", minImprovement: "1%%", wantErr: true},
		{name: "nan-invalid", minImprovement: "NaN", wantErr: true},
		{name: "inf-invalid", minImprovement: "Inf", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yamlContent := `---
metric:
  command: ./test.sh
  direction: lower
`
			if tc.minImprovement != "" {
				yamlContent += "  minImprovement: " + tc.minImprovement + "\n"
			}
			yamlContent += "---\n# Body\n"

			_, err := goals.Parse("bench", []byte(yamlContent))
			if tc.wantErr && err == nil {
				t.Errorf("minImprovement %q: expected error, got nil", tc.minImprovement)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("minImprovement %q: unexpected error: %v", tc.minImprovement, err)
			}
		})
	}
}

func TestLoadDir(t *testing.T) {
	fsys := fstest.MapFS{
		".goals/sort-benchmark.md": &fstest.MapFile{
			Data: []byte(`---
metric:
  command: ./bench.sh
  direction: lower
---
# Sort Benchmark
`),
		},
		".goals/body-only.md": &fstest.MapFile{
			Data: []byte("# Body only goal\n"),
		},
		".goals/judged.md": &fstest.MapFile{
			Data: []byte(`---
verify: go test ./...
---
# Judged Goal
`),
		},
		".goals/README.txt": &fstest.MapFile{
			Data: []byte("Ignored non-markdown file\n"),
		},
		".goals/.gitkeep": &fstest.MapFile{
			Data: []byte(""),
		},
		".goals/sub/nested.md": &fstest.MapFile{
			Data: []byte("# Nested inside directory\n"),
		},
	}

	goalsList, err := goals.LoadDir(fsys, ".goals")
	if err != nil {
		t.Fatalf("LoadDir failed: %v", err)
	}

	if len(goalsList) != 3 {
		t.Fatalf("got %d goals, want 3", len(goalsList))
	}

	// Verify goals are sorted by name
	wantNames := []string{"body-only", "judged", "sort-benchmark"}
	for i, wantName := range wantNames {
		if goalsList[i].Name != wantName {
			t.Errorf("goal[%d].Name = %q, want %q", i, goalsList[i].Name, wantName)
		}
	}
}

func TestLoadDir_InvalidFileFails(t *testing.T) {
	fsys := fstest.MapFS{
		".goals/valid.md": &fstest.MapFile{
			Data: []byte("# Valid\n"),
		},
		".goals/invalid.md": &fstest.MapFile{
			Data: []byte(`---
metric:
  command: ./test.sh
  direction: invalid-direction
---
`),
		},
	}

	_, err := goals.LoadDir(fsys, ".goals")
	if err == nil {
		t.Fatal("expected LoadDir to fail on invalid goal file, got nil")
	}
	if !strings.Contains(err.Error(), "invalid-direction") {
		t.Errorf("expected error to mention invalid-direction, got: %v", err)
	}
}

func TestLoadDir_NonexistentDir(t *testing.T) {
	fsys := fstest.MapFS{}
	_, err := goals.LoadDir(fsys, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent directory, got nil")
	}
}
