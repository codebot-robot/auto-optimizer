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

// Package goals finds, parses, and validates goal definitions from Markdown files.
package goals

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"
)

// Kind identifies whether a goal is a metric goal or a judged goal.
type Kind string

const (
	// KindMetric indicates a goal with a metric command that is re-measured.
	KindMetric Kind = "metric"
	// KindJudged indicates a goal evaluated by maintainer judgement without a metric command.
	KindJudged Kind = "judged"
)

// Goal represents a parsed and validated goal definition.
type Goal struct {
	Name     string   `json:"name"`
	Body     string   `json:"body"`
	Hash     string   `json:"hash"`
	Metric   *Metric  `json:"metric,omitempty"`
	Verify   string   `json:"verify,omitempty"`
	Budget   *Budget  `json:"budget,omitempty"`
	Schedule string   `json:"schedule,omitempty"`
	Agents   []string `json:"agents,omitempty"`
}

// Kind returns KindMetric if the goal has a Metric section, or KindJudged otherwise.
func (g *Goal) Kind() Kind {
	if g.Metric != nil {
		return KindMetric
	}
	return KindJudged
}

// Metric defines benchmark measurement parameters for a metric goal.
type Metric struct {
	Command        string `json:"command"`
	Direction      string `json:"direction"`
	Unit           string `json:"unit,omitempty"`
	MinImprovement string `json:"minImprovement,omitempty"`
	Runs           int    `json:"runs,omitempty"`
	Timeout        string `json:"timeout,omitempty"`
}

// Budget defines resource and time limits for a goal run.
type Budget struct {
	MaxDuration string  `json:"maxDuration,omitempty"`
	MaxCostUSD  float64 `json:"maxCostUSD,omitempty"`
}

type rawGoal struct {
	Metric   *rawMetric `json:"metric"`
	Verify   string     `json:"verify"`
	Budget   *rawBudget `json:"budget"`
	Schedule string     `json:"schedule"`
	Agents   []string   `json:"agents"`
}

type rawMetric struct {
	Command        string `json:"command"`
	Direction      string `json:"direction"`
	Unit           string `json:"unit"`
	MinImprovement string `json:"minImprovement"`
	Runs           *int   `json:"runs"`
	Timeout        string `json:"timeout"`
}

type rawBudget struct {
	MaxDuration string   `json:"maxDuration"`
	MaxCostUSD  *float64 `json:"maxCostUSD"`
}

// Parse takes the goal name (the file name without .md) and file contents,
// parses optional YAML front matter, computes SHA-256 content hash, validates all fields,
// and returns the Goal.
func Parse(name string, data []byte) (*Goal, error) {
	if !isSafeGoalName(name) {
		return nil, fmt.Errorf("goals: invalid goal name %q: must be safe in a branch name (lower-case letters, digits, '-', '_', '.')", name)
	}

	h := sha256.Sum256(data)
	hash := hex.EncodeToString(h[:])

	yamlBytes, bodyBytes, hasFrontMatter, err := splitFrontMatter(data)
	if err != nil {
		return nil, err
	}

	g := &Goal{
		Name:     name,
		Body:     string(bodyBytes),
		Hash:     hash,
		Schedule: "on-change",
	}

	if !hasFrontMatter || len(bytes.TrimSpace(yamlBytes)) == 0 {
		return g, nil
	}

	var raw rawGoal
	if err := yaml.UnmarshalStrict(yamlBytes, &raw); err != nil {
		return nil, fmt.Errorf("goals: invalid front matter: %w", err)
	}

	// Schedule validation and defaulting
	if raw.Schedule != "" {
		switch raw.Schedule {
		case "daily", "weekly", "on-change", "manual":
			g.Schedule = raw.Schedule
		default:
			return nil, fmt.Errorf("goals: invalid schedule %q: must be one of daily, weekly, on-change, manual", raw.Schedule)
		}
	}

	g.Verify = raw.Verify
	g.Agents = raw.Agents

	if raw.Budget != nil {
		b := &Budget{
			MaxDuration: raw.Budget.MaxDuration,
		}
		if raw.Budget.MaxCostUSD != nil {
			b.MaxCostUSD = *raw.Budget.MaxCostUSD
		}
		g.Budget = b
	}

	if raw.Metric != nil {
		if strings.TrimSpace(raw.Metric.Command) == "" {
			return nil, fmt.Errorf("goals: metric section without command is an error")
		}

		if raw.Metric.Direction != "lower" && raw.Metric.Direction != "higher" {
			return nil, fmt.Errorf("goals: invalid metric direction %q: must be 'lower' or 'higher'", raw.Metric.Direction)
		}

		runs := 1
		if raw.Metric.Runs != nil {
			if *raw.Metric.Runs < 1 {
				return nil, fmt.Errorf("goals: invalid metric runs %d: must be at least 1", *raw.Metric.Runs)
			}
			runs = *raw.Metric.Runs
		}

		if raw.Metric.MinImprovement != "" {
			if !isValidMinImprovement(raw.Metric.MinImprovement) {
				return nil, fmt.Errorf("goals: invalid minImprovement %q: must be a number or percentage", raw.Metric.MinImprovement)
			}
		}

		g.Metric = &Metric{
			Command:        raw.Metric.Command,
			Direction:      raw.Metric.Direction,
			Unit:           raw.Metric.Unit,
			MinImprovement: raw.Metric.MinImprovement,
			Runs:           runs,
			Timeout:        raw.Metric.Timeout,
		}
	}

	return g, nil
}

// LoadDir reads every *.md file in a directory and returns goals sorted by name.
// Non-Markdown files and directories are ignored.
func LoadDir(fsys fs.FS, dir string) ([]*Goal, error) {
	if dir == "" {
		dir = "."
	}
	dir = path.Clean(dir)
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}

	goals := make([]*Goal, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileName := entry.Name()
		if !strings.HasSuffix(fileName, ".md") {
			continue
		}

		goalName := strings.TrimSuffix(fileName, ".md")
		filePath := path.Join(dir, fileName)
		data, err := fs.ReadFile(fsys, filePath)
		if err != nil {
			return nil, err
		}

		g, err := Parse(goalName, data)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", filePath, err)
		}
		goals = append(goals, g)
	}

	sort.Slice(goals, func(i, j int) bool {
		return goals[i].Name < goals[j].Name
	})

	return goals, nil
}

func isSafeGoalName(name string) bool {
	if name == "" {
		return false
	}
	if name[0] == '-' || name[0] == '.' {
		return false
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, ".lock") {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func isValidMinImprovement(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if strings.HasSuffix(s, "%") {
		numStr := strings.TrimSpace(strings.TrimSuffix(s, "%"))
		if numStr == "" {
			return false
		}
		val, err := strconv.ParseFloat(numStr, 64)
		return err == nil && !math.IsNaN(val) && !math.IsInf(val, 0)
	}
	val, err := strconv.ParseFloat(s, 64)
	return err == nil && !math.IsNaN(val) && !math.IsInf(val, 0)
}

func splitFrontMatter(data []byte) (yamlBytes []byte, bodyBytes []byte, hasFrontMatter bool, err error) {
	content := data
	if bytes.HasPrefix(content, []byte("\xef\xbb\xbf")) {
		content = content[3:]
	}

	if len(content) == 0 {
		return nil, content, false, nil
	}

	firstLine := content
	var rest []byte
	idx := bytes.IndexByte(content, '\n')
	if idx != -1 {
		firstLine = content[:idx]
		rest = content[idx+1:]
	}

	if string(bytes.TrimRight(firstLine, " \t\r")) != "---" {
		return nil, content, false, nil
	}

	// Front matter delimiter found on line 1.
	if rest == nil {
		return nil, nil, true, fmt.Errorf("goals: unclosed front matter")
	}

	lineStart := 0
	for lineStart <= len(rest) {
		if lineStart == len(rest) {
			break
		}
		var line []byte
		var nextLineStart int
		newlineIdx := bytes.IndexByte(rest[lineStart:], '\n')
		if newlineIdx == -1 {
			line = rest[lineStart:]
			nextLineStart = len(rest)
		} else {
			line = rest[lineStart : lineStart+newlineIdx]
			nextLineStart = lineStart + newlineIdx + 1
		}

		if string(bytes.TrimRight(line, " \t\r")) == "---" {
			// Found closing delimiter
			yamlBytes = rest[:lineStart]
			bodyBytes = rest[nextLineStart:]
			return yamlBytes, bodyBytes, true, nil
		}

		lineStart = nextLineStart
	}

	return nil, nil, true, fmt.Errorf("goals: unclosed front matter")
}
