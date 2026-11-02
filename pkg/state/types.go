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
	"fmt"
	"path/filepath"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/goals"
)

// RepoKey identifies a repository by its host, owner, and repository name.
type RepoKey struct {
	Host  string `json:"host"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

// String returns a human-readable identifier for the repository, like host/owner/name.
func (k RepoKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.Host, k.Owner, k.Name)
}

// Dir returns the relative path components of the repository key.
func (k RepoKey) Dir() string {
	return filepath.Join(k.Host, k.Owner, k.Name)
}

// Repo represents a registered repository record in repo.json.
type Repo struct {
	URL            string    `json:"url"`
	Host           string    `json:"host"`
	Owner          string    `json:"owner"`
	Name           string    `json:"name"`
	DefaultBranch  string    `json:"defaultBranch"`
	HeadCommit     string    `json:"headCommit,omitempty"`
	ForgeKind      string    `json:"forgeKind"`
	RegisteredTime time.Time `json:"registeredTime"`
	LastChangeTime time.Time `json:"lastChangeTime,omitempty"`
}

// Key returns the RepoKey corresponding to this repository.
func (r *Repo) Key() RepoKey {
	return RepoKey{
		Host:  r.Host,
		Owner: r.Owner,
		Name:  r.Name,
	}
}

// GoalRecord represents a discovered goal and its history in goal.json.
type GoalRecord struct {
	// Goal holds the parsed goal definition from .goals/<name>.md.
	Goal *goals.Goal `json:"goal"`

	// Hash is the content hash of the goal file.
	Hash string `json:"hash"`

	// BaseCommit is the commit SHA at which the goal was read.
	BaseCommit string `json:"baseCommit"`

	// BestValue is the best measurement value observed so far for metric goals.
	BestValue *float64 `json:"bestValue,omitempty"`

	// LastRunID is the identifier of the most recent run for this goal.
	LastRunID string `json:"lastRunID,omitempty"`
}

// RunPhase represents the lifecycle phase of a run.
type RunPhase string

const (
	PhasePending       RunPhase = "Pending"
	PhasePreparing     RunPhase = "Preparing"
	PhaseWorking       RunPhase = "Working"
	PhaseSelecting     RunPhase = "Selecting"
	PhaseOpeningPR     RunPhase = "OpeningPR"
	PhaseSucceeded     RunPhase = "Succeeded"
	PhaseNoImprovement RunPhase = "NoImprovement"
	PhaseFailed        RunPhase = "Failed"
	PhaseCancelled     RunPhase = "Cancelled"
	PhaseExpired       RunPhase = "Expired"
)

// IsTerminal returns true if the phase is one of the terminal run phases.
func (p RunPhase) IsTerminal() bool {
	switch p {
	case PhaseSucceeded, PhaseNoImprovement, PhaseFailed, PhaseCancelled, PhaseExpired:
		return true
	default:
		return false
	}
}

// Run represents a run record in run.json.
type Run struct {
	ID              string                 `json:"id"`
	Repo            RepoKey                `json:"repo"`
	GoalName        string                 `json:"goalName"`
	Phase           RunPhase               `json:"phase"`
	PhaseTimestamps map[RunPhase]time.Time `json:"phaseTimestamps,omitempty"`
	BaseCommit      string                 `json:"baseCommit,omitempty"`
	Branch          string                 `json:"branch,omitempty"`
	PRURL           string                 `json:"prURL,omitempty"`
	SessionID       string                 `json:"sessionId,omitempty"`
	SandboxName     string                 `json:"sandboxName,omitempty"`
	Summary         string                 `json:"summary,omitempty"`
	Error           string                 `json:"error,omitempty"`

	// Goal holds the frozen goal definition captured for this run.
	Goal *goals.Goal `json:"goal,omitempty"`

	// Baseline holds the baseline metric measurement for metric goals.
	Baseline *float64 `json:"baseline,omitempty"`

	// Candidate holds the candidate metric measurement for metric goals.
	Candidate *float64 `json:"candidate,omitempty"`

	// CostUSD holds the total estimated LLM cost for this run.
	CostUSD float64 `json:"costUSD,omitempty"`
}

// SetPhase transitions the run to a new phase and records the timestamp.
func (r *Run) SetPhase(phase RunPhase, t time.Time) {
	r.Phase = phase
	if r.PhaseTimestamps == nil {
		r.PhaseTimestamps = make(map[RunPhase]time.Time)
	}
	r.PhaseTimestamps[phase] = t
}

// Event represents an event entry appended to events.jsonl.
type Event struct {
	Time    time.Time `json:"time"`
	Phase   RunPhase  `json:"phase,omitempty"`
	Kind    string    `json:"kind,omitempty"`
	Message string    `json:"message,omitempty"`
	Error   string    `json:"error,omitempty"`
	Data    any       `json:"data,omitempty"`
}
