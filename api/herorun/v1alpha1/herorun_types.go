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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HeroRunSpec defines the desired state of a HeroRun.
// Spec is written once when the orchestrator publishes the object and never edited by the agent.
type HeroRunSpec struct {
	// Repository is the forge URL of the repository being optimized, matching state.Repo.URL.
	Repository string `json:"repository"`

	// Goal is the name of the optimization goal.
	Goal string `json:"goal"`

	// RunID is the unique identifier of the run.
	RunID string `json:"runID"`

	// BaseCommit is the commit SHA in the repository from which the run started.
	BaseCommit string `json:"baseCommit"`

	// GoalDefinition holds the frozen goal definition captured for this run.
	// +optional
	GoalDefinition *GoalDefinition `json:"goalDefinition,omitempty"`

	// Deadline is the time by which the run should complete.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
}

// GoalDefinition holds the frozen goal definition captured for this run.
type GoalDefinition struct {
	// Body holds the Markdown description of the goal.
	Body string `json:"body"`

	// Hash is the content hash of the goal file.
	Hash string `json:"hash"`

	// Metric defines benchmark measurement parameters for a metric goal.
	// +optional
	Metric *MetricSpec `json:"metric,omitempty"`

	// Verify is the shell command used to verify goal satisfaction.
	// +optional
	Verify string `json:"verify,omitempty"`

	// Budget defines resource and time limits for a goal run.
	// +optional
	Budget *BudgetSpec `json:"budget,omitempty"`

	// Schedule specifies when the goal should run automatically.
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// Agents lists custom agents permitted to work on this goal.
	// +optional
	Agents []string `json:"agents,omitempty"`
}

// MetricSpec defines benchmark measurement parameters for a metric goal.
type MetricSpec struct {
	// Command is the benchmark command to measure.
	Command string `json:"command"`

	// Direction is "lower" or "higher", indicating whether a smaller or larger measurement is better.
	Direction string `json:"direction"`

	// Unit is the measurement unit string (e.g., "ns/op", "ms", "MB").
	// +optional
	Unit string `json:"unit,omitempty"`

	// MinImprovement is the minimum improvement threshold required to declare victory.
	// +optional
	MinImprovement string `json:"minImprovement,omitempty"`

	// Runs is the number of benchmark repetitions to execute per measurement.
	// +optional
	Runs int `json:"runs,omitempty"`

	// Timeout is the maximum execution duration for one benchmark run.
	// +optional
	Timeout string `json:"timeout,omitempty"`
}

// BudgetSpec defines resource and time limits for a goal run.
type BudgetSpec struct {
	// MaxDuration is the maximum wall-clock duration for the run.
	// +optional
	MaxDuration string `json:"maxDuration,omitempty"`

	// MaxCostUSD is the maximum estimated LLM budget in USD for the run.
	// +optional
	MaxCostUSD float64 `json:"maxCostUSD,omitempty"`
}

// HeroRunStatus defines the observed state and lifecycle progress of a HeroRun.
type HeroRunStatus struct {
	// Phase is the current lifecycle phase of the run, matching the state.RunPhase value.
	// +optional
	Phase string `json:"phase,omitempty"`

	// PhaseTimestamps tracks the timestamps of lifecycle phase transitions.
	// +optional
	PhaseTimestamps map[string]metav1.Time `json:"phaseTimestamps,omitempty"`

	// Conditions track the state transitions and observations of the run.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// SandboxName is the name of the Sandbox resource running this optimization session.
	// +optional
	SandboxName string `json:"sandboxName,omitempty"`

	// WorkspaceClaimName is the name of the persistent volume claim holding the workspace.
	// +optional
	WorkspaceClaimName string `json:"workspaceClaimName,omitempty"`

	// SessionID is the unique identifier of the active optimization session.
	// +optional
	SessionID string `json:"sessionId,omitempty"`

	// Branch is the git branch name created for this run.
	// +optional
	Branch string `json:"branch,omitempty"`

	// CandidateCommit is the commit SHA produced by the run.
	// +optional
	CandidateCommit string `json:"candidateCommit,omitempty"`

	// PRTitle is the title of the pull request for this run.
	// +optional
	PRTitle string `json:"prTitle,omitempty"`

	// PRBody is the markdown description of the pull request for this run.
	// +optional
	PRBody string `json:"prBody,omitempty"`

	// PullRequestURL is the forge URL of the pull request opened for this run, if any.
	// +optional
	PullRequestURL string `json:"pullRequestURL,omitempty"`

	// Turns is the count of agent interaction turns executed in this run.
	// +optional
	Turns int `json:"turns,omitempty"`

	// Baseline holds the baseline metric measurement for metric goals.
	// +optional
	Baseline *float64 `json:"baseline,omitempty"`

	// Candidate holds the candidate metric measurement for metric goals.
	// +optional
	Candidate *float64 `json:"candidate,omitempty"`

	// CostUSD holds the total estimated LLM cost for this run.
	// +optional
	CostUSD float64 `json:"costUSD,omitempty"`

	// Summary is a human-readable summary of the run outcome.
	// +optional
	Summary string `json:"summary,omitempty"`

	// Error holds any fatal error message that caused the run to fail.
	// +optional
	Error string `json:"error,omitempty"`

	// ErrorKind classifies the fatal error (for example "failed" or "portal_bind_failed").
	// +optional
	ErrorKind string `json:"errorKind,omitempty"`

	// ReadyDeadline is the deadline by which the sandbox environment must become ready.
	// Temporary: retained for compatibility with today's reconciler until PhasePreparing is migrated to Run.
	// +optional
	ReadyDeadline *metav1.Time `json:"readyDeadline,omitempty"`

	// StartedAt is the time when the run started.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// FinishedAt is the time when the run reached a terminal phase.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// ObservedAt is the time when the orchestrator last wrote status for this run.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=hr
// +kubebuilder:printcolumn:name="Goal",type=string,JSONPath=`.spec.goal`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Branch",type=string,JSONPath=`.status.branch`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// HeroRun is the Schema for the heroruns API.
type HeroRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   HeroRunSpec   `json:"spec,omitempty"`
	Status HeroRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// HeroRunList contains a list of HeroRun.
type HeroRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []HeroRun `json:"items"`
}
