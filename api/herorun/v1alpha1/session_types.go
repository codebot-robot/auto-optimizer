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

// SessionPhase represents the lifecycle phase of a Session.
type SessionPhase string

const (
	// SessionPhasePending indicates the session was created and has not started yet.
	SessionPhasePending SessionPhase = "Pending"
	// SessionPhaseRunning indicates the session's LLM loop is actively running.
	SessionPhaseRunning SessionPhase = "Running"
	// SessionPhaseWaiting indicates the session is paused waiting for messages or child sessions.
	SessionPhaseWaiting SessionPhase = "Waiting"
	// SessionPhaseSucceeded indicates the session completed without a fatal error.
	SessionPhaseSucceeded SessionPhase = "Succeeded"
	// SessionPhaseFailed indicates the session terminated due to an error or deadline.
	SessionPhaseFailed SessionPhase = "Failed"
	// SessionPhaseCanceled indicates the session was canceled before completing.
	SessionPhaseCanceled SessionPhase = "Canceled"
)

// IsTerminal reports whether the session phase is terminal.
func (p SessionPhase) IsTerminal() bool {
	switch p {
	case SessionPhaseSucceeded, SessionPhaseFailed, SessionPhaseCanceled:
		return true
	default:
		return false
	}
}

// SessionOutcome classifies why a Session reached a terminal phase.
type SessionOutcome string

const (
	// SessionOutcomeCompleted indicates the session ended normally without a termination tool call.
	SessionOutcomeCompleted SessionOutcome = "Completed"
	// SessionOutcomePullRequest indicates the agent requested opening a pull request.
	SessionOutcomePullRequest SessionOutcome = "PullRequest"
	// SessionOutcomeFinished indicates the agent called the finish tool.
	SessionOutcomeFinished SessionOutcome = "Finished"
	// SessionOutcomeMaxTurns indicates the session reached its configured turn limit.
	SessionOutcomeMaxTurns SessionOutcome = "MaxTurns"
	// SessionOutcomeDeadlineExceeded indicates the session reached its deadline before completing.
	SessionOutcomeDeadlineExceeded SessionOutcome = "DeadlineExceeded"
	// SessionOutcomeCanceled indicates the session was canceled before completing.
	SessionOutcomeCanceled SessionOutcome = "Canceled"
	// SessionOutcomeFailed indicates the session failed with an error.
	SessionOutcomeFailed SessionOutcome = "Failed"
)

// SessionSpec defines the immutable desired inputs for running a root or child Session.
// Bulk payloads such as conversation transcripts (messages.jsonl) and unabridged tool
// outputs live on the state volume rather than in Session.
type SessionSpec struct {
	// RunID is the unique identifier of the HeroRun this session belongs to, if any.
	// +optional
	RunID string `json:"runID,omitempty"`
	// HeroRunName is the name of the HeroRun resource in the same namespace, if any.
	// +optional
	HeroRunName string `json:"heroRunName,omitempty"`
	// Repository is the forge URL of the repository this session operates on, if any.
	// +optional
	Repository string `json:"repository,omitempty"`
	// Branch is the expected git branch name for this session's work, if any.
	// +optional
	Branch string `json:"branch,omitempty"`
	// Parent is the name of the parent Session that spawned this child session.
	// Empty for root run sessions and standalone sessions.
	// +optional
	Parent string `json:"parent,omitempty"`
	// Ancestors is the ordered chain of ancestor Session names from the root session down to Parent.
	// +optional
	Ancestors []string `json:"ancestors,omitempty"`
	// Toolset is the name of the toolset (tools and default system prompt) for this session.
	// +optional
	Toolset string `json:"toolset,omitempty"`
	// Model overrides the LLM model name used for this session; empty uses the controller default.
	// +optional
	Model string `json:"model,omitempty"`
	// SystemPrompt is the initial system prompt seeded into a new session; empty uses the toolset default.
	// +optional
	SystemPrompt string `json:"systemPrompt,omitempty"`
	// InitialPrompt is the initial user prompt or task assigned to the session.
	// +optional
	InitialPrompt string `json:"initialPrompt,omitempty"`
	// SandboxName is the name of the Sandbox resource in the same namespace backing tool execution.
	// +optional
	SandboxName string `json:"sandboxName,omitempty"`
	// WorkspaceClaimName is the name of the PVC holding the shared workspace, if any.
	// +optional
	WorkspaceClaimName string `json:"workspaceClaimName,omitempty"`
	// MaxTurns limits the number of LLM turns for this session (0 uses the controller default).
	// +optional
	MaxTurns int `json:"maxTurns,omitempty"`
	// MaxDepth limits how deep child sessions may be nested under this session (0 uses the controller default).
	// +optional
	MaxDepth int `json:"maxDepth,omitempty"`
	// Deadline is the time by which the session must stop.
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`
	// AllowPullRequest reports whether this session may invoke the open_pull_request tool.
	// +optional
	AllowPullRequest bool `json:"allowPullRequest,omitempty"`
}

// Depth returns the session's depth in the session tree.
// Root and standalone sessions have depth 0.
func (s SessionSpec) Depth() int {
	if len(s.Ancestors) > 0 {
		return len(s.Ancestors)
	}
	if s.Parent != "" {
		return 1
	}
	return 0
}

// SessionStatus defines the observed progress and outcome of a Session.
type SessionStatus struct {
	// Phase is the current lifecycle phase of the session.
	// +optional
	Phase SessionPhase `json:"phase,omitempty"`
	// Outcome classifies why the session reached a terminal phase.
	// +optional
	Outcome SessionOutcome `json:"outcome,omitempty"`
	// Conditions track high-level readiness and state transitions of the session.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Turns is the number of LLM turns executed by this session so far.
	// +optional
	Turns int `json:"turns,omitempty"`
	// MessageCount is the number of messages recorded in the session transcript on the state volume.
	// +optional
	MessageCount int `json:"messageCount,omitempty"`
	// CostUSD is the estimated LLM cost accrued by this session in USD.
	// +optional
	CostUSD float64 `json:"costUSD,omitempty"`
	// PRTitle is the pull request title proposed by the agent when Outcome is PullRequest.
	// +optional
	PRTitle string `json:"prTitle,omitempty"`
	// PRBody is the pull request description proposed by the agent when Outcome is PullRequest.
	// +optional
	PRBody string `json:"prBody,omitempty"`
	// Summary is a human-readable summary of the session outcome (for example from the finish tool).
	// +optional
	Summary string `json:"summary,omitempty"`
	// Error holds the error message when the session failed or was canceled.
	// +optional
	Error string `json:"error,omitempty"`
	// WaitingUntil records when a waiting session's wait timeout expires.
	// +optional
	WaitingUntil *metav1.Time `json:"waitingUntil,omitempty"`
	// StartedAt is the time when the session started executing.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// FinishedAt is the time when the session reached a terminal phase.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// ObservedAt is the time when the session controller last wrote status for this session.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sess
// +kubebuilder:printcolumn:name="Run",type=string,JSONPath=`.spec.runID`
// +kubebuilder:printcolumn:name="Parent",type=string,JSONPath=`.spec.parent`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Outcome",type=string,JSONPath=`.status.outcome`
// +kubebuilder:printcolumn:name="Turns",type=integer,JSONPath=`.status.turns`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Session is the Schema for the sessions API.
type Session struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SessionSpec   `json:"spec,omitempty"`
	Status SessionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SessionList contains a list of Session.
type SessionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Session `json:"items"`
}
