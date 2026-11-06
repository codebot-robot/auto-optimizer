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
}

// HeroRunStatus defines the observed state and lifecycle progress of a HeroRun.
type HeroRunStatus struct {
	// Phase is the current lifecycle phase of the run, matching the state.RunPhase value.
	// +optional
	Phase string `json:"phase,omitempty"`

	// Conditions track the state transitions and observations of the run.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// SandboxName is the name of the Sandbox resource running this optimization session.
	// +optional
	SandboxName string `json:"sandboxName,omitempty"`

	// Branch is the git branch name created for this run.
	// +optional
	Branch string `json:"branch,omitempty"`

	// PullRequestURL is the forge URL of the pull request opened for this run, if any.
	// +optional
	PullRequestURL string `json:"pullRequestURL,omitempty"`

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
