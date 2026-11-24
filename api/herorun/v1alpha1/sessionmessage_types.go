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

const (
	// LabelSession records the target Session name so controllers can list a
	// session's resources such as SessionMessage objects.
	LabelSession = "auto-optimizer.labs.gke.io/session"

	// LabelRun records the HeroRun ID associated with a resource.
	LabelRun = "auto-optimizer.labs.gke.io/run"
)

// SessionMessageSpec defines the immutable payload of a message sent to a Session.
// Callers write Spec once when creating the resource, and the Session controller never mutates it.
type SessionMessageSpec struct {
	// SessionName is the name of the target Session resource in the same namespace.
	SessionName string `json:"sessionName"`

	// RunID is the unique identifier of the HeroRun the target session belongs to, if any.
	// +optional
	RunID string `json:"runID,omitempty"`

	// From identifies the message sender, such as another Session name or a user.
	// Empty when the sender is an anonymous user.
	// +optional
	From string `json:"from,omitempty"`

	// Text is the message content to deliver to the target session.
	Text string `json:"text"`
}

// Labels returns the standard Kubernetes labels for a SessionMessage with this spec.
func (s SessionMessageSpec) Labels() map[string]string {
	labels := make(map[string]string, 2)
	if s.SessionName != "" {
		labels[LabelSession] = s.SessionName
	}
	if s.RunID != "" {
		labels[LabelRun] = s.RunID
	}
	return labels
}

// SessionMessageStatus records the observed delivery state of a SessionMessage.
type SessionMessageStatus struct {
	// DeliveredAt is the time when the Session controller appended this message
	// to the target session transcript.
	// +optional
	DeliveredAt *metav1.Time `json:"deliveredAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=smsg
// +kubebuilder:printcolumn:name="Session",type=string,JSONPath=`.spec.sessionName`
// +kubebuilder:printcolumn:name="Run",type=string,JSONPath=`.spec.runID`
// +kubebuilder:printcolumn:name="From",type=string,JSONPath=`.spec.from`
// +kubebuilder:printcolumn:name="Delivered",type=date,JSONPath=`.status.deliveredAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SessionMessage is the Schema for the sessionmessages API.
// Each SessionMessage represents an immutable message addressed to a Session.
// Callers label each message with Spec.Labels() and set a controller owner reference
// pointing to the target Session so the Session controller wakes via Owns(&SessionMessage{})
// and Kubernetes garbage collection deletes messages with their session.
type SessionMessage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SessionMessageSpec   `json:"spec,omitempty"`
	Status SessionMessageStatus `json:"status,omitempty"`
}

// IsDelivered reports whether the Session controller has recorded delivery of this message.
func (m *SessionMessage) IsDelivered() bool {
	return m != nil && m.Status.DeliveredAt != nil
}

// +kubebuilder:object:root=true

// SessionMessageList contains a list of SessionMessage.
type SessionMessageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SessionMessage `json:"items"`
}
