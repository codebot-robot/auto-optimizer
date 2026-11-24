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

package v1alpha1_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
)

func TestSessionMessage_SchemeRegistration(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := herorunv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	tests := []struct {
		name string
		obj  runtime.Object
		kind string
	}{
		{name: "SessionMessage", obj: &herorunv1alpha1.SessionMessage{}, kind: "SessionMessage"},
		{name: "SessionMessageList", obj: &herorunv1alpha1.SessionMessageList{}, kind: "SessionMessageList"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gvks, _, err := scheme.ObjectKinds(tc.obj)
			if err != nil {
				t.Fatalf("ObjectKinds(%T) failed: %v", tc.obj, err)
			}
			wantGVK := herorunv1alpha1.GroupVersion.WithKind(tc.kind)
			if diff := cmp.Diff(wantGVK, gvks[0]); diff != "" {
				t.Errorf("GVK mismatch (-want +got):\n%s", diff)
			}
			created, err := scheme.New(wantGVK)
			if err != nil {
				t.Fatalf("scheme.New(%v) failed: %v", wantGVK, err)
			}
			if created == nil {
				t.Fatalf("scheme.New(%v) returned nil", wantGVK)
			}
		})
	}
}

func TestSessionMessage_JSONRoundTrip(t *testing.T) {
	deliveredAt := metav1.NewTime(time.Date(2026, 11, 24, 12, 15, 0, 0, time.UTC))

	spec := herorunv1alpha1.SessionMessageSpec{
		SessionName: "run20261124abcd",
		RunID:       "20261124T120000Z-abcd",
		From:        "run20261124abcd-c1",
		Text:        "Profile results are ready in profile.txt.",
	}
	original := &herorunv1alpha1.SessionMessage{
		TypeMeta: metav1.TypeMeta{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "SessionMessage",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run20261124abcd-msg-abcde",
			Namespace: "ao-sandboxes",
			Labels:    spec.Labels(),
		},
		Spec: spec,
		Status: herorunv1alpha1.SessionMessageStatus{
			DeliveredAt: &deliveredAt,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var roundTripped herorunv1alpha1.SessionMessage
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if diff := cmp.Diff(original, &roundTripped); diff != "" {
		t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
	}

	list := &herorunv1alpha1.SessionMessageList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "SessionMessageList",
		},
		Items: []herorunv1alpha1.SessionMessage{*original},
	}
	listData, err := json.Marshal(list)
	if err != nil {
		t.Fatalf("json.Marshal(SessionMessageList) failed: %v", err)
	}
	var roundTrippedList herorunv1alpha1.SessionMessageList
	if err := json.Unmarshal(listData, &roundTrippedList); err != nil {
		t.Fatalf("json.Unmarshal(SessionMessageList) failed: %v", err)
	}
	if diff := cmp.Diff(list, &roundTrippedList); diff != "" {
		t.Errorf("SessionMessageList round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestSessionMessage_JSONOmitEmpty(t *testing.T) {
	minimal := &herorunv1alpha1.SessionMessage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone-msg-1",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.SessionMessageSpec{
			SessionName: "standalone1",
			Text:        "Please focus on allocations.",
		},
	}

	data, err := json.Marshal(minimal)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal into map failed: %v", err)
	}

	specMap, ok := raw["spec"].(map[string]any)
	if !ok {
		t.Fatalf("expected spec map in serialized JSON, got %T", raw["spec"])
	}
	wantSpec := map[string]any{
		"sessionName": "standalone1",
		"text":        "Please focus on allocations.",
	}
	if diff := cmp.Diff(wantSpec, specMap); diff != "" {
		t.Errorf("minimal spec mismatch (-want +got):\n%s", diff)
	}
}

func TestSessionMessage_DeepCopy(t *testing.T) {
	deliveredAt := metav1.NewTime(time.Date(2026, 11, 24, 12, 15, 0, 0, time.UTC))
	later := metav1.NewTime(time.Date(2026, 11, 24, 12, 30, 0, 0, time.UTC))

	original := &herorunv1alpha1.SessionMessage{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run20261124abcd-msg-1",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.SessionMessageSpec{
			SessionName: "run20261124abcd",
			RunID:       "20261124T120000Z-abcd",
			From:        "child1",
			Text:        "hello",
		},
		Status: herorunv1alpha1.SessionMessageStatus{
			DeliveredAt: &deliveredAt,
		},
	}

	copied := original.DeepCopy()
	if diff := cmp.Diff(original, copied); diff != "" {
		t.Fatalf("DeepCopy did not return equal object (-want +got):\n%s", diff)
	}

	*copied.Status.DeliveredAt = later
	if original.Status.DeliveredAt.Equal(copied.Status.DeliveredAt) {
		t.Errorf("mutating copy modified original Status.DeliveredAt")
	}

	list := &herorunv1alpha1.SessionMessageList{Items: []herorunv1alpha1.SessionMessage{*original}}
	copiedList := list.DeepCopy()
	if diff := cmp.Diff(list, copiedList); diff != "" {
		t.Fatalf("SessionMessageList.DeepCopy mismatch (-want +got):\n%s", diff)
	}
	copiedList.Items[0].Spec.Text = "mutated"
	if list.Items[0].Spec.Text == copiedList.Items[0].Spec.Text {
		t.Errorf("mutating copied SessionMessageList modified original")
	}
}

func TestSessionMessage_Helpers(t *testing.T) {
	deliveredAt := metav1.NewTime(time.Date(2026, 11, 24, 12, 15, 0, 0, time.UTC))

	var nilMsg *herorunv1alpha1.SessionMessage
	if nilMsg.IsDelivered() {
		t.Errorf("nil SessionMessage.IsDelivered() = true, want false")
	}

	undelivered := &herorunv1alpha1.SessionMessage{}
	if undelivered.IsDelivered() {
		t.Errorf("undelivered SessionMessage.IsDelivered() = true, want false")
	}

	delivered := &herorunv1alpha1.SessionMessage{
		Status: herorunv1alpha1.SessionMessageStatus{
			DeliveredAt: &deliveredAt,
		},
	}
	if !delivered.IsDelivered() {
		t.Errorf("delivered SessionMessage.IsDelivered() = false, want true")
	}

	labelTests := []struct {
		name string
		spec herorunv1alpha1.SessionMessageSpec
		want map[string]string
	}{
		{
			name: "empty",
			spec: herorunv1alpha1.SessionMessageSpec{},
			want: map[string]string{},
		},
		{
			name: "session only",
			spec: herorunv1alpha1.SessionMessageSpec{SessionName: "standalone1"},
			want: map[string]string{
				herorunv1alpha1.LabelSession: "standalone1",
			},
		},
		{
			name: "session and run",
			spec: herorunv1alpha1.SessionMessageSpec{
				SessionName: "run20261124abcd",
				RunID:       "20261124T120000Z-abcd",
			},
			want: map[string]string{
				herorunv1alpha1.LabelSession: "run20261124abcd",
				herorunv1alpha1.LabelRun:     "20261124T120000Z-abcd",
			},
		},
	}

	for _, tc := range labelTests {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, tc.spec.Labels()); diff != "" {
				t.Errorf("Labels() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
