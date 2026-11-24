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

func TestSession_SchemeRegistration(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := herorunv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	tests := []struct {
		name string
		obj  runtime.Object
		kind string
	}{
		{name: "Session", obj: &herorunv1alpha1.Session{}, kind: "Session"},
		{name: "SessionList", obj: &herorunv1alpha1.SessionList{}, kind: "SessionList"},
		{name: "HeroRun", obj: &herorunv1alpha1.HeroRun{}, kind: "HeroRun"},
		{name: "HeroRunList", obj: &herorunv1alpha1.HeroRunList{}, kind: "HeroRunList"},
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

func TestSession_JSONRoundTrip(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))
	later := metav1.NewTime(time.Date(2026, 11, 24, 12, 30, 0, 0, time.UTC))
	waitUntil := metav1.NewTime(time.Date(2026, 11, 24, 12, 5, 0, 0, time.UTC))

	original := &herorunv1alpha1.Session{
		TypeMeta: metav1.TypeMeta{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "Session",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run20261124abcd-c1",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.SessionSpec{
			RunID:              "20261124T120000Z-abcd",
			HeroRunName:        "run-20261124t120000z-abcd",
			Repository:         "https://github.com/gke-labs/test-repo",
			Branch:             "ao/optimize-latency/20261124T120000Z-abcd",
			Parent:             "run20261124abcd",
			Ancestors:          []string{"run20261124abcd"},
			Toolset:            "basic",
			Model:              "gemini-2.5-pro",
			SystemPrompt:       "You are optimizing latency.",
			InitialPrompt:      "Benchmark and profile the parser.",
			SandboxName:        "run-20261124t120000z-abcd",
			WorkspaceClaimName: "run-20261124t120000z-abcd",
			MaxTurns:           25,
			MaxDepth:           3,
			Deadline:           &later,
			AllowPullRequest:   true,
		},
		Status: herorunv1alpha1.SessionStatus{
			Phase:   herorunv1alpha1.SessionPhaseSucceeded,
			Outcome: herorunv1alpha1.SessionOutcomePullRequest,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "Completed",
					Message:            "session requested a pull request",
				},
			},
			Turns:        4,
			MessageCount: 12,
			CostUSD:      0.42,
			PRTitle:      "perf: optimize parser allocation",
			PRBody:       "Reduces allocations by 30%.",
			Summary:      "Completed optimization and requested pull request.",
			Error:        "",
			WaitingUntil: &waitUntil,
			StartedAt:    &now,
			FinishedAt:   &later,
			ObservedAt:   &later,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var roundTripped herorunv1alpha1.Session
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if diff := cmp.Diff(original, &roundTripped); diff != "" {
		t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
	}

	list := &herorunv1alpha1.SessionList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "SessionList",
		},
		Items: []herorunv1alpha1.Session{*original},
	}
	listData, err := json.Marshal(list)
	if err != nil {
		t.Fatalf("json.Marshal(SessionList) failed: %v", err)
	}
	var roundTrippedList herorunv1alpha1.SessionList
	if err := json.Unmarshal(listData, &roundTrippedList); err != nil {
		t.Fatalf("json.Unmarshal(SessionList) failed: %v", err)
	}
	if diff := cmp.Diff(list, &roundTrippedList); diff != "" {
		t.Errorf("SessionList round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestSession_JSONOmitEmpty(t *testing.T) {
	minimal := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone-1",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.SessionSpec{
			Toolset: "basic",
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
	if len(specMap) != 1 || specMap["toolset"] != "basic" {
		t.Errorf("expected only toolset in minimal spec, got %v", specMap)
	}
}

func TestSession_DeepCopy(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))
	later := metav1.NewTime(time.Date(2026, 11, 24, 12, 30, 0, 0, time.UTC))

	original := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "child-session",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.SessionSpec{
			RunID:     "20261124T120000Z-abcd",
			Parent:    "parent-session",
			Ancestors: []string{"root-session", "parent-session"},
			Deadline:  &later,
		},
		Status: herorunv1alpha1.SessionStatus{
			Phase:   herorunv1alpha1.SessionPhaseWaiting,
			Outcome: herorunv1alpha1.SessionOutcomeCompleted,
			Conditions: []metav1.Condition{
				{
					Type:               "Waiting",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "WaitTool",
				},
			},
			WaitingUntil: &later,
			StartedAt:    &now,
			FinishedAt:   &later,
			ObservedAt:   &now,
		},
	}

	copied := original.DeepCopy()
	if diff := cmp.Diff(original, copied); diff != "" {
		t.Fatalf("DeepCopy did not return equal object (-want +got):\n%s", diff)
	}

	copied.Spec.Ancestors[0] = "mutated-root"
	*copied.Spec.Deadline = now
	copied.Status.Conditions[0].Reason = "Mutated"
	*copied.Status.WaitingUntil = now
	*copied.Status.StartedAt = later
	*copied.Status.FinishedAt = now
	*copied.Status.ObservedAt = later

	if original.Spec.Ancestors[0] == copied.Spec.Ancestors[0] {
		t.Errorf("mutating copy modified original Spec.Ancestors")
	}
	if original.Spec.Deadline.Equal(copied.Spec.Deadline) {
		t.Errorf("mutating copy modified original Spec.Deadline")
	}
	if original.Status.Conditions[0].Reason == copied.Status.Conditions[0].Reason {
		t.Errorf("mutating copy modified original Status.Conditions")
	}
	if original.Status.WaitingUntil.Equal(copied.Status.WaitingUntil) {
		t.Errorf("mutating copy modified original Status.WaitingUntil")
	}
	if original.Status.StartedAt.Equal(copied.Status.StartedAt) {
		t.Errorf("mutating copy modified original Status.StartedAt")
	}
	if original.Status.FinishedAt.Equal(copied.Status.FinishedAt) {
		t.Errorf("mutating copy modified original Status.FinishedAt")
	}
	if original.Status.ObservedAt.Equal(copied.Status.ObservedAt) {
		t.Errorf("mutating copy modified original Status.ObservedAt")
	}

	list := &herorunv1alpha1.SessionList{Items: []herorunv1alpha1.Session{*original}}
	copiedList := list.DeepCopy()
	if diff := cmp.Diff(list, copiedList); diff != "" {
		t.Fatalf("SessionList.DeepCopy mismatch (-want +got):\n%s", diff)
	}
	copiedList.Items[0].Spec.RunID = "mutated-run"
	if list.Items[0].Spec.RunID == copiedList.Items[0].Spec.RunID {
		t.Errorf("mutating copied SessionList modified original")
	}
}

func TestSession_Helpers(t *testing.T) {
	phaseTests := []struct {
		phase    herorunv1alpha1.SessionPhase
		terminal bool
	}{
		{phase: "", terminal: false},
		{phase: herorunv1alpha1.SessionPhasePending, terminal: false},
		{phase: herorunv1alpha1.SessionPhaseRunning, terminal: false},
		{phase: herorunv1alpha1.SessionPhaseWaiting, terminal: false},
		{phase: herorunv1alpha1.SessionPhaseSucceeded, terminal: true},
		{phase: herorunv1alpha1.SessionPhaseFailed, terminal: true},
		{phase: herorunv1alpha1.SessionPhaseCanceled, terminal: true},
	}
	for _, tc := range phaseTests {
		if got := tc.phase.IsTerminal(); got != tc.terminal {
			t.Errorf("SessionPhase(%q).IsTerminal() = %v, want %v", tc.phase, got, tc.terminal)
		}
	}

	depthTests := []struct {
		name string
		spec herorunv1alpha1.SessionSpec
		want int
	}{
		{name: "root", spec: herorunv1alpha1.SessionSpec{}, want: 0},
		{name: "parent only", spec: herorunv1alpha1.SessionSpec{Parent: "root"}, want: 1},
		{name: "ancestors", spec: herorunv1alpha1.SessionSpec{Parent: "child1", Ancestors: []string{"root", "child1"}}, want: 2},
	}
	for _, tc := range depthTests {
		if got := tc.spec.Depth(); got != tc.want {
			t.Errorf("SessionSpec(%s).Depth() = %d, want %d", tc.name, got, tc.want)
		}
	}
}
