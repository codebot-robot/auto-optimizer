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

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
)

func TestHeroRun_JSONRoundTrip(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC))
	later := metav1.NewTime(time.Date(2026, 11, 5, 12, 30, 0, 0, time.UTC))

	original := &herorunv1alpha1.HeroRun{
		TypeMeta: metav1.TypeMeta{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "HeroRun",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: "https://github.com/gke-labs/test-repo",
			Goal:       "optimize-latency",
			RunID:      "20261105T120000Z-abcd",
			BaseCommit: "0123456789abcdef0123456789abcdef01234567",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: "Working",
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "Started",
					Message:            "run session is in progress",
				},
			},
			SandboxName:    "run-sandbox-abcd",
			Branch:         "ao/optimize-latency/20261105T120000Z-abcd",
			PullRequestURL: "https://github.com/gke-labs/test-repo/pull/1",
			StartedAt:      &now,
			FinishedAt:     &later,
			ObservedAt:     &now,
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var roundTripped herorunv1alpha1.HeroRun
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if diff := cmp.Diff(original, &roundTripped); diff != "" {
		t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
	}
}

func TestHeroRun_DeepCopy(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC))
	original := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: "https://github.com/gke-labs/test-repo",
			Goal:       "optimize-latency",
			RunID:      "20261105T120000Z-abcd",
			BaseCommit: "0123456789abcdef0123456789abcdef01234567",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: "Working",
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: now,
					Reason:             "Started",
				},
			},
			ObservedAt: &now,
		},
	}

	copied := original.DeepCopy()
	if diff := cmp.Diff(original, copied); diff != "" {
		t.Fatalf("DeepCopy did not return equal object (-want +got):\n%s", diff)
	}

	// Mutate the copy and verify independence
	copied.Spec.Goal = "new-goal"
	copied.Status.Conditions[0].Reason = "Updated"
	if original.Spec.Goal == copied.Spec.Goal {
		t.Errorf("mutating copy modified original Spec.Goal")
	}
	if original.Status.Conditions[0].Reason == copied.Status.Conditions[0].Reason {
		t.Errorf("mutating copy modified original Status.Conditions")
	}
}
