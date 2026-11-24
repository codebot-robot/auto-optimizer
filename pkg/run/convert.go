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

package run

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/scoped"
	"github.com/gke-labs/auto-optimizer/pkg/state"
)

// GetRun reads a HeroRun custom resource from kube-apiserver by run ID and converts it to a state.Run.
// If the HeroRun is not found, it returns state.ErrNotFound.
func GetRun(ctx context.Context, c client.Client, namespace, runID string) (*state.Run, error) {
	name := sandboxNameForRun(runID)
	var hr herorunv1alpha1.HeroRun
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &hr); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, state.ErrNotFound
		}
		return nil, fmt.Errorf("getting HeroRun %s: %w", name, err)
	}
	run, err := HeroRunToRun(&hr)
	if err != nil {
		return nil, fmt.Errorf("converting HeroRun %s: %w", name, err)
	}
	return run, nil
}

// ListRuns reads HeroRun custom resources from kube-apiserver for a repository and converts them to state.Runs.
// If repo is the zero RepoKey, all HeroRuns in the namespace are returned.
// Returned runs are sorted by ID in ascending order.
func ListRuns(ctx context.Context, c client.Client, namespace string, repo state.RepoKey) ([]*state.Run, error) {
	var hrList herorunv1alpha1.HeroRunList
	if err := c.List(ctx, &hrList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing HeroRuns: %w", err)
	}

	var runs []*state.Run
	for i := range hrList.Items {
		hr := &hrList.Items[i]
		run, err := HeroRunToRun(hr)
		if err != nil {
			scoped.Log(ctx).Error(err, "failed to convert HeroRun to run", "heroRun", hr.Name)
			continue
		}
		if repo != (state.RepoKey{}) && run.Repo != repo {
			continue
		}
		runs = append(runs, run)
	}
	slices.SortFunc(runs, func(a, b *state.Run) int {
		return strings.Compare(a.ID, b.ID)
	})
	return runs, nil
}

// HeroRunPhase returns the phase of a HeroRun, defaulting an empty phase to PhasePending.
func HeroRunPhase(hr *herorunv1alpha1.HeroRun) state.RunPhase {
	if hr.Status.Phase == "" {
		return state.PhasePending
	}
	return state.RunPhase(hr.Status.Phase)
}

// HeroRunToRun converts a HeroRun custom resource into a state.Run.
// This conversion is temporary scaffolding while run state is migrated from
// state.Run in run.json to the HeroRun custom resource in kube-apiserver,
// and will be removed once state.Run is phased out.
func HeroRunToRun(hr *herorunv1alpha1.HeroRun) (*state.Run, error) {
	run := &state.Run{
		ID:                 hr.Spec.RunID,
		GoalName:           hr.Spec.Goal,
		Phase:              HeroRunPhase(hr),
		BaseCommit:         hr.Spec.BaseCommit,
		Branch:             hr.Status.Branch,
		PRTitle:            hr.Status.PRTitle,
		PRBody:             hr.Status.PRBody,
		PRURL:              hr.Status.PullRequestURL,
		SessionID:          hr.Status.SessionID,
		ActiveOperation:    fromHeroRunActiveOperation(hr.Status.ActiveOperation),
		SandboxName:        hr.Status.SandboxName,
		WorkspaceClaimName: hr.Status.WorkspaceClaimName,
		ReadyDeadline:      fromMetaTime(hr.Status.ReadyDeadline),
		Deadline:           fromMetaTime(hr.Spec.Deadline),
		Summary:            hr.Status.Summary,
		Error:              hr.Status.Error,
		ErrorKind:          hr.Status.ErrorKind,
		CostUSD:            hr.Status.CostUSD,
		Baseline:           hr.Status.Baseline,
		Candidate:          hr.Status.Candidate,
	}

	if hr.Spec.Repository != "" {
		repoKey, err := state.ParseRepoURL(hr.Spec.Repository)
		if err != nil {
			return nil, fmt.Errorf("parsing repository URL %q: %w", hr.Spec.Repository, err)
		}
		run.Repo = repoKey
	}

	if hr.Spec.GoalDefinition != nil {
		run.Goal = &goals.Goal{
			Name:     hr.Spec.Goal,
			Body:     hr.Spec.GoalDefinition.Body,
			Hash:     hr.Spec.GoalDefinition.Hash,
			Verify:   hr.Spec.GoalDefinition.Verify,
			Schedule: hr.Spec.GoalDefinition.Schedule,
			Agents:   slices.Clone(hr.Spec.GoalDefinition.Agents),
		}
		if hr.Spec.GoalDefinition.Metric != nil {
			m := hr.Spec.GoalDefinition.Metric
			run.Goal.Metric = &goals.Metric{
				Command:        m.Command,
				Direction:      m.Direction,
				Unit:           m.Unit,
				MinImprovement: m.MinImprovement,
				Runs:           m.Runs,
				Timeout:        m.Timeout,
			}
		}
		if hr.Spec.GoalDefinition.Budget != nil {
			b := hr.Spec.GoalDefinition.Budget
			run.Goal.Budget = &goals.Budget{
				MaxDuration: b.MaxDuration,
				MaxCostUSD:  b.MaxCostUSD,
			}
		}
	}

	if hr.Status.PhaseTimestamps != nil {
		run.PhaseTimestamps = make(map[state.RunPhase]time.Time, len(hr.Status.PhaseTimestamps))
		for p, t := range hr.Status.PhaseTimestamps {
			run.PhaseTimestamps[state.RunPhase(p)] = t.Time.Truncate(time.Second).UTC()
		}
	}

	return run, nil
}

// HeroRunFromRun converts a state.Run into a HeroRun custom resource.
// This conversion is temporary scaffolding while run state is migrated from
// state.Run in run.json to the HeroRun custom resource in kube-apiserver,
// and will be removed once state.Run is phased out.
//
// Note: state.Run carries only state.RepoKey, not the full repository forge URL.
// Spec.Repository is left empty here and populated by the caller creating the
// HeroRun (such as the scheduler or orchestrator) from state.Repo.URL.
func HeroRunFromRun(run *state.Run) *herorunv1alpha1.HeroRun {
	hr := &herorunv1alpha1.HeroRun{
		TypeMeta: metav1.TypeMeta{
			APIVersion: herorunv1alpha1.GroupVersion.String(),
			Kind:       "HeroRun",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: sandboxNameForRun(run.ID),
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      run.ID,
			Goal:       run.GoalName,
			BaseCommit: run.BaseCommit,
			Deadline:   toMetaTime(run.Deadline),
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase:              string(run.Phase),
			Branch:             run.Branch,
			PRTitle:            run.PRTitle,
			PRBody:             run.PRBody,
			PullRequestURL:     run.PRURL,
			SessionID:          run.SessionID,
			ActiveOperation:    toHeroRunActiveOperation(run.ActiveOperation),
			SandboxName:        run.SandboxName,
			WorkspaceClaimName: run.WorkspaceClaimName,
			ReadyDeadline:      toMetaTime(run.ReadyDeadline),
			Summary:            run.Summary,
			Error:              run.Error,
			ErrorKind:          run.ErrorKind,
			CostUSD:            run.CostUSD,
			Baseline:           run.Baseline,
			Candidate:          run.Candidate,
		},
	}

	if run.Goal != nil {
		if hr.Spec.Goal == "" {
			hr.Spec.Goal = run.Goal.Name
		}
		hr.Spec.GoalDefinition = &herorunv1alpha1.GoalDefinition{
			Body:     run.Goal.Body,
			Hash:     run.Goal.Hash,
			Verify:   run.Goal.Verify,
			Schedule: run.Goal.Schedule,
			Agents:   slices.Clone(run.Goal.Agents),
		}
		if run.Goal.Metric != nil {
			m := run.Goal.Metric
			hr.Spec.GoalDefinition.Metric = &herorunv1alpha1.MetricSpec{
				Command:        m.Command,
				Direction:      m.Direction,
				Unit:           m.Unit,
				MinImprovement: m.MinImprovement,
				Runs:           m.Runs,
				Timeout:        m.Timeout,
			}
		}
		if run.Goal.Budget != nil {
			b := run.Goal.Budget
			hr.Spec.GoalDefinition.Budget = &herorunv1alpha1.BudgetSpec{
				MaxDuration: b.MaxDuration,
				MaxCostUSD:  b.MaxCostUSD,
			}
		}
	}

	if run.PhaseTimestamps != nil {
		hr.Status.PhaseTimestamps = make(map[string]metav1.Time, len(run.PhaseTimestamps))
		for p, t := range run.PhaseTimestamps {
			hr.Status.PhaseTimestamps[string(p)] = metav1.NewTime(t.Truncate(time.Second).UTC())
		}
	}

	return hr
}

// toMetaTime truncates t to whole seconds in UTC, matching metav1.Time's JSON serialization.
func toMetaTime(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	mt := metav1.NewTime(t.Truncate(time.Second).UTC())
	return &mt
}

// fromMetaTime returns a time.Time truncated to whole seconds in UTC.
func fromMetaTime(mt *metav1.Time) *time.Time {
	if mt == nil {
		return nil
	}
	t := mt.Time.Truncate(time.Second).UTC()
	return &t
}

func fromHeroRunActiveOperation(op *herorunv1alpha1.ActiveOperation) *state.ActiveOperation {
	if op == nil {
		return nil
	}
	return &state.ActiveOperation{
		ID:         op.ID,
		Done:       op.Done,
		Result:     state.OperationResult(op.Result),
		PRTitle:    op.PRTitle,
		PRBody:     op.PRBody,
		Summary:    op.Summary,
		Error:      op.Error,
		FinishedAt: fromMetaTime(op.FinishedAt),
	}
}

func toHeroRunActiveOperation(op *state.ActiveOperation) *herorunv1alpha1.ActiveOperation {
	if op == nil {
		return nil
	}
	return &herorunv1alpha1.ActiveOperation{
		ID:         op.ID,
		Done:       op.Done,
		Result:     herorunv1alpha1.OperationResult(op.Result),
		PRTitle:    op.PRTitle,
		PRBody:     op.PRBody,
		Summary:    op.Summary,
		Error:      op.Error,
		FinishedAt: toMetaTime(op.FinishedAt),
	}
}
