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
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/google/go-cmp/cmp"
)

func TestRepo_PutGetListDelete(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := NewFileStore(storeDir)

	// Check initially empty
	repos, err := store.ListRepos(ctx)
	if err != nil {
		t.Fatalf("ListRepos on empty store failed: %v", err)
	}
	if len(repos) != 0 {
		t.Fatalf("expected 0 repos, got %d", len(repos))
	}

	r1 := &Repo{
		URL:            "https://github.com/gke-labs/auto-optimizer",
		Host:           "github.com",
		Owner:          "gke-labs",
		Name:           "auto-optimizer",
		DefaultBranch:  "main",
		HeadCommit:     "1234567890abcdef",
		ForgeKind:      "github",
		RegisteredTime: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC),
	}

	r2 := &Repo{
		URL:            "https://github.com/example/foo",
		Host:           "github.com",
		Owner:          "example",
		Name:           "foo",
		DefaultBranch:  "master",
		ForgeKind:      "gitremote",
		RegisteredTime: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
	}

	r3 := &Repo{
		URL:            "/var/git/local-repo",
		Host:           "local",
		Owner:          "git",
		Name:           "local-repo",
		DefaultBranch:  "main",
		ForgeKind:      "gitremote",
		RegisteredTime: time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC),
	}

	for _, r := range []*Repo{r1, r2, r3} {
		if err := store.PutRepo(ctx, r); err != nil {
			t.Fatalf("PutRepo(%v) failed: %v", r.Key(), err)
		}
	}

	// Verify GetRepo
	got, err := store.GetRepo(ctx, r1.Key())
	if err != nil {
		t.Fatalf("GetRepo(%v) failed: %v", r1.Key(), err)
	}
	if diff := cmp.Diff(r1, got); diff != "" {
		t.Errorf("GetRepo mismatch (-want +got):\n%s", diff)
	}

	// Verify not found
	missingKey := RepoKey{Host: "github.com", Owner: "none", Name: "missing"}
	_, err = store.GetRepo(ctx, missingKey)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing repo, got %v", err)
	}

	// Verify ListRepos sorted order (github.com/example/foo, github.com/gke-labs/auto-optimizer, local/git/local-repo)
	all, err := store.ListRepos(ctx)
	if err != nil {
		t.Fatalf("ListRepos failed: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 repos, got %d", len(all))
	}
	if all[0].String() != r2.String() || all[1].String() != r1.String() || all[2].String() != r3.String() {
		t.Errorf("unexpected sort order in ListRepos: %v, %v, %v", all[0].Key(), all[1].Key(), all[2].Key())
	}

	// Verify DeleteRepo
	if err := store.DeleteRepo(ctx, r2.Key()); err != nil {
		t.Fatalf("DeleteRepo failed: %v", err)
	}
	_, err = store.GetRepo(ctx, r2.Key())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after DeleteRepo, got %v", err)
	}
	allAfter, err := store.ListRepos(ctx)
	if err != nil {
		t.Fatalf("ListRepos failed: %v", err)
	}
	if len(allAfter) != 2 {
		t.Fatalf("expected 2 repos after delete, got %d", len(allAfter))
	}
}

func (r *Repo) String() string {
	return r.Key().String()
}

func TestGoal_PutGetListDelete(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := NewFileStore(storeDir)
	key := RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}

	bestVal := 123.45
	g1 := &GoalRecord{
		Goal: &goals.Goal{
			Name: "benchmark",
			Body: "Make quicksort faster",
			Metric: &goals.Metric{
				Command: "./hack/bench.sh",
			},
		},
		Hash:       "deadbeef",
		BaseCommit: "abcdef123456",
		BestValue:  &bestVal,
		LastRunID:  "20261023T120000Z-ab12",
	}

	g2 := &GoalRecord{
		Goal: &goals.Goal{
			Name: "clean-code",
			Body: "Fix linter warnings",
		},
		Hash:       "feedface",
		BaseCommit: "abcdef123456",
	}

	// Initially empty
	goalsList, err := store.ListGoals(ctx, key)
	if err != nil {
		t.Fatalf("ListGoals on empty repo failed: %v", err)
	}
	if len(goalsList) != 0 {
		t.Fatalf("expected 0 goals, got %d", len(goalsList))
	}

	// Put goals
	if err := store.PutGoal(ctx, key, g2); err != nil {
		t.Fatalf("PutGoal(g2) failed: %v", err)
	}
	if err := store.PutGoal(ctx, key, g1); err != nil {
		t.Fatalf("PutGoal(g1) failed: %v", err)
	}

	// GetGoal
	got, err := store.GetGoal(ctx, key, "benchmark")
	if err != nil {
		t.Fatalf("GetGoal(benchmark) failed: %v", err)
	}
	if diff := cmp.Diff(g1, got); diff != "" {
		t.Errorf("GetGoal mismatch (-want +got):\n%s", diff)
	}

	// Get missing goal
	_, err = store.GetGoal(ctx, key, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing goal, got %v", err)
	}

	// ListGoals (should be sorted by name: benchmark, clean-code)
	list, err := store.ListGoals(ctx, key)
	if err != nil {
		t.Fatalf("ListGoals failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 goals, got %d", len(list))
	}
	if list[0].Goal.Name != "benchmark" || list[1].Goal.Name != "clean-code" {
		t.Errorf("unexpected goal sort order: %s, %s", list[0].Goal.Name, list[1].Goal.Name)
	}

	// DeleteGoal
	if err := store.DeleteGoal(ctx, key, "clean-code"); err != nil {
		t.Fatalf("DeleteGoal failed: %v", err)
	}
	_, err = store.GetGoal(ctx, key, "clean-code")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after DeleteGoal, got %v", err)
	}
}

func TestRun_PutRun(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := NewFileStore(storeDir)
	key := RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}

	baseline := 150.0
	candidate := 120.0
	t1 := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	readyDeadline := t1.Add(5 * time.Minute)
	deadline := t1.Add(time.Hour)

	run := &Run{
		ID:            "20261023T100000Z-0001",
		Repo:          key,
		GoalName:      "bench",
		Phase:         PhaseSucceeded,
		BaseCommit:    "c0ffee",
		Branch:        "ao/bench/20261023T100000Z-0001",
		PRTitle:       "optimize benchmark",
		PRBody:        "improves runtime by 15%",
		PRURL:         "https://github.com/gke-labs/auto-optimizer/pull/1",
		SessionID:     "sess-1",
		SandboxName:   "run-sandbox-1",
		ReadyDeadline: &readyDeadline,
		Deadline:      &deadline,
		Summary:       "optimized benchmark runtime",
		Baseline:      &baseline,
		Candidate:     &candidate,
		CostUSD:       0.45,
	}
	run.SetPhase(PhasePending, t1)
	run.SetPhase(PhaseSucceeded, t1.Add(5*time.Minute))

	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun failed: %v", err)
	}

	data, err := os.ReadFile(store.runPath(key, run.ID))
	if err != nil {
		t.Fatalf("reading exported run.json: %v", err)
	}
	var got Run
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshaling exported run.json: %v", err)
	}
	if diff := cmp.Diff(run, &got); diff != "" {
		t.Errorf("exported run.json mismatch (-want +got):\n%s", diff)
	}
}

func TestEvents_AppendAndRead(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := NewFileStore(storeDir)
	key := RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	runID := "20261023T120000Z-abcd"

	// Reading empty events returns nil
	events, err := store.ReadEvents(ctx, key, runID)
	if err != nil {
		t.Fatalf("ReadEvents on empty failed: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}

	e1 := Event{
		Time:    time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC),
		Phase:   PhasePending,
		Message: "Run created",
	}
	e2 := Event{
		Time:    time.Date(2026, 10, 23, 12, 1, 0, 0, time.UTC),
		Phase:   PhasePreparing,
		Message: "Cloning repository",
	}
	e3 := Event{
		Time:    time.Date(2026, 10, 23, 12, 5, 0, 0, time.UTC),
		Kind:    "measurement",
		Message: "Baseline measurement recorded",
		Data:    map[string]any{"value": 150.0},
	}

	// Append first event
	if err := store.AppendEvent(ctx, key, runID, e1); err != nil {
		t.Fatalf("AppendEvent(e1) failed: %v", err)
	}

	// Append next two events in one batch
	if err := store.AppendEvent(ctx, key, runID, e2, e3); err != nil {
		t.Fatalf("AppendEvent(e2, e3) failed: %v", err)
	}

	// Read all events
	readBack, err := store.ReadEvents(ctx, key, runID)
	if err != nil {
		t.Fatalf("ReadEvents failed: %v", err)
	}
	if len(readBack) != 3 {
		t.Fatalf("expected 3 events, got %d", len(readBack))
	}

	if readBack[0].Message != e1.Message || readBack[1].Message != e2.Message || readBack[2].Message != e3.Message {
		t.Errorf("events content or order mismatch: got %+v", readBack)
	}
}

func TestEvents_CutOffRecovery(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())
	key := RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	runID := "20261023T120000Z-test"

	e1 := Event{Message: "event 1"}
	e2 := Event{Message: "event 2"}
	e3 := Event{Message: "event 3"}
	e4 := Event{Message: "event 4"}

	if err := store.AppendEvent(ctx, key, runID, e1, e2, e3); err != nil {
		t.Fatalf("AppendEvent(e1, e2, e3): %v", err)
	}
	path := store.eventsPath(key, runID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	n0 := bytes.IndexByte(data, '\n')
	n1 := bytes.IndexByte(data[n0+1:], '\n') + n0 + 1
	n2 := bytes.IndexByte(data[n1+1:], '\n') + n1 + 1
	len2 := n2 + 1

	// Cut the file at every byte offset inside the third record, check that the
	// reader returns the first two records, then append a record and read back
	// three complete records.
	for cut := len2; cut < len(data); cut++ {
		if err := os.WriteFile(path, data[:cut], 0600); err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		got, err := store.ReadEvents(ctx, key, runID)
		if err != nil {
			t.Fatalf("cut at %d: ReadEvents failed: %v", cut, err)
		}
		if len(got) != 2 || got[0].Message != e1.Message || got[1].Message != e2.Message {
			t.Fatalf("cut at %d: expected first 2 events, got %+v", cut, got)
		}
		if err := store.AppendEvent(ctx, key, runID, e4); err != nil {
			t.Fatalf("cut at %d: AppendEvent(e4) failed: %v", cut, err)
		}
		got3, err := store.ReadEvents(ctx, key, runID)
		if err != nil {
			t.Fatalf("cut at %d: ReadEvents after append failed: %v", cut, err)
		}
		// If cut was before the closing bracket of e3, e3 was incomplete and is skipped,
		// leaving [e1, e2, e4]. If cut was after the closing bracket (missing only the newline),
		// the appended newline completes e3, giving [e1, e2, e3, e4].
		if cut < len(data)-1 {
			if len(got3) != 3 || got3[0].Message != e1.Message || got3[1].Message != e2.Message || got3[2].Message != e4.Message {
				t.Fatalf("cut at %d: expected [e1, e2, e4], got %+v", cut, got3)
			}
		} else {
			if len(got3) != 4 || got3[0].Message != e1.Message || got3[1].Message != e2.Message || got3[2].Message != e3.Message || got3[3].Message != e4.Message {
				t.Fatalf("cut at %d: expected [e1, e2, e3, e4], got %+v", cut, got3)
			}
		}
	}

	// A damaged record anywhere in the file is skipped and logged.
	damagedPath := store.eventsPath(key, "damaged")
	if err := os.MkdirAll(filepath.Dir(damagedPath), 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	damaged := `{"message":"1"}` + "\n" + `{"bad json` + "\n" + `{"message":"3"}` + "\n"
	if err := os.WriteFile(damagedPath, []byte(damaged), 0600); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	gotDamaged, err := store.ReadEvents(ctx, key, "damaged")
	if err != nil {
		t.Fatalf("ReadEvents with damaged record: %v", err)
	}
	if len(gotDamaged) != 2 || gotDamaged[0].Message != "1" || gotDamaged[1].Message != "3" {
		t.Fatalf("expected [1, 3], got %+v", gotDamaged)
	}

	// Two appends in a row with no crash read back with no extra records.
	normalRunID := "20261023T120000Z-normal"
	if err := store.AppendEvent(ctx, key, normalRunID, e1, e2); err != nil {
		t.Fatalf("AppendEvent(e1, e2): %v", err)
	}
	if err := store.AppendEvent(ctx, key, normalRunID, e3, e4); err != nil {
		t.Fatalf("AppendEvent(e3, e4): %v", err)
	}
	gotNormal, err := store.ReadEvents(ctx, key, normalRunID)
	if err != nil {
		t.Fatalf("ReadEvents(normal): %v", err)
	}
	if len(gotNormal) != 4 || gotNormal[0].Message != e1.Message || gotNormal[1].Message != e2.Message || gotNormal[2].Message != e3.Message || gotNormal[3].Message != e4.Message {
		t.Fatalf("expected [e1, e2, e3, e4], got %+v", gotNormal)
	}
}

func TestRunID_Ordering(t *testing.T) {
	// Generate IDs across timestamps
	base := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	times := []time.Time{
		base,
		base.Add(1 * time.Second),
		base.Add(2 * time.Second),
		base.Add(1 * time.Minute),
		base.Add(1 * time.Hour),
		base.Add(24 * time.Hour),
	}

	var ids []string
	for _, ts := range times {
		id, err := NewRunIDAt(ts)
		if err != nil {
			t.Fatalf("NewRunIDAt(%v) failed: %v", ts, err)
		}
		if err := ValidateRunID(id); err != nil {
			t.Fatalf("generated invalid run id %q: %v", id, err)
		}

		parsed, err := ParseRunIDTime(id)
		if err != nil {
			t.Fatalf("ParseRunIDTime(%q) failed: %v", id, err)
		}
		if !parsed.Equal(ts) {
			t.Errorf("ParseRunIDTime = %v, want %v", parsed, ts)
		}

		ids = append(ids, id)
	}

	// Shuffle the ids
	shuffled := slices.Clone(ids)
	shuffled[0], shuffled[3] = shuffled[3], shuffled[0]
	shuffled[1], shuffled[5] = shuffled[5], shuffled[1]

	// Sort lexicographically
	slices.Sort(shuffled)

	// Lexicographical order must match chronological time order
	if diff := cmp.Diff(ids, shuffled); diff != "" {
		t.Errorf("run ID ordering mismatch (-want +got):\n%s", diff)
	}

	// Also verify NewRunID() returns a valid run ID
	currentID, err := NewRunID()
	if err != nil {
		t.Fatalf("NewRunID() failed: %v", err)
	}
	if err := ValidateRunID(currentID); err != nil {
		t.Fatalf("ValidateRunID(currentID) failed: %v", err)
	}
}

func TestAtomicWrite_SimulatedFailure(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := NewFileStore(storeDir)

	repo := &Repo{
		URL:            "https://github.com/gke-labs/auto-optimizer",
		Host:           "github.com",
		Owner:          "gke-labs",
		Name:           "auto-optimizer",
		DefaultBranch:  "main",
		ForgeKind:      "github",
		RegisteredTime: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC),
	}

	// First, successfully write initial valid content
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("initial PutRepo failed: %v", err)
	}

	p := store.repoPath(repo.Key())
	origContent, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading repo.json failed: %v", err)
	}

	// Now simulate a failure before rename during an update
	simulatedErr := errors.New("simulated disk error before rename")
	testHookBeforeRename = func(tmpPath string) error {
		// Confirm the temp file exists before rename
		if _, err := os.Stat(tmpPath); err != nil {
			t.Errorf("expected temp file %s to exist: %v", tmpPath, err)
		}
		return simulatedErr
	}
	defer func() {
		testHookBeforeRename = nil
	}()

	updatedRepo := *repo
	updatedRepo.DefaultBranch = "corrupted-branch"
	err = store.PutRepo(ctx, &updatedRepo)
	if !errors.Is(err, simulatedErr) {
		t.Fatalf("expected simulated error, got %v", err)
	}

	// Verify original file was not modified or corrupted
	afterFailContent, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading repo.json after failure failed: %v", err)
	}
	if string(afterFailContent) != string(origContent) {
		t.Errorf("file was altered during failed atomic write:\nwant:\n%s\ngot:\n%s",
			string(origContent), string(afterFailContent))
	}

	// Verify no temporary files remain in the repository directory
	dir := filepath.Dir(p)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading repo dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".tmp-") {
			t.Errorf("temporary file %s was not cleaned up after failure", entry.Name())
		}
	}

	// Now clear hook and verify a subsequent write succeeds cleanly
	testHookBeforeRename = nil
	updatedRepo.DefaultBranch = "develop"
	if err := store.PutRepo(ctx, &updatedRepo); err != nil {
		t.Fatalf("subsequent PutRepo failed: %v", err)
	}

	got, err := store.GetRepo(ctx, repo.Key())
	if err != nil {
		t.Fatalf("GetRepo failed: %v", err)
	}
	if got.DefaultBranch != "develop" {
		t.Errorf("expected DefaultBranch 'develop', got %q", got.DefaultBranch)
	}
}

func TestParseRepoURL(t *testing.T) {
	tests := []struct {
		input   string
		want    RepoKey
		wantErr bool
	}{
		{
			input: "https://github.com/gke-labs/auto-optimizer",
			want:  RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
		},
		{
			input: "https://github.com/gke-labs/auto-optimizer.git",
			want:  RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
		},
		{
			input: "http://github.com/gke-labs/auto-optimizer.git",
			want:  RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
		},
		{
			input: "git@github.com:gke-labs/auto-optimizer.git",
			want:  RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
		},
		{
			input: "ssh://git@github.com/gke-labs/auto-optimizer.git",
			want:  RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
		},
		{
			input: "github.com/gke-labs/auto-optimizer",
			want:  RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"},
		},
		{
			input: "file:///path/to/myrepo",
			want:  RepoKey{Host: "local", Owner: "to", Name: "myrepo"},
		},
		{
			input: "file:///myrepo.git",
			want:  RepoKey{Host: "local", Owner: "_", Name: "myrepo"},
		},
		{
			input: "/tmp/test-repo",
			want:  RepoKey{Host: "local", Owner: "tmp", Name: "test-repo"},
		},
		{
			input: "./local-repo",
			want:  RepoKey{Host: "local", Owner: "_", Name: "local-repo"},
		},
		{
			input:   "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseRepoURL(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseRepoURL(%q) expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseRepoURL(%q) unexpected error: %v", tc.input, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("ParseRepoURL(%q) mismatch (-want +got):\n%s", tc.input, diff)
			}
		})
	}
}

func TestValidation(t *testing.T) {
	// RepoKey validation
	if err := ValidateRepoKey(RepoKey{Host: "", Owner: "o", Name: "n"}); err == nil {
		t.Error("expected error for empty Host")
	}
	if err := ValidateRepoKey(RepoKey{Host: "h/bad", Owner: "o", Name: "n"}); err == nil {
		t.Error("expected error for Host with slash")
	}
	if err := ValidateRepoKey(RepoKey{Host: "h", Owner: "..", Name: "n"}); err == nil {
		t.Error("expected error for Owner with ..")
	}

	// GoalName validation
	if err := ValidateGoalName(""); err == nil {
		t.Error("expected error for empty goal name")
	}
	if err := ValidateGoalName("bad/name"); err == nil {
		t.Error("expected error for goal name with slash")
	}
	if err := ValidateGoalName("INVALID_CAPS"); err == nil {
		t.Error("expected error for goal name with uppercase")
	}
	if err := ValidateGoalName("valid-goal.1_name"); err != nil {
		t.Errorf("unexpected error for valid goal name: %v", err)
	}

	// RunPhase terminal check
	if !PhaseSucceeded.IsTerminal() || !PhaseFailed.IsTerminal() || !PhaseCancelled.IsTerminal() || !PhaseExpired.IsTerminal() || !PhaseNoImprovement.IsTerminal() {
		t.Error("expected terminal phases to return true from IsTerminal()")
	}
	if PhasePending.IsTerminal() || PhasePreparing.IsTerminal() || PhaseWorking.IsTerminal() || PhaseSelecting.IsTerminal() || PhaseOpeningPR.IsTerminal() {
		t.Error("expected active phases to return false from IsTerminal()")
	}
}

func TestRun_BranchName(t *testing.T) {
	var nilRun *Run
	if got := nilRun.BranchName(); got != "" {
		t.Errorf("nil Run.BranchName() = %q, want empty", got)
	}

	explicitRun := &Run{
		Branch:   "custom-branch",
		GoalName: "my-goal",
		ID:       "run-1",
	}
	if got := explicitRun.BranchName(); got != "custom-branch" {
		t.Errorf("explicit Run.BranchName() = %q, want custom-branch", got)
	}

	defaultRun := &Run{
		GoalName: "my-goal",
		ID:       "run-1",
	}
	if got := defaultRun.BranchName(); got != "ao/my-goal/run-1" {
		t.Errorf("default Run.BranchName() = %q, want ao/my-goal/run-1", got)
	}

	emptyRun := &Run{}
	if got := emptyRun.BranchName(); got != "" {
		t.Errorf("empty Run.BranchName() = %q, want empty", got)
	}
}

func TestFileStore_PendingMessages(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := NewFileStore(storeDir)

	repoKey := RepoKey{Host: "github.com", Owner: "gke-labs", Name: "auto-optimizer"}
	runID := "20261123T100000Z-0001"
	sessA := "sessiona"
	sessB := "sessionb"

	// Initially empty
	msgs, err := store.ReadPendingMessages(ctx, repoKey, runID, sessA)
	if err != nil {
		t.Fatalf("ReadPendingMessages on empty: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 pending messages, got %d", len(msgs))
	}

	// Append messages to sessA and sessB
	if err := store.AppendPendingMessage(ctx, repoKey, runID, sessA, PendingMessage{From: "sessroot", Text: "first message"}); err != nil {
		t.Fatalf("AppendPendingMessage: %v", err)
	}
	if err := store.AppendPendingMessage(ctx, repoKey, runID, sessA, PendingMessage{Text: "second message"}); err != nil {
		t.Fatalf("AppendPendingMessage: %v", err)
	}
	if err := store.AppendPendingMessage(ctx, repoKey, runID, sessB, PendingMessage{From: "sessroot", Text: "message for b"}); err != nil {
		t.Fatalf("AppendPendingMessage for b: %v", err)
	}

	// Reading sessA only returns sessA's messages
	msgs, err = store.ReadPendingMessages(ctx, repoKey, runID, sessA)
	if err != nil {
		t.Fatalf("ReadPendingMessages: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 pending messages for sessA, got %d", len(msgs))
	}
	if msgs[0].From != "sessroot" || msgs[0].Text != "first message" {
		t.Fatalf("unexpected msgs[0]: %+v", msgs[0])
	}
	if msgs[1].From != "" || msgs[1].Text != "second message" {
		t.Fatalf("unexpected msgs[1]: %+v", msgs[1])
	}

	// Reading sessB returns sessB's message
	msgsB, err := store.ReadPendingMessages(ctx, repoKey, runID, sessB)
	if err != nil {
		t.Fatalf("ReadPendingMessages sessB: %v", err)
	}
	if len(msgsB) != 1 || msgsB[0].Text != "message for b" {
		t.Fatalf("unexpected msgsB: %+v", msgsB)
	}

	// Clear messages for sessA
	if err := store.ClearPendingMessages(ctx, repoKey, runID, sessA); err != nil {
		t.Fatalf("ClearPendingMessages: %v", err)
	}

	msgs, err = store.ReadPendingMessages(ctx, repoKey, runID, sessA)
	if err != nil {
		t.Fatalf("ReadPendingMessages after clear: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 pending messages after clear, got %d", len(msgs))
	}

	// sessB still has its pending message
	msgsB, err = store.ReadPendingMessages(ctx, repoKey, runID, sessB)
	if err != nil {
		t.Fatalf("ReadPendingMessages sessB after sessA cleared: %v", err)
	}
	if len(msgsB) != 1 || msgsB[0].Text != "message for b" {
		t.Fatalf("expected sessB pending message to remain, got %+v", msgsB)
	}

	// Clear on non-existent is a no-op
	if err := store.ClearPendingMessages(ctx, repoKey, runID, sessA); err != nil {
		t.Fatalf("ClearPendingMessages on non-existent: %v", err)
	}
}
