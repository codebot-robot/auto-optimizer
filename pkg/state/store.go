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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/scoped"
)

// ErrNotFound is returned (wrapped) when a requested repository, goal, or run record does not exist.
var ErrNotFound = errors.New("not found")

// Test hooks for simulating failures during atomic writes in tests.
var (
	testHookBeforeSync   func(tmpPath string) error
	testHookBeforeRename func(tmpPath string) error
)

// Store defines the persistence operations for repositories, goals, and runs under the state directory.
type Store interface {
	// PutRepo creates or updates a repository record in repo.json.
	PutRepo(ctx context.Context, repo *Repo) error

	// GetRepo retrieves a repository record by its key.
	GetRepo(ctx context.Context, key RepoKey) (*Repo, error)

	// ListRepos returns all registered repositories sorted by host, owner, and name.
	ListRepos(ctx context.Context) ([]*Repo, error)

	// DeleteRepo deletes a repository and all its goals and runs.
	DeleteRepo(ctx context.Context, key RepoKey) error

	// PutGoal creates or updates a goal record in goal.json.
	PutGoal(ctx context.Context, key RepoKey, record *GoalRecord) error

	// GetGoal retrieves a goal record for a repository.
	GetGoal(ctx context.Context, key RepoKey, goalName string) (*GoalRecord, error)

	// ListGoals returns all goal records for a repository sorted by goal name.
	ListGoals(ctx context.Context, key RepoKey) ([]*GoalRecord, error)

	// DeleteGoal deletes a goal record directory.
	DeleteGoal(ctx context.Context, key RepoKey, goalName string) error

	// PutRun creates or updates a run record in run.json.
	PutRun(ctx context.Context, run *Run) error

	// GetRun retrieves a run record by repository key and run ID.
	GetRun(ctx context.Context, key RepoKey, runID string) (*Run, error)

	// ListRuns returns all runs for a repository sorted chronologically by run ID.
	ListRuns(ctx context.Context, key RepoKey) ([]*Run, error)

	// AppendEvent appends one or more events to events.jsonl for a run.
	AppendEvent(ctx context.Context, key RepoKey, runID string, events ...Event) error

	// ReadEvents reads all events from events.jsonl for a run in append order.
	ReadEvents(ctx context.Context, key RepoKey, runID string) ([]Event, error)
}

// FileStore is the disk-backed implementation of Store.
type FileStore struct {
	rootDir string
}

var _ Store = &FileStore{}

// NewFileStore creates a new FileStore rooted at rootDir.
func NewFileStore(rootDir string) *FileStore {
	return &FileStore{rootDir: rootDir}
}

func (s *FileStore) reposDir() string {
	return filepath.Join(s.rootDir, "repos")
}

func (s *FileStore) repoDir(key RepoKey) string {
	return filepath.Join(s.reposDir(), key.Host, key.Owner, key.Name)
}

func (s *FileStore) repoPath(key RepoKey) string {
	return filepath.Join(s.repoDir(key), "repo.json")
}

func (s *FileStore) goalPath(key RepoKey, goalName string) string {
	return filepath.Join(s.repoDir(key), "goals", goalName, "goal.json")
}

func (s *FileStore) runPath(key RepoKey, runID string) string {
	return filepath.Join(s.repoDir(key), "runs", runID, "run.json")
}

func (s *FileStore) eventsPath(key RepoKey, runID string) string {
	return filepath.Join(s.repoDir(key), "runs", runID, "events.jsonl")
}

// writeJSONAtomically writes a JSON object atomically using a temporary file in the same
// directory, calling fsync, and renaming.
func writeJSONAtomically(ctx context.Context, filePath string, v any) error {
	log := scoped.Log(ctx)
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling json for %s: %w", filePath, err)
	}
	data = append(data, '\n')

	tmpFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}
	tmpName := tmpFile.Name()

	var success bool
	defer func() {
		if !success {
			if rmErr := os.Remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
				log.V(2).Info("failed to remove temporary file", "path", tmpName, "error", rmErr)
			}
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		closeErr := tmpFile.Close()
		return errors.Join(fmt.Errorf("writing to temp file %s: %w", tmpName, err), closeErr)
	}

	if testHookBeforeSync != nil {
		if err := testHookBeforeSync(tmpName); err != nil {
			closeErr := tmpFile.Close()
			return errors.Join(err, closeErr)
		}
	}

	if err := tmpFile.Sync(); err != nil {
		closeErr := tmpFile.Close()
		return errors.Join(fmt.Errorf("syncing temp file %s: %w", tmpName, err), closeErr)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp file %s: %w", tmpName, err)
	}

	if testHookBeforeRename != nil {
		if err := testHookBeforeRename(tmpName); err != nil {
			return err
		}
	}

	if err := os.Rename(tmpName, filePath); err != nil {
		return fmt.Errorf("renaming temp file %s to %s: %w", tmpName, filePath, err)
	}

	success = true
	return nil
}

// PutRepo creates or updates repo.json atomically.
func (s *FileStore) PutRepo(ctx context.Context, repo *Repo) error {
	if repo == nil {
		return errors.New("repo cannot be nil")
	}
	key := repo.Key()
	if err := ValidateRepoKey(key); err != nil {
		return err
	}

	log := scoped.Log(ctx)
	p := s.repoPath(key)
	log.V(2).Info("putting repo record", "key", key.String(), "path", p)
	return writeJSONAtomically(ctx, p, repo)
}

// GetRepo reads repo.json for the given repository.
func (s *FileStore) GetRepo(ctx context.Context, key RepoKey) (*Repo, error) {
	if err := ValidateRepoKey(key); err != nil {
		return nil, err
	}

	log := scoped.Log(ctx)
	p := s.repoPath(key)
	log.V(2).Info("getting repo record", "key", key.String(), "path", p)

	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("repo %s: %w", key, ErrNotFound)
		}
		return nil, fmt.Errorf("reading repo record %s: %w", p, err)
	}

	var repo Repo
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, fmt.Errorf("decoding repo record %s: %w", p, err)
	}
	return &repo, nil
}

// ListRepos returns all repository records, sorted by Host, Owner, and Name.
func (s *FileStore) ListRepos(ctx context.Context) ([]*Repo, error) {
	log := scoped.Log(ctx)
	base := s.reposDir()
	log.V(2).Info("listing repo records", "base", base)

	hostEntries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading repos directory %s: %w", base, err)
	}

	var repos []*Repo
	for _, hostEntry := range hostEntries {
		if !hostEntry.IsDir() {
			continue
		}
		hostName := hostEntry.Name()
		hostPath := filepath.Join(base, hostName)

		ownerEntries, err := os.ReadDir(hostPath)
		if err != nil {
			log.V(2).Info("failed to read host dir", "host", hostName, "error", err)
			continue
		}

		for _, ownerEntry := range ownerEntries {
			if !ownerEntry.IsDir() {
				continue
			}
			ownerName := ownerEntry.Name()
			ownerPath := filepath.Join(hostPath, ownerName)

			nameEntries, err := os.ReadDir(ownerPath)
			if err != nil {
				log.V(2).Info("failed to read owner dir", "owner", ownerName, "error", err)
				continue
			}

			for _, nameEntry := range nameEntries {
				if !nameEntry.IsDir() {
					continue
				}
				repoName := nameEntry.Name()
				key := RepoKey{Host: hostName, Owner: ownerName, Name: repoName}
				if err := ValidateRepoKey(key); err != nil {
					continue
				}

				repo, err := s.GetRepo(ctx, key)
				if err != nil {
					if !errors.Is(err, ErrNotFound) {
						log.V(2).Info("failed to load repo record", "key", key.String(), "error", err)
					}
					continue
				}
				repos = append(repos, repo)
			}
		}
	}

	slices.SortFunc(repos, func(a, b *Repo) int {
		if a.Host != b.Host {
			return strings.Compare(a.Host, b.Host)
		}
		if a.Owner != b.Owner {
			return strings.Compare(a.Owner, b.Owner)
		}
		return strings.Compare(a.Name, b.Name)
	})

	return repos, nil
}

// DeleteRepo removes a repository directory and all its contents.
func (s *FileStore) DeleteRepo(ctx context.Context, key RepoKey) error {
	if err := ValidateRepoKey(key); err != nil {
		return err
	}
	p := s.repoDir(key)
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("deleting repo directory %s: %w", p, err)
	}
	return nil
}

// PutGoal creates or updates goal.json atomically.
func (s *FileStore) PutGoal(ctx context.Context, key RepoKey, record *GoalRecord) error {
	if err := ValidateRepoKey(key); err != nil {
		return err
	}
	if record == nil {
		return errors.New("goal record cannot be nil")
	}
	if record.Goal == nil || record.Goal.Name == "" {
		return errors.New("goal record must have a non-nil goal with a name")
	}

	goalName := record.Goal.Name
	if err := ValidateGoalName(goalName); err != nil {
		return err
	}

	log := scoped.Log(ctx)
	p := s.goalPath(key, goalName)
	log.V(2).Info("putting goal record", "repo", key.String(), "goal", goalName, "path", p)
	return writeJSONAtomically(ctx, p, record)
}

// GetGoal reads goal.json for the given goal.
func (s *FileStore) GetGoal(ctx context.Context, key RepoKey, goalName string) (*GoalRecord, error) {
	if err := ValidateRepoKey(key); err != nil {
		return nil, err
	}
	if err := ValidateGoalName(goalName); err != nil {
		return nil, err
	}

	log := scoped.Log(ctx)
	p := s.goalPath(key, goalName)
	log.V(2).Info("getting goal record", "repo", key.String(), "goal", goalName, "path", p)

	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("goal %q for repo %s: %w", goalName, key, ErrNotFound)
		}
		return nil, fmt.Errorf("reading goal record %s: %w", p, err)
	}

	var record GoalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("decoding goal record %s: %w", p, err)
	}
	if record.Goal != nil && record.Goal.Name == "" {
		record.Goal.Name = goalName
	}
	return &record, nil
}

// ListGoals returns all goal records for a repository, sorted by goal name.
func (s *FileStore) ListGoals(ctx context.Context, key RepoKey) ([]*GoalRecord, error) {
	if err := ValidateRepoKey(key); err != nil {
		return nil, err
	}

	log := scoped.Log(ctx)
	goalsDir := filepath.Join(s.repoDir(key), "goals")
	log.V(2).Info("listing goal records", "repo", key.String(), "dir", goalsDir)

	entries, err := os.ReadDir(goalsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading goals directory %s: %w", goalsDir, err)
	}

	var goalsList []*GoalRecord
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if err := ValidateGoalName(name); err != nil {
			continue
		}

		record, err := s.GetGoal(ctx, key, name)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				log.V(2).Info("failed to load goal record", "goal", name, "error", err)
			}
			continue
		}
		goalsList = append(goalsList, record)
	}

	slices.SortFunc(goalsList, func(a, b *GoalRecord) int {
		var aName, bName string
		if a.Goal != nil {
			aName = a.Goal.Name
		}
		if b.Goal != nil {
			bName = b.Goal.Name
		}
		return strings.Compare(aName, bName)
	})

	return goalsList, nil
}

// DeleteGoal deletes a goal record directory.
func (s *FileStore) DeleteGoal(ctx context.Context, key RepoKey, goalName string) error {
	if err := ValidateRepoKey(key); err != nil {
		return err
	}
	if err := ValidateGoalName(goalName); err != nil {
		return err
	}
	p := filepath.Join(s.repoDir(key), "goals", goalName)
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("deleting goal directory %s: %w", p, err)
	}
	return nil
}

// PutRun creates or updates run.json atomically.
func (s *FileStore) PutRun(ctx context.Context, run *Run) error {
	if run == nil {
		return errors.New("run cannot be nil")
	}
	if err := ValidateRepoKey(run.Repo); err != nil {
		return err
	}
	if run.ID == "" {
		return errors.New("run id cannot be empty")
	}

	log := scoped.Log(ctx)
	p := s.runPath(run.Repo, run.ID)
	log.V(2).Info("putting run record", "repo", run.Repo.String(), "run", run.ID, "path", p)
	return writeJSONAtomically(ctx, p, run)
}

// GetRun reads run.json for the given run ID.
func (s *FileStore) GetRun(ctx context.Context, key RepoKey, runID string) (*Run, error) {
	if err := ValidateRepoKey(key); err != nil {
		return nil, err
	}
	if runID == "" {
		return nil, errors.New("run id cannot be empty")
	}

	log := scoped.Log(ctx)
	p := s.runPath(key, runID)
	log.V(2).Info("getting run record", "repo", key.String(), "run", runID, "path", p)

	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("run %q for repo %s: %w", runID, key, ErrNotFound)
		}
		return nil, fmt.Errorf("reading run record %s: %w", p, err)
	}

	var run Run
	if err := json.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("decoding run record %s: %w", p, err)
	}
	return &run, nil
}

// ListRuns returns all runs for a repository, sorted chronologically by run ID.
func (s *FileStore) ListRuns(ctx context.Context, key RepoKey) ([]*Run, error) {
	if err := ValidateRepoKey(key); err != nil {
		return nil, err
	}

	log := scoped.Log(ctx)
	runsDir := filepath.Join(s.repoDir(key), "runs")
	log.V(2).Info("listing run records", "repo", key.String(), "dir", runsDir)

	entries, err := os.ReadDir(runsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading runs directory %s: %w", runsDir, err)
	}

	var runs []*Run
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runID := entry.Name()
		run, err := s.GetRun(ctx, key, runID)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				log.V(2).Info("failed to load run record", "run", runID, "error", err)
			}
			continue
		}
		runs = append(runs, run)
	}

	slices.SortFunc(runs, func(a, b *Run) int {
		return strings.Compare(a.ID, b.ID)
	})

	return runs, nil
}

// AppendEvent appends one or more events to events.jsonl.
func (s *FileStore) AppendEvent(ctx context.Context, key RepoKey, runID string, events ...Event) error {
	if err := ValidateRepoKey(key); err != nil {
		return err
	}
	if runID == "" {
		return errors.New("run id cannot be empty")
	}
	if len(events) == 0 {
		return nil
	}

	log := scoped.Log(ctx)
	p := s.eventsPath(key, runID)
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range events {
		if events[i].Time.IsZero() {
			events[i].Time = time.Now().UTC()
		}
		if err := enc.Encode(events[i]); err != nil {
			return fmt.Errorf("marshaling event: %w", err)
		}
	}

	log.V(2).Info("appending events", "repo", key.String(), "run", runID, "count", len(events))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("opening events file %s: %w", p, err)
	}

	if _, err := buf.WriteTo(f); err != nil {
		closeErr := f.Close()
		return errors.Join(fmt.Errorf("writing events to %s: %w", p, err), closeErr)
	}
	if err := f.Sync(); err != nil {
		closeErr := f.Close()
		return errors.Join(fmt.Errorf("syncing events file %s: %w", p, err), closeErr)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing events file %s: %w", p, err)
	}

	return nil
}

// ReadEvents reads all events from events.jsonl for the given run in append order.
func (s *FileStore) ReadEvents(ctx context.Context, key RepoKey, runID string) ([]Event, error) {
	if err := ValidateRepoKey(key); err != nil {
		return nil, err
	}
	if runID == "" {
		return nil, errors.New("run id cannot be empty")
	}

	log := scoped.Log(ctx)
	p := s.eventsPath(key, runID)
	log.V(2).Info("reading events", "repo", key.String(), "run", runID, "path", p)

	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("opening events file %s: %w", p, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.V(2).Info("failed to close events file", "path", p, "error", closeErr)
		}
	}()

	var events []Event
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("decoding event at line %d in %s: %w", lineNum, p, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading events file %s: %w", p, err)
	}

	return events, nil
}
