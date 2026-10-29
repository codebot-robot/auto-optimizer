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

package session

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
)

// newTestSession returns a Session backed by a scratch backup directory.
// getBackupDir() resolves backups under os.UserHomeDir()/.local/sandboxed-tools/<name>/fs,
// so redirecting HOME keeps these tests off the real user's home directory.
func newTestSession(t *testing.T) *Session {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	return &Session{Name: "test-session"}
}

// writeBackup creates a fake backup tarball with the given timestamp
// component (e.g. "20260103T000000") and returns its full path.
func writeBackup(t *testing.T, session *Session, timestamp string) string {
	t.Helper()
	dir, err := session.getBackupDir()
	if err != nil {
		t.Fatalf("getBackupDir: %v", err)
	}
	path := filepath.Join(dir, "backup-"+timestamp+".tar.gz")
	if err := os.WriteFile(path, []byte("fake-tarball"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
	return path
}

func remainingBackups(t *testing.T, session *Session) []string {
	t.Helper()
	dir, err := session.getBackupDir()
	if err != nil {
		t.Fatalf("getBackupDir: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "backup-*.tar.gz"))
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	return matches
}

func TestFindLatestBackup_NoBackups(t *testing.T) {
	session := newTestSession(t)

	got, err := session.FindLatestBackup()
	if err != nil {
		t.Fatalf("FindLatestBackup: %v", err)
	}
	if got != "" {
		t.Errorf("FindLatestBackup() = %q, want empty string when no backups exist", got)
	}
}

func TestFindLatestBackup_ReturnsMostRecentByTimestamp(t *testing.T) {
	session := newTestSession(t)
	// Written out of chronological order -- FindLatestBackup must sort by
	// name, not by creation order or mtime.
	writeBackup(t, session, "20260101T000000")
	want := writeBackup(t, session, "20260103T000000")
	writeBackup(t, session, "20260102T000000")

	got, err := session.FindLatestBackup()
	if err != nil {
		t.Fatalf("FindLatestBackup: %v", err)
	}
	if got != want {
		t.Errorf("FindLatestBackup() = %q, want %q (the newest timestamp)", got, want)
	}
}

func TestFindLatestBackup_IgnoresNonBackupFiles(t *testing.T) {
	session := newTestSession(t)
	want := writeBackup(t, session, "20260101T000000")

	dir, err := session.getBackupDir()
	if err != nil {
		t.Fatalf("getBackupDir: %v", err)
	}
	for _, name := range []string{"notes.txt", "backup-corrupt", "backup-20260102T000000.tar.gz.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := session.FindLatestBackup()
	if err != nil {
		t.Fatalf("FindLatestBackup: %v", err)
	}
	if got != want {
		t.Errorf("FindLatestBackup() = %q, want %q (non-matching files must be ignored)", got, want)
	}
}

func TestPruneBackups_NoBackupsIsNoop(t *testing.T) {
	session := newTestSession(t)

	if err := session.PruneBackups(t.Context(), 5); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}
}

func TestPruneBackups_KeepsEverythingWhenAtOrUnderKeepCount(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000")
	writeBackup(t, session, "20260102T000000")

	if err := session.PruneBackups(t.Context(), 5); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	if got := remainingBackups(t, session); len(got) != 2 {
		t.Errorf("remaining backups = %v, want 2 (nothing pruned)", got)
	}
}

func TestPruneBackups_ExactlyAtKeepCountIsNoop(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000")
	writeBackup(t, session, "20260102T000000")
	writeBackup(t, session, "20260103T000000")

	if err := session.PruneBackups(t.Context(), 3); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	if got := remainingBackups(t, session); len(got) != 3 {
		t.Errorf("remaining backups = %v, want 3 (boundary case: len == keepCount, nothing pruned)", got)
	}
}

func TestPruneBackups_DeletesOldestKeepingNewest(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000") // oldest, must be pruned
	writeBackup(t, session, "20260102T000000") // must be pruned
	keep1 := writeBackup(t, session, "20260103T000000")
	keep2 := writeBackup(t, session, "20260104T000000")

	if err := session.PruneBackups(t.Context(), 2); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	got := remainingBackups(t, session)
	if len(got) != 2 {
		t.Fatalf("remaining backups = %v, want exactly 2", got)
	}
	remaining := map[string]bool{got[0]: true, got[1]: true}
	if !remaining[keep1] || !remaining[keep2] {
		t.Errorf("remaining backups = %v, want the two newest (%q, %q) -- pruning kept the wrong files", got, keep1, keep2)
	}
}

func TestPruneBackups_ZeroKeepCountDeletesEverything(t *testing.T) {
	session := newTestSession(t)
	writeBackup(t, session, "20260101T000000")
	writeBackup(t, session, "20260102T000000")

	if err := session.PruneBackups(t.Context(), 0); err != nil {
		t.Fatalf("PruneBackups: %v", err)
	}

	if got := remainingBackups(t, session); len(got) != 0 {
		t.Errorf("remaining backups = %v, want 0", got)
	}
}

func TestNewAndLoadSession_Metadata(t *testing.T) {
	ctx := t.Context()
	store := NewFileStore(t.TempDir())
	harness := NewHarness(nil, nil, nil, RunOptions{Namespace: "ns", HomeDir: "/home/x"})

	if _, err := harness.LoadSession(ctx, store, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("LoadSession(missing) err = %v, want ErrNotFound", err)
	}

	// A new session persists the metadata it was built with, stamping the
	// creation time.
	before := time.Now()
	session, err := harness.NewSession(ctx, store, "child1", Metadata{Parent: "parent1", Toolset: "geminicli"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if session.Metadata.Parent != "parent1" || session.Metadata.Toolset != "geminicli" {
		t.Errorf("new session metadata = %+v, want parent1/geminicli", session.Metadata)
	}
	if session.Metadata.CreatedAt.Before(before) {
		t.Errorf("CreatedAt = %v, want >= %v", session.Metadata.CreatedAt, before)
	}

	// Reusing an existing session name in NewSession fails.
	if _, err := harness.NewSession(ctx, store, "child1", Metadata{Toolset: "basic"}); err == nil {
		t.Error("NewSession with existing session name should fail")
	}

	// Loading the session restores the stored metadata.
	resumed, err := harness.LoadSession(ctx, store, "child1")
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if !resumed.Metadata.CreatedAt.Equal(session.Metadata.CreatedAt) || resumed.Metadata.Parent != "parent1" || resumed.Metadata.Toolset != "geminicli" {
		t.Errorf("resumed metadata = %+v, want the stored %+v", resumed.Metadata, session.Metadata)
	}

	// A session recorded before metadata existed loads with zero metadata
	// rather than being backfilled.
	content := "hello"
	if err := store.AppendMessages(ctx, "legacy", llm.Message{Role: "user", Content: &content}); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}
	legacy, err := harness.LoadSession(ctx, store, "legacy")
	if err != nil {
		t.Fatalf("LoadSession (legacy): %v", err)
	}
	if legacy.Metadata != (Metadata{}) {
		t.Errorf("legacy metadata = %+v, want zero", legacy.Metadata)
	}
	if _, err := harness.NewSession(ctx, store, "legacy", Metadata{Toolset: "basic"}); err == nil {
		t.Error("NewSession on existing legacy session should fail")
	}
}

func TestWaitForReady_Success(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-ready"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				},
			},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset()

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	if err := sandbox.WaitForReady(ctx); err != nil {
		t.Fatalf("WaitForReady failed unexpectedly: %v", err)
	}

	if sandbox.podName != sbName {
		t.Errorf("podName = %q, want %q", sandbox.podName, sbName)
	}
}

func TestWaitForReady_FailsFast_ImagePullBackOff_WithEvent(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-pull-error"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionFalse,
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "sandbox",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: `Back-off pulling image "geminicli-toolbox:latest"`,
						},
					},
				},
			},
		},
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "pull-failure-event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Pod",
			Name: sbName,
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "Failed",
		Message: `Failed to pull image "geminicli-toolbox:latest": rpc error: code = NotFound desc = manifest unknown`,
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset(pod, event)

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	start := time.Now()
	err := sandbox.WaitForReady(ctx)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected WaitForReady to fail fast, got nil error")
	}

	if duration > 10*time.Second {
		t.Errorf("WaitForReady took %v, want < 10s", duration)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "failed to pull image") {
		t.Errorf("expected error to contain 'failed to pull image', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "ImagePullBackOff") {
		t.Errorf("expected error to contain 'ImagePullBackOff', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "manifest unknown") {
		t.Errorf("expected error to contain event message with 'manifest unknown', got: %s", errMsg)
	}
}

func TestWaitForReady_FailsFast_ErrImagePull_NoEvent(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-err-pull"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "sandbox",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ErrImagePull",
							Message: `rpc error: code = Unknown desc = repository not found`,
						},
					},
				},
			},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset(pod)

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	start := time.Now()
	err := sandbox.WaitForReady(ctx)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected WaitForReady to fail fast, got nil error")
	}

	if duration > 10*time.Second {
		t.Errorf("WaitForReady took %v, want < 10s", duration)
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "ErrImagePull") {
		t.Errorf("expected error to contain 'ErrImagePull', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "repository not found") {
		t.Errorf("expected error to contain 'repository not found', got: %s", errMsg)
	}
}

func TestWaitForReady_FailsFast_InitContainer_ImagePullBackOff(t *testing.T) {
	ctx := t.Context()
	ns := "test-ns"
	sbName := "sb-init-pull-error"

	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sbName,
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "init-service-portals",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: `Back-off pulling image "init-service-portals:latest"`,
						},
					},
				},
			},
		},
	}

	agentsCS := fakeagents.NewSimpleClientset()
	if _, err := agentsCS.AgentsV1beta1().Sandboxes(ns).Create(ctx, sb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create fake sandbox: %v", err)
	}
	coreCS := fakek8s.NewSimpleClientset(pod)

	client := &SandboxClient{
		agentsClient: agentsCS,
		coreClient:   coreCS.CoreV1(),
	}

	session := &Session{
		Name:   "test-session",
		client: client,
	}

	sandbox := &Sandbox{
		session: session,
		id: types.NamespacedName{
			Namespace: ns,
			Name:      sbName,
		},
	}

	err := sandbox.WaitForReady(ctx)
	if err == nil {
		t.Fatal("expected WaitForReady to fail fast for init container, got nil error")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "init-service-portals") {
		t.Errorf("expected error to mention container 'init-service-portals', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "ImagePullBackOff") {
		t.Errorf("expected error to contain 'ImagePullBackOff', got: %s", errMsg)
	}
}
