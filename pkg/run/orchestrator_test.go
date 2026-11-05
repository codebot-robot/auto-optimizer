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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"

	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

type mockProcessServer struct {
	processv1.UnimplementedProcessServiceServer

	mu       sync.Mutex
	commands [][]string
	exitCode int32
	stderr   string
	stdout   string
	err      error
}

func (m *mockProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if req.Config != nil {
		m.commands = append(m.commands, req.Config.Command)
	}
	if m.err != nil {
		return nil, m.err
	}

	return &processv1.ExecuteResponse{
		ExitCode: m.exitCode,
		Stdout:   []byte(m.stdout),
		Stderr:   []byte(m.stderr),
	}, nil
}

func setupTestHarness(t *testing.T, procSrv processv1.ProcessServiceServer) (*session.Harness, *fakeagents.Clientset, *fakek8s.Clientset) {
	return setupTestHarnessWithLLM(t, procSrv, nil)
}

func setupTestHarnessWithLLM(t *testing.T, procSrv processv1.ProcessServiceServer, llmClient llm.ChatClient) (*session.Harness, *fakeagents.Clientset, *fakek8s.Clientset) {
	t.Helper()

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	sandboxClient := session.NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	if procSrv != nil {
		lis := bufconn.Listen(1024 * 1024)
		grpcServer := grpc.NewServer()
		processv1.RegisterProcessServiceServer(grpcServer, procSrv)
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		t.Cleanup(func() {
			grpcServer.Stop()
			_ = lis.Close()
		})

		sandboxClient.SetProcessClientFactory(func(ctx context.Context, sb *session.Sandbox) (processv1.ProcessServiceClient, func(), error) {
			conn, err := grpc.NewClient("passthrough://bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return lis.Dial()
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				return nil, nil, err
			}
			return processv1.NewProcessServiceClient(conn), func() { _ = conn.Close() }, nil
		})
	}

	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	opts := session.RunOptions{
		Namespace:             "ao-sandboxes",
		ControlPlaneNamespace: "ao-system",
		SigningKey:            privKey,
	}

	var toolsRegistry *tools.Registry
	if llmClient != nil {
		toolsRegistry = tools.NewRegistry()
		toolsRegistry.Add(&tools.RunCommand{})
	}

	harness := session.NewHarness(llmClient, sandboxClient, toolsRegistry, opts)
	return harness, agentsCS, coreCS
}

func TestOrchestrator_PendingToWorking(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-abcd"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	const author = "auto-optimizer <auto-optimizer@users.noreply.github.com>"
	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: author,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending run.
	res, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID})
	if err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// 1. After the first step the run has a SandboxName, is in Preparing phase, and has ReadyDeadline set.
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}
	if gotRun.SandboxName != "run-20261030t120000z-abcd" {
		t.Errorf("expected sandbox name 'run-20261030t120000z-abcd', got %q", gotRun.SandboxName)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline to be set after step 1")
	}

	// 2. Verify fake Sandbox was created with the run label and repo annotation
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected sandbox to exist in Kubernetes, got err: %v", err)
	}
	if sb.Labels[LabelRun] != runID {
		t.Errorf("expected sandbox label %s=%q, got %q", LabelRun, runID, sb.Labels[LabelRun])
	}
	if sb.Annotations[AnnotationRepo] != repo.Key().String() {
		t.Errorf("expected sandbox annotation %s=%q, got %q", AnnotationRepo, repo.Key().String(), sb.Annotations[AnnotationRepo])
	}

	// 3. Worker is free: no clone or checkout commands have executed yet
	procSrv.mu.Lock()
	cmds := procSrv.commands
	procSrv.mu.Unlock()
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands executed in step 1, got %d: %+v", len(cmds), cmds)
	}

	// 4. Update the fake Sandbox's status to Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox to Ready: %v", err)
	}

	// Step 2: Second reconcile step executes clone and moves run to Working.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.ReadyDeadline != nil {
		t.Errorf("expected ReadyDeadline to be cleared in Working, got %v", gotRun.ReadyDeadline)
	}

	// 5. Verify sandbox still exists in Kubernetes (not deleted)
	sb, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected sandbox to exist in Kubernetes, got err: %v", err)
	}
	if sb.Name != gotRun.SandboxName {
		t.Errorf("expected sandbox name %s, got %s", gotRun.SandboxName, sb.Name)
	}

	// 6. Verify clone, checkout, and git config commands were issued directly without shell
	procSrv.mu.Lock()
	cmds = procSrv.commands
	procSrv.mu.Unlock()

	if len(cmds) != 4 {
		t.Fatalf("expected exactly 4 commands executed in sandbox (clone, checkout, 2 git config), got %d: %+v", len(cmds), cmds)
	}

	cloneCmdStr := strings.Join(cmds[0], " ")
	if cloneCmdStr != "git clone -- "+repo.URL+" /workspace/repo" {
		t.Errorf("expected clone command 'git clone -- %s /workspace/repo', got: %s", repo.URL, cloneCmdStr)
	}

	checkoutCmdStr := strings.Join(cmds[1], " ")
	if checkoutCmdStr != "git -C /workspace/repo checkout commit-base123" {
		t.Errorf("expected checkout command 'git -C /workspace/repo checkout commit-base123', got: %s", checkoutCmdStr)
	}

	configNameCmdStr := strings.Join(cmds[2], " ")
	if configNameCmdStr != "git -C /workspace/repo config user.name auto-optimizer" {
		t.Errorf("expected config user.name command 'git -C /workspace/repo config user.name auto-optimizer', got: %s", configNameCmdStr)
	}

	configEmailCmdStr := strings.Join(cmds[3], " ")
	if configEmailCmdStr != "git -C /workspace/repo config user.email auto-optimizer@users.noreply.github.com" {
		t.Errorf("expected config user.email command 'git -C /workspace/repo config user.email auto-optimizer@users.noreply.github.com', got: %s", configEmailCmdStr)
	}

	// 7. Verify events.jsonl contains expected events
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var phases []state.RunPhase
	var foundIdentityEvent bool
	for _, e := range events {
		phases = append(phases, e.Phase)
		if e.Kind == "git_identity_configured" && strings.Contains(e.Message, author) {
			foundIdentityEvent = true
		}
	}
	if len(events) < 6 {
		t.Fatalf("expected at least 6 events (preparing phase, sandbox ready, clone command, checkout command, git identity configured, working phase), got %d: %+v", len(events), events)
	}
	if !foundIdentityEvent {
		t.Errorf("expected git identity configured event in events: %+v", events)
	}
	if phases[0] != state.PhasePreparing || phases[len(phases)-1] != state.PhaseWorking {
		t.Errorf("unexpected event phases: %v", phases)
	}
}

func TestOrchestrator_ConfigureGitIdentityFailure(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-gitfail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "ok\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: "invalid-author<",
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}

	// Mark fake sandbox as Ready
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Reconcile Preparing run; clone/checkout succeed, but git identity fails.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("orch.Reconcile completed (errors handled)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "invalid author") {
		t.Errorf("expected error to mention 'invalid author', got: %s", gotRun.Error)
	}

	// 2. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but it still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 3. Verify events recorded failure event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFailureEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "invalid author") {
			foundFailureEvent = true
			break
		}
	}
	if !foundFailureEvent {
		t.Errorf("expected a PhaseFailed event mentioning invalid author in events.jsonl, got: %+v", events)
	}
}

func TestOrchestrator_CloneFailure(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-fail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "bad-commit-sha",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Simulate clone failure with exit code 128
	procSrv := &mockProcessServer{
		exitCode: 128,
		stderr:   "fatal: reference is not a tree: bad-commit-sha\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase %s after step 1, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Mark fake sandbox as Ready
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: reconcile Preparing run with Ready sandbox; clone fails.
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("orch.Reconcile completed (errors logged or returned)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "128") {
		t.Errorf("expected error to mention exit code 128, got: %s", gotRun.Error)
	}

	// 2. Verify sandbox was deleted from Kubernetes
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but it still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error for deleted sandbox, got: %v", err)
	}

	// 3. Verify events recorded command failure and transition to Failed
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var hasFailureEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed {
			hasFailureEvent = true
			break
		}
	}
	if !hasFailureEvent {
		t.Errorf("expected a PhaseFailed event in events.jsonl, got: %+v", events)
	}
}

func TestOrchestrator_Preparing_DeleteSandboxFailsRun(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-deleted"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile creates sandbox and moves to Preparing
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected Preparing phase, got %s", gotRun.Phase)
	}

	// Delete sandbox before readiness
	if err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, gotRun.SandboxName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete sandbox: %v", err)
	}

	// Step 2: Reconcile detects sandbox is gone and transitions run to Failed
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected Failed phase, got %s", gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "deleted before becoming ready") {
		t.Errorf("expected error to mention deleted before becoming ready, got: %q", gotRun.Error)
	}

	// Verify events contain failure event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var failureEventFound bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "deleted before becoming ready") {
			failureEventFound = true
			break
		}
	}
	if !failureEventFound {
		t.Errorf("expected failure event mentioning deleted before becoming ready in events: %+v", events)
	}
}

func TestOrchestrator_Preparing_NeverReadyDeadlinePasses(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-timeout"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-search",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	currentTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		Harness:      harness,
		Clock:        func() time.Time { return currentTime },
		ReadyTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Reconcile creates sandbox and sets ReadyDeadline = currentTime + 10s
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected Preparing phase, got %s", gotRun.Phase)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline to be recorded on run")
	}

	// Advance clock past the deadline
	currentTime = currentTime.Add(15 * time.Second)

	// Step 2: Reconcile when deadline has passed fails the run
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected Failed phase, got %s", gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "sandbox not ready before deadline") {
		t.Errorf("expected error to mention sandbox not ready before deadline, got: %q", gotRun.Error)
	}

	// Verify sandbox was cleaned up
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted after timeout, but it still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// Verify events contain failure event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var timeoutEventFound bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && strings.Contains(e.Message, "sandbox not ready before deadline") {
			timeoutEventFound = true
			break
		}
	}
	if !timeoutEventFound {
		t.Errorf("expected failure event mentioning sandbox not ready before deadline in events: %+v", events)
	}
}

func TestOrchestrator_Resume_PreparingEnqueued(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-preparing"
	deadline := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	run := &state.Run{
		ID:            runID,
		Repo:          repo.Key(),
		GoalName:      "benchmark",
		Phase:         state.PhasePreparing,
		BaseCommit:    "commit-abc",
		SandboxName:   "run-20261030t120000z-preparing",
		ReadyDeadline: &deadline,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	// Pre-create the sandbox in Kubernetes
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.SandboxName,
			Namespace: "ao-sandboxes",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 5, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// 1. Verify run remains in Preparing phase (not failed)
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// 2. Verify sandbox still exists
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Errorf("expected sandbox %s to exist, got: %v", run.SandboxName, err)
	}

	// 3. Verify notification was sent
	select {
	case evt := <-orch.notifyCh:
		if evt.Object.RunID != runID {
			t.Errorf("expected enqueued run %s, got %s", runID, evt.Object.RunID)
		}
	default:
		t.Error("expected Preparing run to be enqueued on Resume")
	}
}

func TestOrchestrator_Working_StartsSession_ModelStops_NoImprovement(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-worksess"
	sandboxName := "run-20261030t120000z-worksess"
	goalBody := "read the README and summarise it"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "readme-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-123",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "readme-goal",
			Body: goalBody,
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "# My Repo\nThis is a readme.\n",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	// Pre-create the sandbox in fake k8s
	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run is marked NoImprovement
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.SessionID == "" {
		t.Fatal("expected run.SessionID to be populated")
	}

	// 2. Verify events recorded "session ended without a pull request"
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundNoImpEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Message == "session ended without a pull request" {
			foundNoImpEvent = true
			break
		}
	}
	if !foundNoImpEvent {
		t.Errorf("expected event 'session ended without a pull request', got events: %+v", events)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 4. Verify session transcript in sessionStore contains the goal prompt, tool call, and tool result
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("expected session messages, got 0")
	}
	firstMsg := msgs[0]
	if firstMsg.Role != "system" {
		t.Errorf("first message role = %q, want system", firstMsg.Role)
	}
	if !strings.Contains(*firstMsg.Content, goalBody) {
		t.Errorf("expected system prompt to contain goal body %q, got %q", goalBody, *firstMsg.Content)
	}
	if !strings.Contains(*firstMsg.Content, repo.URL) || !strings.Contains(*firstMsg.Content, "/workspace/repo") {
		t.Errorf("expected system prompt to contain repo facts, got %q", *firstMsg.Content)
	}
	if !strings.Contains(*firstMsg.Content, "Never edit .goals/ or the metric script") {
		t.Errorf("expected system prompt to contain rules, got %q", *firstMsg.Content)
	}
}

type loopingLLM struct {
	mu        sync.Mutex
	callCount int
}

func (l *loopingLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	l.mu.Lock()
	l.callCount++
	callIdx := l.callCount
	l.mu.Unlock()

	args, _ := json.Marshal(map[string]string{"command": "echo loop"})
	return &llm.ChatCompletionResponse{
		ID: "looping-llm",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   fmt.Sprintf("loop-%d", callIdx),
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "run_command",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_MaxTurns(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-maxturns"
	sandboxName := "run-20261030t120000z-maxturns"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "loop-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-loop",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "loop-goal",
			Body: "run forever",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "loop\n",
	}
	mockLLM := &loopingLLM{}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, mockLLM)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     3,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify LLM stopped after exactly MaxTurns
	mockLLM.mu.Lock()
	calls := mockLLM.callCount
	mockLLM.mu.Unlock()
	if calls != 3 {
		t.Errorf("expected exactly 3 LLM calls before max turns stopped it, got %d", calls)
	}

	// 2. Verify run is marked NoImprovement
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}
}

func TestOrchestrator_Working_ExpiredSandbox_AtStart(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-expired"
	sandboxName := "run-20261030t120000z-expired"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "expired-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-exp",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
	// Sandbox is intentionally NOT created in fake k8s (already expired/deleted)

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("orch.Reconcile completed (errors handled)")
	}

	// Verify run transitioned to Failed
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "expired or deleted") && !strings.Contains(gotRun.Error, "not found") {
		t.Errorf("expected error to mention expired or deleted, got: %s", gotRun.Error)
	}

	// Verify failure event was recorded
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFailEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && (strings.Contains(e.Message, "expired or deleted") || strings.Contains(e.Message, "not found")) {
			foundFailEvent = true
			break
		}
	}
	if !foundFailEvent {
		t.Errorf("expected failure event mentioning expired or deleted sandbox, got events: %+v", events)
	}
}

type deleteSandboxLLM struct {
	agentsCS *fakeagents.Clientset
	sbName   string
}

func (d *deleteSandboxLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	// Simulate sandbox expiring/being deleted in k8s during turn
	_ = d.agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, d.sbName, metav1.DeleteOptions{})

	args, _ := json.Marshal(map[string]string{"command": "echo after deletion"})
	return &llm.ChatCompletionResponse{
		ID: "delete-llm",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   "call-after-del",
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "run_command",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_ExpiredSandbox_DuringSession(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-expduring"
	sandboxName := "run-20261030t120000z-expduring"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "expired-during",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-exp2",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{}
	delLLM := &deleteSandboxLLM{sbName: sandboxName}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, delLLM)
	delLLM.agentsCS = agentsCS

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err == nil {
		t.Logf("orch.Reconcile completed (errors handled)")
	}

	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "expired or was deleted") {
		t.Errorf("expected error to mention expired or deleted, got: %s", gotRun.Error)
	}
}

func TestOrchestrator_PendingToWorkingToNoImprovement(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-fullflow"
	goalBody := "read the README and summarise it"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "readme-goal",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
		Goal: &goals.Goal{
			Name: "readme-goal",
			Body: goalBody,
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "# My Repo\nThis is a readme.\n",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Step 1: Pending -> Preparing
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile 1 (step 1): %v", err)
	}

	// Mark sandbox Ready
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, "run-20261030t120000z-fullflow", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Preparing -> Working
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile 2 (step 2): %v", err)
	}

	// Step 3: Working -> NoImprovement
	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile 3: %v", err)
	}

	// 1. Verify run reached NoImprovement
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.SessionID == "" {
		t.Fatal("expected run.SessionID to be populated")
	}

	// 2. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted after session, but still exists", gotRun.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 3. Verify event recorded
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundNoImpEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Message == "session ended without a pull request" {
			foundNoImpEvent = true
			break
		}
	}
	if !foundNoImpEvent {
		t.Errorf("expected event 'session ended without a pull request', got events: %+v", events)
	}
}

type scriptedFinishLLM struct {
	summary string
}

func (s *scriptedFinishLLM) CreateChatCompletion(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	args, _ := json.Marshal(map[string]string{"summary": s.summary})
	return &llm.ChatCompletionResponse{
		ID: "finish-llm",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   "call-finish-1",
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "finish",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_FinishTool_EndsSession_RecordsSummary(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-finishrun"
	sandboxName := "run-20261030t120000z-finishrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "finish-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-finish",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "finish-goal",
			Body: "achieve optimization and finish",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	finishSummary := "Successfully investigated and finished with no PR needed."
	finishLLM := &scriptedFinishLLM{summary: finishSummary}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, finishLLM)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run is in NoImprovement and has the summary recorded
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.Summary != finishSummary {
		t.Errorf("expected run.Summary %q, got %q", finishSummary, gotRun.Summary)
	}

	// 2. Verify events recorded "finished" event with the summary as message
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFinishedEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Kind == "finished" && e.Message == finishSummary {
			foundFinishedEvent = true
			break
		}
	}
	if !foundFinishedEvent {
		t.Errorf("expected finished event with message %q, got events: %+v", finishSummary, events)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 4. Verify session transcript in sessionStore contains system prompt, user prompt, finish call, and tool result
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages in 1 turn, got %d: %+v", len(msgs), msgs)
	}
	if len(msgs[2].ToolCalls) == 0 || msgs[2].ToolCalls[0].Function.Name != "finish" {
		t.Errorf("tool call = %+v, want finish", msgs[2].ToolCalls)
	}
	if msgs[3].Role != "tool" || msgs[3].Content == nil || *msgs[3].Content != finishSummary {
		t.Errorf("tool message = %+v, want role tool and content %q", msgs[3], finishSummary)
	}
}

type scriptedErrorThenFinishLLM struct {
	mu        sync.Mutex
	callCount int
	summary   string
}

func (s *scriptedErrorThenFinishLLM) CreateChatCompletion(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	s.mu.Lock()
	s.callCount++
	count := s.callCount
	s.mu.Unlock()

	if count == 1 {
		args, _ := json.Marshal(map[string]string{"command": "failing-cmd"})
		return &llm.ChatCompletionResponse{
			ID: "err-llm-1",
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-err-1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
					FinishReason: "tool_calls",
				},
			},
		}, nil
	}

	// Verify we received the tool error in message history, then call finish
	var sawToolError bool
	for _, m := range req.Messages {
		if m.Role == "tool" && m.Content != nil && strings.Contains(*m.Content, "failed") {
			sawToolError = true
			break
		}
	}
	if !sawToolError {
		return nil, fmt.Errorf("expected tool error in messages on second turn, got: %+v", req.Messages)
	}

	args, _ := json.Marshal(map[string]string{"summary": s.summary})
	return &llm.ChatCompletionResponse{
		ID: "err-llm-2",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{
						{
							ID:   "call-finish-2",
							Type: "function",
							Function: llm.FunctionCall{
								Name:      "finish",
								Arguments: string(args),
							},
						},
					},
				},
				FinishReason: "tool_calls",
			},
		},
	}, nil
}

func TestOrchestrator_Working_OrdinaryToolError_ContinuesSession(t *testing.T) {
	ctx := t.Context()
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-errfinishrun"
	sandboxName := "run-20261030t120000z-errfinishrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "errfinish-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-errfinish",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "errfinish-goal",
			Body: "recover from error and finish",
		},
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	finishSummary := "Finished after recovering from command error."
	errLLM := &scriptedErrorThenFinishLLM{summary: finishSummary}
	procSrv := &mockProcessServer{err: errors.New("command execution failed")}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, errLLM)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if _, err := orch.Reconcile(ctx, Request{Repo: repo.Key(), RunID: runID}); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify session completed and run has the finish summary
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.Summary != finishSummary {
		t.Errorf("expected run.Summary %q, got %q", finishSummary, gotRun.Summary)
	}

	// 2. Verify events recorded "finished" event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFinishedEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseNoImprovement && e.Kind == "finished" && e.Message == finishSummary {
			foundFinishedEvent = true
			break
		}
	}
	if !foundFinishedEvent {
		t.Errorf("expected finished event with message %q, got events: %+v", finishSummary, events)
	}

	// 3. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 4. Verify session transcript in sessionStore contains 6 messages:
	// turn 1: system, user, call-err-1, tool error result
	// turn 2: call-finish-2, tool success result
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 6 {
		t.Fatalf("expected 6 messages in 2 turns, got %d: %+v", len(msgs), msgs)
	}
	if len(msgs[2].ToolCalls) == 0 || msgs[2].ToolCalls[0].Function.Name != "run_command" {
		t.Errorf("first tool call = %+v, want run_command", msgs[2].ToolCalls)
	}
	if msgs[3].Role != "tool" || msgs[3].Content == nil || !strings.Contains(*msgs[3].Content, "command execution failed") {
		t.Errorf("first tool result = %+v, want command execution failed", msgs[3])
	}
	if len(msgs[4].ToolCalls) == 0 || msgs[4].ToolCalls[0].Function.Name != "finish" {
		t.Errorf("second tool call = %+v, want finish", msgs[4].ToolCalls)
	}
	if msgs[5].Role != "tool" || msgs[5].Content == nil || *msgs[5].Content != finishSummary {
		t.Errorf("second tool result = %+v, want finishSummary", msgs[5])
	}
}

func TestOrchestrator_ReconciledAfterNotify_WithoutTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := state.NewFileStore(t.TempDir())

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	goalRec := &state.GoalRecord{
		BaseCommit: "commit-base123",
		Goal: &goals.Goal{
			Name: "optimize-latency",
			Body: "reduce latency by 10%",
		},
		Hash: "hash-v1",
	}
	if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
		t.Fatalf("PutGoal: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	scheduler, err := NewScheduler(SchedulerOptions{
		Store:        store,
		Orchestrator: orch,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	go func() {
		_ = mgr.Start(ctx)
	}()

	// Watch goroutine to mark created sandbox Ready, simulating agent-sandbox controller
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			runs, err := store.ListRuns(ctx, repo.Key())
			if err == nil && len(runs) > 0 && runs[0].SandboxName != "" {
				sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, runs[0].SandboxName, metav1.GetOptions{})
				if err == nil && len(sb.Status.Conditions) == 0 {
					sb.Status.Conditions = []metav1.Condition{
						{
							Type:   string(sandboxv1beta1.SandboxConditionReady),
							Status: metav1.ConditionTrue,
						},
					}
					_, _ = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{})
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// The scheduler creates the pending run and calls Orchestrator.Notify.
	// There is no orchestrator timer.
	if err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("scheduler.Tick: %v", err)
	}

	// Verify the run is reconciled to Working without any timer
	var reachedWorking bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := store.ListRuns(ctx, repo.Key())
		if err == nil && len(runs) > 0 {
			if runs[0].Phase == state.PhaseWorking {
				reachedWorking = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !reachedWorking {
		t.Fatal("expected run created by scheduler to reach Working after Notify without timer")
	}
}

type blockingConcurrentProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	onCommand func([]string)
}

func (b *blockingConcurrentProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	if req.Config != nil && b.onCommand != nil {
		b.onCommand(req.Config.Command)
	}
	return &processv1.ExecuteResponse{
		ExitCode: 0,
		Stdout:   []byte("ok"),
	}, nil
}

func TestOrchestrator_ConcurrentRuns_DifferentRepos(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	store := state.NewFileStore(t.TempDir())

	repo1 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo1",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo1",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo1); err != nil {
		t.Fatalf("PutRepo 1: %v", err)
	}

	repo2 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo2",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo2",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo2); err != nil {
		t.Fatalf("PutRepo 2: %v", err)
	}

	run1 := &state.Run{
		ID:         "20261030T120000Z-run1",
		Repo:       repo1.Key(),
		GoalName:   "goal1",
		Phase:      state.PhasePending,
		BaseCommit: "commit1",
	}
	if err := store.PutRun(ctx, run1); err != nil {
		t.Fatalf("PutRun 1: %v", err)
	}

	run2 := &state.Run{
		ID:         "20261030T120000Z-run2",
		Repo:       repo2.Key(),
		GoalName:   "goal2",
		Phase:      state.PhasePending,
		BaseCommit: "commit2",
	}
	if err := store.PutRun(ctx, run2); err != nil {
		t.Fatalf("PutRun 2: %v", err)
	}

	// Use channels to verify both runs execute their clone command concurrently.
	run1Started := make(chan struct{})
	run2Started := make(chan struct{})
	var closeOnce1, closeOnce2 sync.Once

	procSrv := &blockingConcurrentProcessServer{
		onCommand: func(cmd []string) {
			cmdStr := strings.Join(cmd, " ")
			if strings.Contains(cmdStr, "repo1") {
				closeOnce1.Do(func() { close(run1Started) })
				select {
				case <-run2Started:
				case <-time.After(5 * time.Second):
				}
			} else if strings.Contains(cmdStr, "repo2") {
				closeOnce2.Do(func() { close(run2Started) })
				select {
				case <-run1Started:
				case <-time.After(5 * time.Second):
				}
			}
		},
	}

	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			for _, sbName := range []string{"run-20261030t120000z-run1", "run-20261030t120000z-run2"} {
				sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{})
				if err == nil && len(sb.Status.Conditions) == 0 {
					sb.Status.Conditions = []metav1.Condition{
						{
							Type:   string(sandboxv1beta1.SandboxConditionReady),
							Status: metav1.ConditionTrue,
						},
					}
					_, _ = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{})
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:             store,
		Harness:           harness,
		MaxConcurrentRuns: 4,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		_ = mgr.Start(ctx)
	}()

	orch.Notify(repo1.Key(), run1.ID)
	orch.Notify(repo2.Key(), run2.ID)

	// Wait for both to reach Working
	deadline := time.Now().Add(5 * time.Second)
	var run1OK, run2OK bool
	for time.Now().Before(deadline) {
		r1, err1 := store.GetRun(ctx, repo1.Key(), run1.ID)
		r2, err2 := store.GetRun(ctx, repo2.Key(), run2.ID)
		if err1 == nil && r1.Phase == state.PhaseWorking {
			run1OK = true
		}
		if err2 == nil && r2.Phase == state.PhaseWorking {
			run2OK = true
		}
		if run1OK && run2OK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !run1OK || !run2OK {
		t.Fatalf("expected both runs to reach Working concurrently: run1OK=%v, run2OK=%v", run1OK, run2OK)
	}
}

type cancellingChatClient struct {
	cancelFn context.CancelFunc
}

func (c *cancellingChatClient) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	c.cancelFn()
	return nil, context.Canceled
}

func TestOrchestrator_ContextCancelled_LeavesWorking(t *testing.T) {
	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(t.Context(), repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-cancel"
	sandboxName := "run-20261030t120000z-cancel"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "optimize",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base123",
		SandboxName: sandboxName,
	}
	if err := store.PutRun(t.Context(), run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	testCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cancellingLLM := &cancellingChatClient{
		cancelFn: cancel,
	}

	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, cancellingLLM)

	// Create sandbox in fake k8s
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(t.Context(), &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Status: sandboxv1beta1.SandboxStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(sandboxv1beta1.SandboxConditionReady),
					Status: metav1.ConditionTrue,
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	// Reconcile with the cancellable context. The LLM will cancel the context during the session.
	_, _ = orch.Reconcile(testCtx, Request{Repo: repo.Key(), RunID: runID})

	// Check with test context: run MUST stay in Working phase
	gotRun, err := store.GetRun(t.Context(), repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s after context cancellation, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.Error != "" {
		t.Errorf("expected empty error on context cancellation, got %q", gotRun.Error)
	}

	// Verify no failure event was written
	events, err := store.ReadEvents(t.Context(), repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		if e.Phase == state.PhaseFailed {
			t.Errorf("unexpected failed event recorded on context cancellation: %+v", e)
		}
	}
}

func TestOrchestrator_WorkingRunEnqueuedOnStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)
	sessionStore := session.NewFileStore(storeDir)

	repo := &state.Repo{
		URL:           "https://github.com/gke-labs/test-repo",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "test-repo",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	workingRunID := "20261030T120000Z-working"
	workingRun := &state.Run{
		ID:          workingRunID,
		Repo:        repo.Key(),
		GoalName:    "task",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base",
		SandboxName: "run-working",
	}
	if err := store.PutRun(ctx, workingRun); err != nil {
		t.Fatalf("PutRun working: %v", err)
	}

	prepRunID := "20261030T120000Z-prep"
	prepRun := &state.Run{
		ID:          prepRunID,
		Repo:        repo.Key(),
		GoalName:    "task",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit-base",
		SandboxName: "run-prep",
	}
	if err := store.PutRun(ctx, prepRun); err != nil {
		t.Fatalf("PutRun prep: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "ok",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	// Create sandboxes in fake k8s
	for _, sbName := range []string{"run-working", "run-prep"} {
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sbName,
				Namespace: "ao-sandboxes",
			},
			Status: sandboxv1beta1.SandboxStatus{
				Conditions: []metav1.Condition{
					{
						Type:   string(sandboxv1beta1.SandboxConditionReady),
						Status: metav1.ConditionTrue,
					},
				},
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating sandbox %s: %v", sbName, err)
		}
	}

	mgr, err := NewManager(&rest.Config{Host: "http://127.0.0.1:65534"})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}

	go func() {
		_ = mgr.Start(ctx)
	}()

	// Wait for the Working run to be reconciled on start and reach NoImprovement
	deadline := time.Now().Add(5 * time.Second)
	var workingReconciled bool
	for time.Now().Before(deadline) {
		r, err := store.GetRun(ctx, repo.Key(), workingRunID)
		if err == nil && r.Phase == state.PhaseNoImprovement {
			workingReconciled = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !workingReconciled {
		t.Fatal("expected working run to be enqueued on start and reach NoImprovement")
	}

	// Verify Preparing run was enqueued on start and reached Working or NoImprovement
	var prepReconciled bool
	for time.Now().Before(deadline) {
		rPrep, err := store.GetRun(ctx, repo.Key(), prepRunID)
		if err == nil && (rPrep.Phase == state.PhaseWorking || rPrep.Phase == state.PhaseNoImprovement) {
			prepReconciled = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !prepReconciled {
		t.Fatal("expected preparing run to be enqueued on start and reach Working or NoImprovement")
	}
}

func TestBuildSystemPrompt(t *testing.T) {
	repo := &state.Repo{
		URL:           "https://github.com/example/repo",
		DefaultBranch: "main",
	}
	run := &state.Run{
		ID:         "20261104T120000Z-0001",
		GoalName:   "throughput",
		BaseCommit: "abcdef123456",
	}
	prompt := buildSystemPrompt("Base prompt", repo, run, "Improve throughput by 10%")

	expectedSubstrings := []string{
		"Base prompt",
		"# Goal",
		"Improve throughput by 10%",
		"# Repository Facts",
		"- URL: https://github.com/example/repo",
		"- Default Branch: main",
		"- Branch: ao/throughput/20261104T120000Z-0001",
		"- Base Commit: abcdef123456",
		"- Working Directory: /workspace/repo",
		"Commit your changes with git before calling open_pull_request; only commits on the branch are pushed, nothing uncommitted is included.",
		"# Rules",
	}
	for _, expected := range expectedSubstrings {
		if !strings.Contains(prompt, expected) {
			t.Errorf("buildSystemPrompt missing %q, got:\n%s", expected, prompt)
		}
	}
}

func TestOrchestrator_GitAuthor(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	const author = "auto-optimizer <auto-optimizer@users.noreply.github.com>"
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		GitAuthor: author,
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if orch.gitAuthor != author {
		t.Errorf("orch.gitAuthor = %q, want %q", orch.gitAuthor, author)
	}
}
