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
	"k8s.io/apimachinery/pkg/runtime"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	clientgotesting "k8s.io/client-go/testing"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"

	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/state"
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

func setupTestHarness(t *testing.T, procSrv *mockProcessServer) (*session.Harness, *fakeagents.Clientset, *fakek8s.Clientset) {
	t.Helper()

	agentsCS := fakeagents.NewSimpleClientset()
	// React to sandbox get by marking it Ready if it exists in the tracker
	agentsCS.PrependReactor("get", "sandboxes", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		getAction := action.(clientgotesting.GetAction)
		obj, err := agentsCS.Tracker().Get(action.GetResource(), getAction.GetNamespace(), getAction.GetName())
		if err != nil {
			return true, nil, err
		}
		sb := obj.(*sandboxv1beta1.Sandbox).DeepCopy()
		sb.Status.Conditions = []metav1.Condition{
			{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			},
		}
		return true, sb, nil
	})

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

	harness := session.NewHarness(nil, sandboxClient, nil, opts)
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

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch, err := NewOrchestrator(OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}

	if err := orch.Tick(ctx); err != nil {
		t.Fatalf("orch.Tick: %v", err)
	}

	// 1. Verify run is in Working phase
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}
	if gotRun.SandboxName != "run-20261030t120000z-abcd" {
		t.Errorf("expected sandbox name 'run-20261030t120000z-abcd', got %q", gotRun.SandboxName)
	}

	// 2. Verify sandbox still exists in Kubernetes (not deleted)
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected sandbox to exist in Kubernetes, got err: %v", err)
	}
	if sb.Name != gotRun.SandboxName {
		t.Errorf("expected sandbox name %s, got %s", gotRun.SandboxName, sb.Name)
	}

	// 3. Verify clone and checkout commands were issued directly without shell
	procSrv.mu.Lock()
	cmds := procSrv.commands
	procSrv.mu.Unlock()

	if len(cmds) != 2 {
		t.Fatalf("expected exactly 2 commands executed in sandbox (clone then checkout), got %d: %+v", len(cmds), cmds)
	}

	cloneCmdStr := strings.Join(cmds[0], " ")
	if cloneCmdStr != "git clone -- "+repo.URL+" /workspace/repo" {
		t.Errorf("expected clone command 'git clone -- %s /workspace/repo', got: %s", repo.URL, cloneCmdStr)
	}

	checkoutCmdStr := strings.Join(cmds[1], " ")
	if checkoutCmdStr != "git -C /workspace/repo checkout commit-base123" {
		t.Errorf("expected checkout command 'git -C /workspace/repo checkout commit-base123', got: %s", checkoutCmdStr)
	}

	// 4. Verify events.jsonl contains expected events
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var phases []state.RunPhase
	for _, e := range events {
		phases = append(phases, e.Phase)
	}
	if len(events) < 5 {
		t.Fatalf("expected at least 5 events (preparing phase, sandbox ready, clone command, checkout command, working phase), got %d: %+v", len(events), events)
	}
	if phases[0] != state.PhasePreparing || phases[len(phases)-1] != state.PhaseWorking {
		t.Errorf("unexpected event phases: %v", phases)
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

	if err := orch.Tick(ctx); err == nil {
		t.Logf("orch.Tick completed (errors logged or returned)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
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

func TestOrchestrator_Resume_PreparingToFailed(t *testing.T) {
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

	runID := "20261030T120000Z-interrupted"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "benchmark",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit-abc",
		SandboxName: "run-20261030t120000z-interrupted",
	}
	if err := store.PutRun(ctx, run); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	// Pre-create the sandbox in Kubernetes to verify cleanup on resume
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

	// 1. Verify run is marked Failed
	gotRun, err := store.GetRun(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "server restarted") {
		t.Errorf("expected error to explain restart, got: %s", gotRun.Error)
	}

	// 2. Verify sandbox was deleted
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted on resume, but it still exists", run.SandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// 3. Verify restart event
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least 1 event appended")
	}
	lastEvent := events[len(events)-1]
	if lastEvent.Phase != state.PhaseFailed || lastEvent.Kind != "restart" {
		t.Errorf("unexpected last event: %+v", lastEvent)
	}
}
