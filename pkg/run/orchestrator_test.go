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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/revlist"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	portalsv1 "github.com/gke-labs/service-portals/pkg/portals/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
	informers "sigs.k8s.io/agent-sandbox/clients/k8s/informers/externalversions"
	processv1 "sigs.k8s.io/agent-sandbox/packages/sandboxd/spec/process/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	crcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/forge"
	"github.com/gke-labs/auto-optimizer/pkg/goals"
	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/portal"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/sessioncontroller"
	"github.com/gke-labs/auto-optimizer/pkg/state"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	"github.com/gke-labs/auto-optimizer/pkg/toolsets/ao"
)

type mockProcessServer struct {
	processv1.UnimplementedProcessServiceServer

	mu              sync.Mutex
	commands        [][]string
	exitCode        int32
	stderr          string
	stdout          string
	err             error
	workspaceReused bool
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

	cmd := req.GetConfig().GetCommand()
	if len(cmd) >= 4 && cmd[0] == "git" && cmd[1] == "-C" && cmd[2] == "/workspace/repo" && cmd[3] == "rev-parse" {
		if m.workspaceReused {
			return &processv1.ExecuteResponse{
				ExitCode: 0,
				Stdout:   []byte(".git\n"),
			}, nil
		}
		if m.exitCode != 0 && m.exitCode != 128 {
			return &processv1.ExecuteResponse{
				ExitCode: m.exitCode,
				Stderr:   []byte(m.stderr),
			}, nil
		}
		return &processv1.ExecuteResponse{
			ExitCode: 128,
			Stderr:   []byte("fatal: not a git repository (or any of the parent directories): .git\n"),
		}, nil
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
	opts.InitDefaults()

	var toolsRegistry *tools.Registry
	if llmClient != nil {
		toolsRegistry = tools.NewRegistry()
		toolsRegistry.Add(&tools.RunCommand{})
	}

	harness := session.NewHarness(llmClient, sandboxClient, toolsRegistry, opts)
	return harness, agentsCS, coreCS
}

type testSessionState struct {
	store      session.Store
	reconciler *sessioncontroller.Reconciler
}

var (
	testSessionsMu sync.Mutex
	testSessions   = make(map[*Orchestrator]*testSessionState)
)

func testSessionInfo(orch *Orchestrator) *testSessionState {
	testSessionsMu.Lock()
	defer testSessionsMu.Unlock()
	return testSessions[orch]
}

func testSessionStore(orch *Orchestrator) session.Store {
	if ts := testSessionInfo(orch); ts != nil {
		return ts.store
	}
	return nil
}

func newTestFakeClientBuilder(scheme *apiruntime.Scheme) *fake.ClientBuilder {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if obj.GetUID() == "" {
					obj.SetUID(types.UID("test-uid-" + obj.GetName()))
				}
				return client.Create(ctx, obj, opts...)
			},
		})
}

func newTestOrchestrator(t *testing.T, opts OrchestratorOptions) *Orchestrator {
	t.Helper()
	if opts.MaxSessionDepth == 0 {
		opts.MaxSessionDepth = DefaultMaxSessionDepth
	}
	if opts.KubeClient == nil {
		scheme, err := NewScheme()
		if err != nil {
			t.Fatalf("NewScheme: %v", err)
		}
		opts.KubeClient = newTestFakeClientBuilder(scheme).Build()
	}
	orch, err := NewOrchestrator(opts)
	if err != nil {
		t.Fatalf("NewOrchestrator: %v", err)
	}
	if opts.SessionStore != nil && orch.harness != nil {
		sr, err := sessioncontroller.NewReconciler(sessioncontroller.Options{
			Client:          orch.kubeClient,
			SessionStore:    opts.SessionStore,
			StateStore:      orch.store,
			HarnessFactory:  func(string) (*session.Harness, error) { return orch.harness, nil },
			SandboxOwnerFor: orch.SandboxOwnerForSession,
			DefaultMaxTurns: orch.maxTurns,
			DefaultMaxDepth: orch.maxSessionDepth,
			Clock:           orch.clock,
		})
		if err != nil {
			t.Fatalf("sessioncontroller.NewReconciler: %v", err)
		}
		testSessionsMu.Lock()
		testSessions[orch] = &testSessionState{
			store:      opts.SessionStore,
			reconciler: sr,
		}
		testSessionsMu.Unlock()
		t.Cleanup(func() {
			testSessionsMu.Lock()
			delete(testSessions, orch)
			testSessionsMu.Unlock()
		})
	}
	return orch
}

func putTestHeroRun(t *testing.T, ctx context.Context, c client.Client, ns, repoURL string, run *state.Run) {
	t.Helper()
	name := sandboxNameForRun(run.ID)
	var existing herorunv1alpha1.HeroRun
	err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &existing)
	if k8serrors.IsNotFound(err) {
		hr := HeroRunFromRun(run)
		hr.Namespace = ns
		if repoURL != "" {
			hr.Spec.Repository = repoURL
		}
		status := hr.Status
		if err := c.Create(ctx, hr); err != nil {
			t.Fatalf("creating HeroRun %s: %v", name, err)
		}
		hr.Status = status
		if err := c.Status().Update(ctx, hr); err != nil {
			t.Fatalf("updating HeroRun status %s: %v", name, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("getting HeroRun %s: %v", name, err)
	}
	fresh := HeroRunFromRun(run)
	existing.Spec = fresh.Spec
	if repoURL != "" {
		existing.Spec.Repository = repoURL
	}
	if err := c.Update(ctx, &existing); err != nil {
		t.Fatalf("updating HeroRun %s: %v", name, err)
	}
	existing.Status = fresh.Status
	if err := c.Status().Update(ctx, &existing); err != nil {
		t.Fatalf("updating HeroRun status %s: %v", name, err)
	}
}

func putTestRun(t *testing.T, ctx context.Context, orch *Orchestrator, run *state.Run) {
	t.Helper()
	var repoURL string
	if orch.store != nil {
		if repo, err := orch.store.GetRepo(ctx, run.Repo); err == nil && repo != nil {
			repoURL = repo.URL
		}
	}
	putTestHeroRun(t, ctx, orch.kubeClient, orch.namespace(), repoURL, run)
}

func runRequest(orch *Orchestrator, repo state.RepoKey, runID string) reconcile.Request {
	name := sandboxNameForRun(runID)
	orch.recordRunIndex(name, repo, runID)
	var ns string
	if orch != nil && orch.harness != nil {
		ns = orch.harness.Namespace()
	}
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: ns,
			Name:      name,
		},
	}
}

func reconcileSessionUntilTerminal(ctx context.Context, orch *Orchestrator, sessName string) error {
	ts := testSessionInfo(orch)
	if ts == nil || ts.reconciler == nil {
		return errors.New("test orchestrator has no session reconciler configured")
	}
	ns := orch.namespace()
	sessReq := reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: sessName},
	}

	// Reconcile any child sessions created while the root session runs.
	childCtx, cancelChildren := context.WithCancel(ctx)
	var childWG sync.WaitGroup
	var activeMu sync.Mutex
	activeChildren := make(map[string]bool)
	childWG.Add(1)
	go func() {
		defer childWG.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-childCtx.Done():
				return
			case <-ticker.C:
				var list herorunv1alpha1.SessionList
				if err := orch.kubeClient.List(childCtx, &list, client.InNamespace(ns)); err != nil {
					continue
				}
				for i := range list.Items {
					s := &list.Items[i]
					if s.Name == sessName || s.Status.Phase.IsTerminal() {
						continue
					}
					activeMu.Lock()
					if activeChildren[s.Name] {
						activeMu.Unlock()
						continue
					}
					activeChildren[s.Name] = true
					activeMu.Unlock()
					childName := s.Name
					childWG.Add(1)
					go func() {
						defer childWG.Done()
						defer func() {
							activeMu.Lock()
							delete(activeChildren, childName)
							activeMu.Unlock()
						}()
						_, _ = ts.reconciler.Reconcile(childCtx, reconcile.Request{
							NamespacedName: types.NamespacedName{Namespace: ns, Name: childName},
						})
					}()
				}
			}
		}
	}()
	defer func() {
		cancelChildren()
		childWG.Wait()
	}()

	for {
		res, err := ts.reconciler.Reconcile(ctx, sessReq)
		if err != nil {
			return err
		}
		var sess herorunv1alpha1.Session
		if err := orch.kubeClient.Get(ctx, sessReq.NamespacedName, &sess); err != nil {
			return err
		}
		if sess.Status.Phase.IsTerminal() {
			return nil
		}
		if res.RequeueAfter > 0 {
			timer := time.NewTimer(min(res.RequeueAfter, 10*time.Millisecond))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if !res.Requeue {
			// Wait briefly for pending messages or child updates if session is waiting.
			if sess.Status.Phase == herorunv1alpha1.SessionPhaseWaiting {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			return nil
		}
	}
}

func reconcileWorkingRun(ctx context.Context, orch *Orchestrator, req reconcile.Request) (reconcile.Result, error) {
	// First pass creates the root Session CRD (or fails immediately if deadline passed).
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		return res, err
	}
	run, _, _, err := orch.getRunForReconcile(ctx, req)
	if err != nil {
		return res, err
	}
	if run == nil || run.Phase != state.PhaseWorking {
		return res, nil
	}
	sessName := topLevelSessionID(run.ID, run.SessionID)
	if err := reconcileSessionUntilTerminal(ctx, orch, sessName); err != nil {
		return reconcile.Result{}, err
	}
	// Second pass consumes the terminal Session outcome.
	return orch.Reconcile(ctx, req)
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

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	const author = "auto-optimizer <auto-optimizer@users.noreply.github.com>"
	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: author,
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Reconcile Pending run.
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// 1. After the first step the run has a SandboxName, is in Preparing phase, and has ReadyDeadline set.
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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

	// 6. Verify rev-parse, clone, checkout, and git config commands were issued directly without shell
	procSrv.mu.Lock()
	cmds = procSrv.commands
	procSrv.mu.Unlock()

	if len(cmds) != 5 {
		t.Fatalf("expected exactly 5 commands executed in sandbox (rev-parse, clone, checkout, 2 git config), got %d: %+v", len(cmds), cmds)
	}

	revParseCmdStr := strings.Join(cmds[0], " ")
	if revParseCmdStr != "git -C /workspace/repo rev-parse --git-dir" {
		t.Errorf("expected rev-parse command 'git -C /workspace/repo rev-parse --git-dir', got: %s", revParseCmdStr)
	}

	cloneCmdStr := strings.Join(cmds[1], " ")
	if cloneCmdStr != "git clone -- "+repo.URL+" /workspace/repo" {
		t.Errorf("expected clone command 'git clone -- %s /workspace/repo', got: %s", repo.URL, cloneCmdStr)
	}

	checkoutCmdStr := strings.Join(cmds[2], " ")
	if checkoutCmdStr != "git -C /workspace/repo checkout commit-base123" {
		t.Errorf("expected checkout command 'git -C /workspace/repo checkout commit-base123', got: %s", checkoutCmdStr)
	}

	configNameCmdStr := strings.Join(cmds[3], " ")
	if configNameCmdStr != "git -C /workspace/repo config user.name auto-optimizer" {
		t.Errorf("expected config user.name command 'git -C /workspace/repo config user.name auto-optimizer', got: %s", configNameCmdStr)
	}

	configEmailCmdStr := strings.Join(cmds[4], " ")
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

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "ok\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		Clock:     func() time.Time { return fixedTime },
		GitAuthor: "invalid-author<",
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
		t.Logf("orch.Reconcile completed (errors handled)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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

	// Simulate clone failure with exit code 128
	procSrv := &mockProcessServer{
		exitCode: 128,
		stderr:   "fatal: reference is not a tree: bad-commit-sha\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: reconcile Pending run to Preparing.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
		t.Logf("orch.Reconcile completed (errors logged or returned)")
	}

	// 1. Verify run is in Failed phase
	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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

func TestOrchestrator_WorkspaceReuse(t *testing.T) {
	setupReadyRun := func(t *testing.T, runID string, procSrv processv1.ProcessServiceServer) (*Orchestrator, state.Store, *fakeagents.Clientset, *state.Repo, *state.Run) {
		t.Helper()
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
		run := &state.Run{
			ID:         runID,
			Repo:       repo.Key(),
			GoalName:   "optimize",
			Phase:      state.PhasePending,
			BaseCommit: "base123",
		}
		harness, agentsCS, _ := setupTestHarness(t, procSrv)
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:     store,
			Harness:   harness,
			Clock:     func() time.Time { return time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC) },
			GitAuthor: "bot <bot@test.com>",
		})
		putTestRun(t, ctx, orch, run)
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("Reconcile step 1: %v", err)
		}
		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting sandbox: %v", err)
		}
		sb.Status.Conditions = []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionTrue}}
		if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating sandbox to Ready: %v", err)
		}
		return orch, store, agentsCS, repo, gotRun
	}

	t.Run("empty workspace clones and checks out", func(t *testing.T) {
		procSrv := &mockProcessServer{}
		orch, store, _, repo, run := setupReadyRun(t, "run-empty", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err != nil {
			t.Fatalf("Reconcile step 2: %v", err)
		}
		gotRun, err := GetRun(t.Context(), orch.kubeClient, orch.namespace(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseWorking {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseWorking)
		}
		procSrv.mu.Lock()
		cmds := procSrv.commands
		procSrv.mu.Unlock()
		if len(cmds) != 5 {
			t.Fatalf("expected 5 commands (rev-parse, clone, checkout, 2 git config), got %d: %+v", len(cmds), cmds)
		}
		if !strings.Contains(cmds[0][4], "--git-dir") || cmds[1][1] != "clone" || cmds[2][3] != "checkout" {
			t.Errorf("unexpected commands: %+v", cmds)
		}
		events, err := store.ReadEvents(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		for _, e := range events {
			if e.Kind == "workspace_reused" {
				t.Errorf("unexpected workspace_reused event in empty workspace")
			}
		}
	})

	t.Run("existing workspace skips clone and checkout", func(t *testing.T) {
		procSrv := &mockProcessServer{workspaceReused: true}
		orch, store, _, repo, run := setupReadyRun(t, "run-reused", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err != nil {
			t.Fatalf("Reconcile step 2: %v", err)
		}
		gotRun, err := GetRun(t.Context(), orch.kubeClient, orch.namespace(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseWorking {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseWorking)
		}
		procSrv.mu.Lock()
		cmds := procSrv.commands
		procSrv.mu.Unlock()
		if len(cmds) != 3 {
			t.Fatalf("expected 3 commands (rev-parse, 2 git config), got %d: %+v", len(cmds), cmds)
		}
		if !strings.Contains(cmds[0][4], "--git-dir") {
			t.Errorf("expected rev-parse check, got: %+v", cmds[0])
		}
		for _, cmd := range cmds[1:] {
			cmdStr := strings.Join(cmd, " ")
			if strings.Contains(cmdStr, "clone") || strings.Contains(cmdStr, "checkout") {
				t.Errorf("clone or checkout should have been skipped, got command: %s", cmdStr)
			}
		}
		events, err := store.ReadEvents(t.Context(), repo.Key(), run.ID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		var foundReused bool
		for _, e := range events {
			if e.Kind == "workspace_reused" {
				foundReused = true
				if m, ok := e.Data.(map[string]any); !ok || m["sandbox"] != run.SandboxName {
					t.Errorf("expected sandbox name in event data, got: %+v", e.Data)
				}
			}
		}
		if !foundReused {
			t.Errorf("expected workspace_reused event, found none")
		}
	})

	t.Run("prepareSandbox is safe to call again on the same sandbox", func(t *testing.T) {
		procSrv := &mockProcessServer{}
		orch, _, _, repo, run := setupReadyRun(t, "run-prepare-twice", procSrv)
		sb := orch.harness.GetOrAttachSandbox(run.SandboxName)
		if sb == nil {
			t.Fatalf("expected sandbox handle, got nil")
		}

		if err := orch.prepareSandbox(t.Context(), repo, run, sb); err != nil {
			t.Fatalf("first prepareSandbox: %v", err)
		}

		// Simulate the workspace now being populated
		procSrv.mu.Lock()
		procSrv.workspaceReused = true
		procSrv.commands = nil
		procSrv.mu.Unlock()

		if err := orch.prepareSandbox(t.Context(), repo, run, sb); err != nil {
			t.Fatalf("second prepareSandbox: %v", err)
		}

		procSrv.mu.Lock()
		cmds := procSrv.commands
		procSrv.mu.Unlock()
		if len(cmds) != 3 {
			t.Fatalf("expected 3 commands on second call (rev-parse, 2 git config), got %d: %+v", len(cmds), cmds)
		}
		for _, cmd := range cmds[1:] {
			cmdStr := strings.Join(cmd, " ")
			if strings.Contains(cmdStr, "clone") || strings.Contains(cmdStr, "checkout") {
				t.Errorf("clone or checkout should have been skipped on second call, got command: %s", cmdStr)
			}
		}
	})

	t.Run("check failure fails step", func(t *testing.T) {
		procSrv := &mockProcessServer{err: errors.New("sandbox process error")}
		orch, _, agentsCS, repo, run := setupReadyRun(t, "run-checkfail", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err == nil {
			t.Logf("Reconcile completed")
		}
		gotRun, err := GetRun(t.Context(), orch.kubeClient, orch.namespace(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseFailed)
		}
		if !strings.Contains(gotRun.Error, "sandbox process error") {
			t.Errorf("expected error to mention failure, got: %s", gotRun.Error)
		}
		_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(t.Context(), run.SandboxName, metav1.GetOptions{})
		if err == nil || !k8serrors.IsNotFound(err) {
			t.Errorf("expected sandbox to be deleted, got err: %v", err)
		}
	})

	t.Run("check exit code failure fails step", func(t *testing.T) {
		procSrv := &mockProcessServer{exitCode: 1, stderr: "fatal: git check failure\n"}
		orch, _, agentsCS, repo, run := setupReadyRun(t, "run-checkexitfail", procSrv)
		if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), run.ID)); err == nil {
			t.Logf("Reconcile completed")
		}
		gotRun, err := GetRun(t.Context(), orch.kubeClient, orch.namespace(), run.ID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("phase = %s, want %s", gotRun.Phase, state.PhaseFailed)
		}
		if !strings.Contains(gotRun.Error, "exit code 1") {
			t.Errorf("expected error to mention exit code 1, got: %s", gotRun.Error)
		}
		_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(t.Context(), run.SandboxName, metav1.GetOptions{})
		if err == nil || !k8serrors.IsNotFound(err) {
			t.Errorf("expected sandbox to be deleted, got err: %v", err)
		}
	})
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

	harness, agentsCS, _ := setupTestHarness(t, nil)

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Reconcile creates sandbox and moves to Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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

	harness, agentsCS, _ := setupTestHarness(t, nil)

	currentTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		Harness:      harness,
		Clock:        func() time.Time { return currentTime },
		ReadyTimeout: 10 * time.Second,
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Reconcile creates sandbox and sets ReadyDeadline = currentTime + 10s
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile step 2: %v", err)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
		Clock:   func() time.Time { return fixedTime },
	})
	putTestRun(t, ctx, orch, run)

	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// 1. Verify run remains in Preparing phase (not failed)
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
		if evt.Object.Name != sandboxNameForRun(runID) {
			t.Errorf("expected enqueued run name %s, got %s", sandboxNameForRun(runID), evt.Object.Name)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify run is marked NoImprovement
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	msgs, meta, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if meta == nil || meta.Repo != repo.Key().String() || meta.RunID != gotRun.ID {
		t.Errorf("session metadata = %+v, want Repo %q and RunID %q", meta, repo.Key().String(), gotRun.ID)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     3,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify LLM stopped after exactly MaxTurns
	mockLLM.mu.Lock()
	calls := mockLLM.callCount
	mockLLM.mu.Unlock()
	if calls != 3 {
		t.Errorf("expected exactly 3 LLM calls before max turns stopped it, got %d", calls)
	}

	// 2. Verify run is marked NoImprovement
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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

type multiCallLLM struct {
	mu            sync.Mutex
	callCount     int
	textResponses int
}

func (l *multiCallLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.callCount++
	callIdx := l.callCount

	if l.textResponses > 0 && callIdx <= l.textResponses {
		text := fmt.Sprintf("text reply %d", callIdx)
		return &llm.ChatCompletionResponse{
			ID: fmt.Sprintf("resp-%d", callIdx),
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role:    "assistant",
						Content: &text,
					},
					FinishReason: "stop",
				},
			},
		}, nil
	}

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

	harness, _, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
	// Sandbox is intentionally NOT created in fake k8s (already expired/deleted).
	// A run gets its Sandbox on demand; a missing sandbox does not fail the run.

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// Verify run carried on to NoImprovement
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		if e.Kind == "sandbox_expired" {
			t.Errorf("unexpected sandbox_expired event: %+v", e)
		}
	}
}

type deleteSandboxLLM struct {
	agentsCS *fakeagents.Clientset
	sbName   string
	called   bool
}

func (d *deleteSandboxLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	if !d.called {
		d.called = true
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

	done := "done after tool"
	return &llm.ChatCompletionResponse{
		ID: "delete-llm-done",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role:    "assistant",
					Content: &done,
				},
				FinishReason: "stop",
			},
		},
	}, nil
}

type failingOnFirstCallServer struct {
	processv1.UnimplementedProcessServiceServer
	mu         sync.Mutex
	failedOnce bool
}

func (f *failingOnFirstCallServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := req.GetConfig().GetCommand()
	if len(cmd) >= 4 && cmd[0] == "git" && cmd[1] == "-C" && cmd[2] == "/workspace/repo" && cmd[3] == "rev-parse" {
		return &processv1.ExecuteResponse{ExitCode: 0, Stdout: []byte(".git\n")}, nil
	}
	if !f.failedOnce {
		f.failedOnce = true
		return nil, status.Error(codes.Unavailable, "sandbox connection lost")
	}
	return &processv1.ExecuteResponse{ExitCode: 0, Stdout: []byte("after deletion\n")}, nil
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
	claimName := "claim-expduring"
	run := &state.Run{
		ID:                 runID,
		Repo:               repo.Key(),
		GoalName:           "expired-during",
		Phase:              state.PhaseWorking,
		BaseCommit:         "base-sha-exp2",
		SandboxName:        sandboxName,
		WorkspaceClaimName: claimName,
	}

	procSrv := &failingOnFirstCallServer{}
	delLLM := &deleteSandboxLLM{sbName: sandboxName}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, delLLM)
	delLLM.agentsCS = agentsCS

	var createdSandbox *sandboxv1beta1.Sandbox
	agentsCS.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		sb := createAction.GetObject().(*sandboxv1beta1.Sandbox).DeepCopy()
		createdSandbox = sb
		sb.Status.Conditions = []metav1.Condition{
			{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			},
		}
		_ = agentsCS.Tracker().Create(action.GetResource(), sb, action.GetNamespace())
		return true, sb, nil
	})

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
			Labels:    map[string]string{LabelRun: runID},
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
				ShutdownPolicy: &policy,
			},
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
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s (error: %s)", state.PhaseNoImprovement, gotRun.Phase, gotRun.Error)
	}

	// Verify new sandbox was created with the same claim
	if createdSandbox == nil {
		t.Fatal("expected new sandbox to be created on demand")
	}
	var foundClaim bool
	for _, v := range createdSandbox.Spec.PodTemplate.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claimName {
			foundClaim = true
			break
		}
	}
	if !foundClaim {
		t.Errorf("expected created sandbox to mount %s, got volumes: %+v", claimName, createdSandbox.Spec.PodTemplate.Spec.Volumes)
	}
}

type midToolDeletingServer struct {
	processv1.UnimplementedProcessServiceServer
	mu          sync.Mutex
	agentsCS    *fakeagents.Clientset
	sbName      string
	deletedOnce bool
	retriedOnce bool
}

func (m *midToolDeletingServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := req.GetConfig().GetCommand()
	if len(cmd) >= 4 && cmd[0] == "git" && cmd[1] == "-C" && cmd[2] == "/workspace/repo" && cmd[3] == "rev-parse" {
		return &processv1.ExecuteResponse{
			ExitCode: 0,
			Stdout:   []byte(".git\n"),
		}, nil
	}

	for _, c := range cmd {
		if c == "interrupted-tool" {
			if !m.deletedOnce {
				m.deletedOnce = true
				_ = m.agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, m.sbName, metav1.DeleteOptions{})
				return nil, status.Error(codes.Unavailable, "sandbox container died")
			}
			m.retriedOnce = true
			return &processv1.ExecuteResponse{
				ExitCode: 0,
				Stdout:   []byte("success on retry\n"),
			}, nil
		}
	}

	return &processv1.ExecuteResponse{ExitCode: 0}, nil
}

type toolCallingThenStopLLM struct {
	called bool
}

func (t *toolCallingThenStopLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	if !t.called {
		t.called = true
		args, _ := json.Marshal(map[string]string{"command": "interrupted-tool"})
		return &llm.ChatCompletionResponse{
			ID: "tool-llm",
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-interrupted",
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

	done := "finished successfully"
	return &llm.ChatCompletionResponse{
		ID: "tool-llm-done",
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role:    "assistant",
					Content: &done,
				},
				FinishReason: "stop",
			},
		},
	}, nil
}

func TestOrchestrator_Working_ToolCallInterruptedBySandboxDeletion_RetriedAndFinishes(t *testing.T) {
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

	runID := "20261030T120000Z-toolinterrupted"
	sandboxName := "run-toolinterrupted"
	claimName := "claim-toolinterrupted"
	deadline := time.Date(2026, 10, 30, 13, 0, 0, 0, time.UTC)
	run := &state.Run{
		ID:                 runID,
		Repo:               repo.Key(),
		GoalName:           "tool-interrupted-goal",
		Phase:              state.PhaseWorking,
		BaseCommit:         "base-sha-tool",
		SandboxName:        sandboxName,
		WorkspaceClaimName: claimName,
		Deadline:           &deadline,
	}

	procSrv := &midToolDeletingServer{sbName: sandboxName}
	toolLLM := &toolCallingThenStopLLM{}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, toolLLM)
	procSrv.agentsCS = agentsCS

	var createdSandbox *sandboxv1beta1.Sandbox
	agentsCS.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		sb := createAction.GetObject().(*sandboxv1beta1.Sandbox).DeepCopy()
		createdSandbox = sb
		sb.Status.Conditions = []metav1.Condition{
			{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			},
		}
		_ = agentsCS.Tracker().Create(action.GetResource(), sb, action.GetNamespace())
		return true, sb, nil
	})

	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
			Labels:    map[string]string{LabelRun: runID},
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: deadline.Add(SandboxShutdownGrace)},
				ShutdownPolicy: &policy,
			},
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{
							{
								Name: "workspace",
								VolumeSource: corev1.VolumeSource{
									PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
										ClaimName: claimName,
									},
								},
							},
						},
					},
				},
			},
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
		t.Fatalf("creating initial sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s (error: %s)", state.PhaseNoImprovement, gotRun.Phase, gotRun.Error)
	}

	if !procSrv.deletedOnce {
		t.Error("expected sandbox to be deleted once during mid-tool execution")
	}
	if !procSrv.retriedOnce {
		t.Error("expected tool call to be retried after sandbox deletion")
	}

	// Verify new sandbox was created with the same workspace claim
	if createdSandbox == nil {
		t.Fatal("expected new sandbox to be created on demand")
	}
	var foundClaim bool
	for _, v := range createdSandbox.Spec.PodTemplate.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claimName {
			foundClaim = true
			break
		}
	}
	if !foundClaim {
		t.Errorf("expected created sandbox to mount %s, got volumes: %+v", claimName, createdSandbox.Spec.PodTemplate.Spec.Volumes)
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

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "# My Repo\nThis is a readme.\n",
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, &llm.ElizaClient{})

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Pending -> Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
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
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile 2 (step 2): %v", err)
	}

	// Step 3: Working -> NoImprovement
	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun 3: %v", err)
	}

	// 1. Verify run reached NoImprovement
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify run is in NoImprovement and has the summary recorded
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	// 1. Verify session completed and run has the finish summary
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
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

type reconcileDriver struct {
	t      *testing.T
	orch   *Orchestrator
	queue  workqueue.TypedRateLimitingInterface[reconcile.Request]
	ctx    context.Context
	cancel context.CancelFunc
}

func startReconcileDriver(t *testing.T, orch *Orchestrator) *reconcileDriver {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	queue := workqueue.NewTypedRateLimitingQueue[reconcile.Request](
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
	)

	d := &reconcileDriver{
		t:      t,
		orch:   orch,
		queue:  queue,
		ctx:    ctx,
		cancel: cancel,
	}

	var workersWG sync.WaitGroup
	workers := orch.maxConcurrentRuns
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			d.worker()
		}()
	}

	t.Cleanup(func() {
		cancel()
		queue.ShutDown()
		workersWG.Wait()
	})

	go func() {
		_ = orch.Start(ctx)
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-orch.notifyCh:
				queue.Add(ev.Object)
			}
		}
	}()

	if ts := testSessionInfo(orch); ts != nil && ts.reconciler != nil && orch.harness != nil && orch.harness.LLMClient() != nil {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			var activeMu sync.Mutex
			active := make(map[string]bool)
			enqueuedTerminal := make(map[string]bool)
			var sessWG sync.WaitGroup
			defer sessWG.Wait()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			ns := orch.namespace()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					var list herorunv1alpha1.SessionList
					if err := orch.kubeClient.List(ctx, &list, client.InNamespace(ns)); err != nil {
						continue
					}
					for i := range list.Items {
						s := &list.Items[i]
						if s.Status.Phase.IsTerminal() {
							activeMu.Lock()
							done := enqueuedTerminal[s.Name]
							if !done {
								enqueuedTerminal[s.Name] = true
							}
							activeMu.Unlock()
							if !done {
								if owner := metav1.GetControllerOf(s); owner != nil && owner.Kind == "HeroRun" {
									queue.Add(reconcile.Request{
										NamespacedName: types.NamespacedName{Namespace: s.Namespace, Name: owner.Name},
									})
								}
							}
							continue
						}
						activeMu.Lock()
						if active[s.Name] {
							activeMu.Unlock()
							continue
						}
						active[s.Name] = true
						activeMu.Unlock()
						sessName := s.Name
						owner := metav1.GetControllerOf(s)
						sessWG.Add(1)
						go func() {
							defer sessWG.Done()
							defer func() {
								activeMu.Lock()
								delete(active, sessName)
								activeMu.Unlock()
							}()
							_, _ = ts.reconciler.Reconcile(ctx, reconcile.Request{
								NamespacedName: types.NamespacedName{Namespace: ns, Name: sessName},
							})
							if owner != nil && owner.Kind == "HeroRun" {
								var updated herorunv1alpha1.Session
								if err := orch.kubeClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: sessName}, &updated); err == nil && updated.Status.Phase.IsTerminal() {
									queue.Add(reconcile.Request{
										NamespacedName: types.NamespacedName{Namespace: ns, Name: owner.Name},
									})
								}
							}
						}()
					}
				}
			}
		}()
	}

	if orch.harness != nil && orch.harness.SandboxClient() != nil && orch.harness.SandboxClient().AgentsClient() != nil {
		factory := informers.NewSharedInformerFactoryWithOptions(
			orch.harness.SandboxClient().AgentsClient(),
			0,
			informers.WithNamespace(orch.harness.Namespace()),
		)
		informer := factory.Agents().V1beta1().Sandboxes().Informer()
		enqueueSandbox := func(obj any) {
			sb, ok := obj.(*sandboxv1beta1.Sandbox)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				sb, ok = tombstone.Obj.(*sandboxv1beta1.Sandbox)
				if !ok || sb == nil {
					return
				}
			}

			owner := metav1.GetControllerOf(sb)
			if owner == nil || owner.Kind != "HeroRun" {
				return
			}

			queue.Add(reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: sb.Namespace,
					Name:      owner.Name,
				},
			})
		}

		_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    enqueueSandbox,
			UpdateFunc: func(_, newObj any) { enqueueSandbox(newObj) },
			DeleteFunc: enqueueSandbox,
		})
		if err != nil {
			t.Fatalf("adding event handler to sandbox informer: %v", err)
		}

		factory.StartWithContext(ctx)
		if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
			if ctx.Err() == nil {
				t.Fatal("timed out waiting for sandbox informer caches to sync")
			}
		}
	}

	return d
}

func (d *reconcileDriver) worker() {
	for {
		req, shutdown := d.queue.Get()
		if shutdown {
			return
		}
		res, err := d.orch.Reconcile(d.ctx, req)
		if err != nil {
			d.queue.AddRateLimited(req)
		} else if res.RequeueAfter > 0 {
			d.queue.Forget(req)
			d.queue.AddAfter(req, res.RequeueAfter)
		} else if res.Requeue {
			d.queue.AddRateLimited(req)
		} else {
			d.queue.Forget(req)
		}
		d.queue.Done(req)
	}
}

func (d *reconcileDriver) runUntil(cond func() bool) {
	d.t.Helper()
	if cond() {
		return
	}

	deadline := time.Now().Add(10 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cond() {
				return
			}
			if time.Now().After(deadline) {
				d.t.Fatal("timed out waiting for condition in runUntil")
			}
		case <-d.ctx.Done():
			d.t.Fatalf("context cancelled while waiting for condition in runUntil: %v", d.ctx.Err())
		}
	}
}

func TestOrchestrator_ReconciledAfterNotify_WithoutTimer(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)

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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	scheduler, err := NewScheduler(SchedulerOptions{
		Store:        store,
		Orchestrator: orch,
	})
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	d := startReconcileDriver(t, orch)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			runs, err := ListRuns(ctx, orch.kubeClient, orch.namespace(), repo.Key())
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
	d.runUntil(func() bool {
		runs, err := ListRuns(ctx, orch.kubeClient, orch.namespace(), repo.Key())
		return err == nil && len(runs) > 0 && runs[0].Phase == state.PhaseWorking
	})
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
	ctx := t.Context()

	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)

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

	run2 := &state.Run{
		ID:         "20261030T120000Z-run2",
		Repo:       repo2.Key(),
		GoalName:   "goal2",
		Phase:      state.PhasePending,
		BaseCommit: "commit2",
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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:             store,
		Harness:           harness,
		KubeClient:        fakeClient,
		MaxConcurrentRuns: 4,
	})
	putTestRun(t, ctx, orch, run1)
	putTestRun(t, ctx, orch, run2)

	d := startReconcileDriver(t, orch)

	orch.Notify(repo1.Key(), run1.ID)
	orch.Notify(repo2.Key(), run2.ID)

	// Wait for both to reach Working
	d.runUntil(func() bool {
		r1, err1 := GetRun(ctx, orch.kubeClient, orch.namespace(), run1.ID)
		r2, err2 := GetRun(ctx, orch.kubeClient, orch.namespace(), run2.ID)
		return err1 == nil && r1.Phase == state.PhaseWorking &&
			err2 == nil && r2.Phase == state.PhaseWorking
	})
}

type cancellingChatClient struct {
	cancelFn context.CancelFunc
}

func (c *cancellingChatClient) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	c.cancelFn()
	return nil, context.Canceled
}

type failingChatClient struct{}

func (f *failingChatClient) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	return nil, errors.New("simulated LLM failure")
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	putTestRun(t, t.Context(), orch, run)

	// Reconcile HeroRun to create root Session CRD, then reconcile Session with the cancellable context.
	// The LLM cancels testCtx mid-turn, simulating process shutdown before Session reaches terminal status.
	if _, err := orch.Reconcile(testCtx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}
	sessName := topLevelSessionID(runID, "")
	ts := testSessionInfo(orch)
	_, _ = ts.reconciler.Reconcile(testCtx, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: orch.namespace(), Name: sessName},
	})

	// Check with test context: run MUST stay in Working phase
	gotRun, err := GetRun(t.Context(), orch.kubeClient, orch.namespace(), runID)
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

	// Reconciling HeroRun after shutdown cancellation must also leave the run in Working
	if _, err := orch.Reconcile(t.Context(), runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile after cancellation: %v", err)
	}

	// Restart: create a new Orchestrator with the scripted finish LLM and verify run finishes normally.
	finishLLM := &scriptedFinishLLM{summary: "restarted and finished"}
	restartedHarness, newAgentsCS, _ := setupTestHarnessWithLLM(t, nil, finishLLM)
	_, err = newAgentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(t.Context(), &sandboxv1beta1.Sandbox{
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
		t.Fatalf("creating sandbox in fake k8s on restart: %v", err)
	}

	restartedOrch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      restartedHarness,
		KubeClient:   orch.kubeClient,
	})
	if _, err := reconcileWorkingRun(t.Context(), restartedOrch, runRequest(restartedOrch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun on restart: %v", err)
	}

	resRun, err := GetRun(t.Context(), restartedOrch.kubeClient, restartedOrch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun on restart: %v", err)
	}
	if resRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s after restart, got %s", state.PhaseNoImprovement, resRun.Phase)
	}
	if resRun.Summary != "restarted and finished" {
		t.Errorf("expected summary 'restarted and finished', got %q", resRun.Summary)
	}
}

func TestOrchestrator_Working_UnansweredToolCall_ContinuesWithoutUserMessage(t *testing.T) {
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

	runID := "20261030T120000Z-unans"
	sandboxName := "run-20261030t120000z-unans"
	sessionID := generateSessionID(runID)
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "unanswered-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-unans",
		SandboxName: sandboxName,
		SessionID:   sessionID,
		Goal: &goals.Goal{
			Name: "unanswered-goal",
			Body: "run command and finish",
		},
	}

	sysPrompt := "system prompt"
	userPrompt := "Please work on the goal."
	if err := sessionStore.NewSession(ctx, sessionID, session.Metadata{}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sessionStore.AppendMessages(ctx, sessionID,
		llm.Message{Role: "system", Content: &sysPrompt},
		llm.Message{Role: "user", Content: &userPrompt},
		llm.Message{
			Role: "assistant",
			ToolCalls: []llm.ToolCall{
				{
					ID:   "call-unans-1",
					Type: "function",
					Function: llm.FunctionCall{
						Name:      "run_command",
						Arguments: `{"command":"uname -a"}`,
					},
				},
			},
		},
	); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Linux unans 6.12.0\n",
	}
	finishSummary := "Unanswered tool executed and finished."
	finishLLM := &scriptedFinishLLM{summary: finishSummary}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, finishLLM)

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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	procSrv.mu.Lock()
	cmds := append([][]string(nil), procSrv.commands...)
	procSrv.mu.Unlock()
	if len(cmds) != 1 {
		t.Fatalf("expected 1 command executed in sandbox, got %d: %v", len(cmds), cmds)
	}

	msgs, _, err := sessionStore.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	userCount := 0
	for _, m := range msgs {
		if m.Role == "user" {
			userCount++
		}
	}
	if userCount != 1 {
		t.Errorf("expected exactly 1 user message in transcript, got %d", userCount)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}
	if gotRun.Summary != finishSummary {
		t.Errorf("expected summary %q, got %q", finishSummary, gotRun.Summary)
	}
}

func TestOrchestrator_Working_SystemPromptOnly_RunsTurnWithGoal(t *testing.T) {
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

	runID := "20261030T120000Z-sysonly"
	sandboxName := "run-20261030t120000z-sysonly"
	sessionID := generateSessionID(runID)
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "sysonly-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha-sysonly",
		SandboxName: sandboxName,
		SessionID:   sessionID,
		Goal: &goals.Goal{
			Name: "sysonly-goal",
			Body: "goal body",
		},
	}

	// Seed session transcript with ONLY a system prompt (no user message)
	sysPrompt := "system prompt"
	if err := sessionStore.NewSession(ctx, sessionID, session.Metadata{}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := sessionStore.AppendMessages(ctx, sessionID, llm.Message{Role: "system", Content: &sysPrompt}); err != nil {
		t.Fatalf("AppendMessages: %v", err)
	}

	finishLLM := &scriptedFinishLLM{summary: "started from system prompt only"}
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	putTestRun(t, ctx, orch, run)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	msgs, _, err := sessionStore.LoadSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	var userMsgs []llm.Message
	for _, m := range msgs {
		if m.Role == "user" {
			userMsgs = append(userMsgs, m)
		}
	}
	if len(userMsgs) != 1 {
		t.Fatalf("expected exactly 1 user message, got %d", len(userMsgs))
	}
	if userMsgs[0].Content == nil || *userMsgs[0].Content != "Please work on the goal." {
		t.Errorf("expected user message %q, got %v", "Please work on the goal.", userMsgs[0].Content)
	}
}

func TestOrchestrator_WorkingRunEnqueuedOnStart(t *testing.T) {
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

	workingRunID := "20261030T120000Z-working"
	workingRun := &state.Run{
		ID:          workingRunID,
		Repo:        repo.Key(),
		GoalName:    "task",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base",
		SandboxName: "run-working",
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

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
	})
	putTestRun(t, ctx, orch, workingRun)
	putTestRun(t, ctx, orch, prepRun)

	d := startReconcileDriver(t, orch)

	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), workingRunID)
		return err == nil && r.Phase == state.PhaseNoImprovement
	})

	// Verify Preparing run was enqueued on start and reached Working or NoImprovement
	d.runUntil(func() bool {
		rPrep, err := GetRun(ctx, orch.kubeClient, orch.namespace(), prepRunID)
		return err == nil && (rPrep.Phase == state.PhaseWorking || rPrep.Phase == state.PhaseNoImprovement)
	})
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
		"- Branch: auto-optimizer/throughput/20261104T120000Z-0001",
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
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		GitAuthor: author,
	})
	if orch.gitAuthor != author {
		t.Errorf("orch.gitAuthor = %q, want %q", orch.gitAuthor, author)
	}
}

func TestOrchestrator_PublicURL(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:     store,
		Harness:   harness,
		PublicURL: "https://ao.example.com/",
	})
	if want := "https://ao.example.com"; orch.publicURL != want {
		t.Errorf("orch.publicURL = %q, want %q", orch.publicURL, want)
	}
}

func TestOrchestrator_RunPageURL(t *testing.T) {
	repo := &state.Repo{Host: "github.com", Owner: "testowner", Name: "testrepo"}
	run := &state.Run{ID: "20261105T120000Z-abcd", Repo: repo.Key()}

	tests := []struct {
		name      string
		publicURL string
		orchNil   bool
		run       *state.Run
		want      string
	}{
		{
			name:      "configured",
			publicURL: "https://ao.example.com",
			run:       run,
			want:      "https://ao.example.com/runs/20261105T120000Z-abcd",
		},
		{
			name:      "trailing slash stripped",
			publicURL: "https://ao.example.com/",
			run:       run,
			want:      "https://ao.example.com/runs/20261105T120000Z-abcd",
		},
		{
			name: "unset",
			run:  run,
			want: "",
		},
		{
			name:    "nil orchestrator",
			orchNil: true,
			run:     run,
			want:    "",
		},
		{
			name:      "nil run",
			publicURL: "https://ao.example.com",
			run:       nil,
			want:      "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var orch *Orchestrator
			if !tc.orchNil {
				orch = &Orchestrator{publicURL: strings.TrimRight(tc.publicURL, "/")}
			}
			if got := orch.RunPageURL(repo, tc.run); got != tc.want {
				t.Errorf("RunPageURL() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOrchestrator_Preparing_WatchStatusReadyEnqueuesRun(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)

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

	runID := "20261030T120000Z-watchready"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "goal-watch",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "Cloned successfully\n",
	}
	harness, agentsCS, _ := setupTestHarness(t, procSrv)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	putTestRun(t, ctx, orch, run)

	d := startReconcileDriver(t, orch)

	orch.Notify(repo.Key(), runID)

	// 1. Wait until the run enters Preparing and has a Sandbox created
	var sb *sandboxv1beta1.Sandbox
	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if s, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sb = s
				return true
			}
		}
		return false
	})

	// 2. Verify worker is free (no clone executed yet)
	procSrv.mu.Lock()
	cmdsCount := len(procSrv.commands)
	procSrv.mu.Unlock()
	if cmdsCount != 0 {
		t.Fatalf("expected 0 commands executed while unready, got %d", cmdsCount)
	}

	// 3. Update the fake Sandbox's status to Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox to Ready: %v", err)
	}

	// 4. Verify watch detects status change, enqueues run, clones, and moves to Working
	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		return err == nil && r.Phase == state.PhaseWorking
	})

	procSrv.mu.Lock()
	finalCmds := len(procSrv.commands)
	procSrv.mu.Unlock()
	if finalCmds != 3 {
		t.Fatalf("expected 3 commands executed after Ready, got %d", finalCmds)
	}
}

func TestOrchestrator_Preparing_WatchDeleteEnqueuesAndFailsRun(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)

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

	runID := "20261030T120000Z-watchdel"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "goal-del",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	putTestRun(t, ctx, orch, run)

	d := startReconcileDriver(t, orch)

	orch.Notify(repo.Key(), runID)

	// Wait until run enters Preparing and sandbox exists in Kubernetes
	var sbName string
	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if _, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sbName = r.SandboxName
				return true
			}
		}
		return false
	})

	// Delete fake sandbox before readiness
	if err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, sbName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting fake sandbox: %v", err)
	}

	// Verify watch detects deletion, enqueues run, and marks it Failed
	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		return err == nil && r.Phase == state.PhaseFailed
	})

	r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if !strings.Contains(r.Error, "deleted before becoming ready") {
		t.Errorf("expected error to mention deleted before becoming ready, got: %q", r.Error)
	}
}

func TestOrchestrator_Preparing_WatchFailureConditionFailsRun(t *testing.T) {
	ctx := t.Context()

	storeDir := t.TempDir()
	store := state.NewFileStore(storeDir)

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

	runID := "20261030T120000Z-watchfailcond"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "goal-failcond",
		Phase:      state.PhasePending,
		BaseCommit: "commit-base123",
	}

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})
	putTestRun(t, ctx, orch, run)

	d := startReconcileDriver(t, orch)

	orch.Notify(repo.Key(), runID)

	// Wait until run enters Preparing and sandbox exists in Kubernetes
	var sb *sandboxv1beta1.Sandbox
	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err == nil && r.Phase == state.PhasePreparing && r.SandboxName != "" {
			if s, getErr := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, r.SandboxName, metav1.GetOptions{}); getErr == nil {
				sb = s
				return true
			}
		}
		return false
	})

	// Update fake sandbox with failure condition (InvalidConfiguration)
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:    string(sandboxv1beta1.SandboxConditionReady),
			Status:  metav1.ConditionFalse,
			Reason:  sandboxv1beta1.SandboxReasonInvalidConfiguration,
			Message: "Sandbox name exceeds maximum length",
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating fake sandbox with failure condition: %v", err)
	}

	// Verify watch detects status change, enqueues run, and marks it Failed
	d.runUntil(func() bool {
		r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		return err == nil && r.Phase == state.PhaseFailed
	})

	r, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if !strings.Contains(r.Error, "Sandbox name exceeds maximum length") {
		t.Errorf("expected error to mention failure message, got: %q", r.Error)
	}
}

func TestNewScheme(t *testing.T) {
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme() failed: %v", err)
	}

	hrGVK := herorunv1alpha1.GroupVersion.WithKind("HeroRun")
	if !scheme.Recognizes(hrGVK) {
		t.Errorf("scheme does not recognize %s", hrGVK)
	}

	sessGVK := herorunv1alpha1.GroupVersion.WithKind("Session")
	if !scheme.Recognizes(sessGVK) {
		t.Errorf("scheme does not recognize %s", sessGVK)
	}

	msgGVK := herorunv1alpha1.GroupVersion.WithKind("SessionMessage")
	if !scheme.Recognizes(msgGVK) {
		t.Errorf("scheme does not recognize %s", msgGVK)
	}

	sbGVK := sandboxv1beta1.GroupVersion.WithKind("Sandbox")
	if !scheme.Recognizes(sbGVK) {
		t.Errorf("scheme does not recognize %s", sbGVK)
	}
}

func TestNewManager(t *testing.T) {
	restConfig := &rest.Config{Host: "http://127.0.0.1:65534"}

	mgr, err := NewManager(restConfig, "test-namespace")
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}

	mgrEmpty, err := NewManager(restConfig, "")
	if err != nil {
		t.Fatalf("NewManager() with empty namespace failed: %v", err)
	}
	if mgrEmpty == nil {
		t.Fatal("expected non-nil manager with empty namespace")
	}
}

func TestOrchestrator_SetupWithManager(t *testing.T) {
	restConfig := &rest.Config{Host: "http://127.0.0.1:65534"}
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	skipNameValidation := true
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme: scheme,
		Logger: klog.NewKlogr(),
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Cache: crcache.Options{
			DefaultNamespaces: map[string]crcache.Config{
				"ao-sandboxes": {},
			},
		},
		Controller: crconfig.Controller{
			SkipNameValidation: &skipNameValidation,
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: mgr.GetClient(),
	})

	if err := orch.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager failed: %v", err)
	}
}

func TestHeroRunGenerationChangedPredicate(t *testing.T) {
	pred := predicate.GenerationChangedPredicate{}

	oldRun := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "run-1",
			Generation: 1,
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}

	// Status-only update: generation unchanged, status differs.
	statusOnlyUpdate := oldRun.DeepCopy()
	statusOnlyUpdate.Status.Phase = string(state.PhaseWorking)
	if pred.Update(event.UpdateEvent{ObjectOld: oldRun, ObjectNew: statusOnlyUpdate}) {
		t.Error("expected status-only update event to be filtered, but predicate returned true")
	}

	// Generation change: spec update increments generation.
	genChangedUpdate := oldRun.DeepCopy()
	genChangedUpdate.Generation = 2
	genChangedUpdate.Status.Phase = string(state.PhaseWorking)
	if !pred.Update(event.UpdateEvent{ObjectOld: oldRun, ObjectNew: genChangedUpdate}) {
		t.Error("expected generation change update event to pass, but predicate returned false")
	}

	// Creates and deletes still pass.
	if !pred.Create(event.CreateEvent{Object: oldRun}) {
		t.Error("expected create event to pass")
	}
	if !pred.Delete(event.DeleteEvent{Object: oldRun}) {
		t.Error("expected delete event to pass")
	}
}

func TestOrchestrator_Notify_PopulatesRunIndex(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:   store,
		Harness: harness,
	})

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	runID := "20261030T120000Z-index"
	orch.Notify(repoKey, runID)

	reqName := sandboxNameForRun(runID)
	orch.runIndexMu.RLock()
	entry, ok := orch.runIndex[reqName]
	orch.runIndexMu.RUnlock()
	if !ok {
		t.Fatalf("expected runIndex to be populated after Notify")
	}
	if entry.repo != repoKey {
		t.Errorf("got repo %+v, want %+v", entry.repo, repoKey)
	}
	if entry.runID != runID {
		t.Errorf("got runID %q, want %q", entry.runID, runID)
	}
}

func TestGetRunForReconcile_ResolvesThroughHeroRunOnIndexMiss(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

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

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	runID := "20261030T120000Z-hrlookup"
	hrName := sandboxNameForRun(runID)

	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hrName,
			Namespace: harness.Namespace(),
			Labels: map[string]string{
				LabelRun: runID,
			},
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: "https://github.com/gke-labs/test-repo",
		},
	}
	if err := fakeClient.Create(ctx, hr); err != nil {
		t.Fatalf("creating HeroRun: %v", err)
	}

	// Verify index does NOT have it before request
	orch.runIndexMu.RLock()
	_, foundBefore := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if foundBefore {
		t.Fatal("expected index to miss before getRunForReconcile")
	}

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      hrName,
		},
	}
	gotRun, gotRepo, _, err := orch.getRunForReconcile(ctx, req)
	if err != nil || gotRun == nil {
		t.Fatalf("expected getRunForReconcile to resolve through HeroRun on index miss, got run=%v err=%v", gotRun, err)
	}
	if gotRepo.Key() != repoKey {
		t.Errorf("got repo %+v, want %+v", gotRepo.Key(), repoKey)
	}
	if gotRun.ID != runID {
		t.Errorf("got runID %q, want %q", gotRun.ID, runID)
	}

	// Verify index is now populated
	orch.runIndexMu.RLock()
	cached, foundAfter := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if !foundAfter {
		t.Fatal("expected index to be populated after getRunForReconcile miss")
	}
	if cached.repo != repoKey || cached.runID != runID {
		t.Errorf("cached entry %+v, want repo %+v runID %s", cached, repoKey, runID)
	}
}

func TestOrchestrator_Reconcile_UnknownNameDroppedWithoutError(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      "unknown-hero-run-name",
		},
	}
	res, err := orch.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("expected unknown request to be dropped without error, got: %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("expected empty reconcile.Result, got: %+v", res)
	}
}

func TestOrchestrator_Reconcile_ReadErrorReturnsError(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return errors.New("simulated API server read error")
		},
	}).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: harness.Namespace(),
			Name:      "some-run",
		},
	}
	_, err := orch.Reconcile(ctx, req)
	if err == nil {
		t.Fatal("expected Reconcile to return error on API read failure")
	}
	if !strings.Contains(err.Error(), "simulated API server read error") {
		t.Errorf("expected error to mention 'simulated API server read error', got: %v", err)
	}
}

func TestOrchestrator_FinishedRunHasNoIndexEntry(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	repoKey := state.RepoKey{Host: "github.com", Owner: "gke-labs", Name: "test-repo"}
	repo := &state.Repo{URL: "https://github.com/gke-labs/test-repo", Host: repoKey.Host, Owner: repoKey.Owner, Name: repoKey.Name}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}
	runID := "20261030T120000Z-finished"
	hrName := sandboxNameForRun(runID)
	orch.Notify(repoKey, runID)

	run := &state.Run{ID: runID, Repo: repoKey, Phase: state.PhaseWorking}
	hr := HeroRunFromRun(run)
	hr.Namespace = harness.Namespace()
	hr.Spec.Repository = repo.URL
	_ = fakeClient.Create(ctx, hr)
	var hrObj herorunv1alpha1.HeroRun
	_ = fakeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: hrName}, &hrObj)
	_ = orch.saveHeroRun(ctx, &hrObj, run)
	now := time.Now().UTC()
	run.SetPhase(state.PhaseNoImprovement, now)
	_ = orch.saveTerminalRun(ctx, repo, run, &hrObj, state.PhaseWorking, now, runEnd{Phase: state.PhaseNoImprovement, Kind: "finished"})

	orch.runIndexMu.RLock()
	_, found := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if found {
		t.Errorf("expected finished run %s to have no index entry", runID)
	}

	// Late event resolves through HeroRun and leaves no index entry.
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: harness.Namespace(), Name: hrName}}
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile on terminal run: %v", err)
	}
	orch.runIndexMu.RLock()
	_, foundLate := orch.runIndex[hrName]
	orch.runIndexMu.RUnlock()
	if foundLate {
		t.Errorf("expected late event for terminal run not to leave index entry")
	}
}

func TestOrchestrator_PublishHeroRun_LeavesPending(t *testing.T) {
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

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()

	fixedTime := time.Date(2026, 10, 30, 12, 1, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		Clock:      func() time.Time { return fixedTime },
		KubeClient: fakeClient,
	})
	putTestRun(t, ctx, orch, run)

	// Reconcile Pending run -> leaves Pending to Preparing
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", res.RequeueAfter)
	}

	// Verify run left Pending
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase Preparing, got %s", gotRun.Phase)
	}

	// Verify one HeroRun was created with expected spec, labels, and phase
	var hrList herorunv1alpha1.HeroRunList
	if err := fakeClient.List(ctx, &hrList, client.InNamespace("ao-sandboxes")); err != nil {
		t.Fatalf("fakeClient.List: %v", err)
	}
	if len(hrList.Items) != 1 {
		t.Fatalf("expected 1 HeroRun, got %d", len(hrList.Items))
	}

	hr := hrList.Items[0]
	expectedName := "run-20261030t120000z-abcd"
	if hr.Name != expectedName {
		t.Errorf("expected HeroRun name %q, got %q", expectedName, hr.Name)
	}
	if hr.Namespace != "ao-sandboxes" {
		t.Errorf("expected HeroRun namespace 'ao-sandboxes', got %q", hr.Namespace)
	}
	if hr.Spec.Repository != repo.URL {
		t.Errorf("expected spec.repository %q, got %q", repo.URL, hr.Spec.Repository)
	}
	if hr.Spec.Goal != "optimize-search" {
		t.Errorf("expected spec.goal %q, got %q", "optimize-search", hr.Spec.Goal)
	}
	if hr.Spec.RunID != runID {
		t.Errorf("expected spec.runID %q, got %q", runID, hr.Spec.RunID)
	}
	if hr.Spec.BaseCommit != "commit-base123" {
		t.Errorf("expected spec.baseCommit %q, got %q", "commit-base123", hr.Spec.BaseCommit)
	}

	// Verify the Sandbox was created with exactly one owner reference to the HeroRun
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference on sandbox, got %d", len(sb.OwnerReferences))
	}
	ref := sb.OwnerReferences[0]
	if ref.APIVersion != herorunv1alpha1.GroupVersion.String() {
		t.Errorf("expected APIVersion %q, got %q", herorunv1alpha1.GroupVersion.String(), ref.APIVersion)
	}
	if ref.Kind != "HeroRun" {
		t.Errorf("expected Kind 'HeroRun', got %q", ref.Kind)
	}
	if ref.Name != hr.Name {
		t.Errorf("expected owner ref name %q, got %q", hr.Name, ref.Name)
	}
	if ref.UID != hr.UID {
		t.Errorf("expected owner ref UID %q, got %q", hr.UID, ref.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("expected owner ref controller to be true")
	}
}

func TestOrchestrator_Resume_EnqueuesNonTerminalRuns(t *testing.T) {
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

	// Legacy disk run with no HeroRun should NOT be imported
	legacyDiskRun := &state.Run{
		ID:         "20261030T120000Z-diskonly",
		Repo:       repo.Key(),
		GoalName:   "goal-disk",
		Phase:      state.PhaseWorking,
		BaseCommit: "commit1",
	}
	if err := store.PutRun(ctx, legacyDiskRun); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	harness, _, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	// Pre-create HeroRun 1 (non-terminal: Working)
	hr1 := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-20261030t120000z-hr1",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: repo.URL,
			Goal:       "goal-working",
			RunID:      "20261030T120000Z-hr1",
			BaseCommit: "commit1",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhaseWorking),
		},
	}

	// Pre-create HeroRun 2 (terminal: Succeeded)
	hr2 := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "run-20261030t120000z-hr2",
			Namespace: "ao-sandboxes",
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: repo.URL,
			Goal:       "goal-succeeded",
			RunID:      "20261030T120000Z-hr2",
			BaseCommit: "commit2",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhaseSucceeded),
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).
		WithObjects(hr1, hr2).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	if err := orch.Resume(ctx); err != nil {
		t.Fatalf("orch.Resume: %v", err)
	}

	// hr1 is non-terminal, so it should be indexed
	orch.runIndexMu.RLock()
	_, found1 := orch.runIndex["run-20261030t120000z-hr1"]
	_, found2 := orch.runIndex["run-20261030t120000z-hr2"]
	_, foundDisk := orch.runIndex["run-20261030t120000z-diskonly"]
	orch.runIndexMu.RUnlock()

	if !found1 {
		t.Errorf("expected non-terminal run hr1 to be indexed")
	}
	if found2 {
		t.Errorf("expected terminal run hr2 NOT to be indexed")
	}
	if foundDisk {
		t.Errorf("expected disk-only run NOT to be indexed or imported")
	}

	// Verify disk-only run was NOT imported into kubeClient
	var hrDisk herorunv1alpha1.HeroRun
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: "run-20261030t120000z-diskonly"}, &hrDisk); err == nil {
		t.Errorf("expected disk-only run NOT to have HeroRun created in kubeClient")
	}
}

func TestOrchestrator_SaveHeroRunFailureFailsReconcile(t *testing.T) {
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

	harness, _, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	hr := HeroRunFromRun(run)
	hr.Namespace = harness.Namespace()
	hr.Spec.Repository = repo.URL
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).
		WithObjects(hr).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return errors.New("simulated save status failure")
			},
		}).Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	_, err = orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err == nil {
		t.Fatal("expected orch.Reconcile to fail when saveHeroRun fails, got nil")
	}
}

func TestOrchestrator_SandboxOwnerReference_PreExistingHeroRunHasUID(t *testing.T) {
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

	runID := "20261030T120000Z-uidtest"

	harness, agentsCS, _ := setupTestHarness(t, nil)

	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	expectedName := "run-20261030t120000z-uidtest"
	const expectedUID = types.UID("custom-herorun-uid-456")
	preExistingHR := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      expectedName,
			Namespace: "ao-sandboxes",
			UID:       expectedUID,
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			Repository: repo.URL,
			Goal:       "optimize-search",
			RunID:      runID,
			BaseCommit: "commit-base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase: string(state.PhasePending),
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).
		WithObjects(preExistingHR).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:      store,
		Harness:    harness,
		KubeClient: fakeClient,
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, expectedName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("agentsCS.Sandboxes.Get: %v", err)
	}
	if len(sb.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(sb.OwnerReferences))
	}
	ref := sb.OwnerReferences[0]
	if ref.APIVersion != herorunv1alpha1.GroupVersion.String() {
		t.Errorf("expected APIVersion %q, got %q", herorunv1alpha1.GroupVersion.String(), ref.APIVersion)
	}
	if ref.Kind != "HeroRun" {
		t.Errorf("expected Kind 'HeroRun', got %q", ref.Kind)
	}
	if ref.Name != expectedName {
		t.Errorf("expected owner ref name %q, got %q", expectedName, ref.Name)
	}
	if ref.UID != expectedUID {
		t.Errorf("expected owner ref UID %q, got %q", expectedUID, ref.UID)
	}
	if ref.Controller == nil || !*ref.Controller {
		t.Errorf("expected controller to be true")
	}
}

type scriptedOpenPullRequestLLM struct {
	repo, branch, title, body string
}

func (s *scriptedOpenPullRequestLLM) CreateChatCompletion(_ context.Context, _ llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	args, _ := json.Marshal(map[string]string{
		"repository": s.repo, "branch": s.branch, "title": s.title, "body": s.body,
	})
	return &llm.ChatCompletionResponse{
		ID: "pr-llm",
		Choices: []llm.Choice{{
			Message: llm.Message{
				Role: "assistant",
				ToolCalls: []llm.ToolCall{{
					ID:   "call-open-pr-1",
					Type: "function",
					Function: llm.FunctionCall{
						Name:      "open_pull_request",
						Arguments: string(args),
					},
				}},
			},
			FinishReason: "tool_calls",
		}},
	}, nil
}

type scriptedExportServer struct {
	processv1.UnimplementedProcessServiceServer
	mu            sync.Mutex
	commands      map[string]string
	pack          []byte
	startHandlers map[string]func(stream processv1.ProcessService_StartServer) error
	startCalls    []string
}

func (s *scriptedExportServer) Execute(_ context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	cmd := strings.Join(req.GetConfig().GetCommand(), " ")
	s.mu.Lock()
	out, ok := s.commands[cmd]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unhandled command: %s", cmd)
	}
	return &processv1.ExecuteResponse{Stdout: []byte(out)}, nil
}

func (s *scriptedExportServer) Start(req *processv1.StartRequest, stream processv1.ProcessService_StartServer) error {
	cmd := strings.Join(req.GetConfig().GetCommand(), " ")
	s.mu.Lock()
	s.startCalls = append(s.startCalls, cmd)
	var handler func(stream processv1.ProcessService_StartServer) error
	if s.startHandlers != nil {
		handler = s.startHandlers[cmd]
	}
	pack := s.pack
	s.mu.Unlock()

	if handler != nil {
		return handler(stream)
	}

	if err := stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 1}}}); err != nil {
		return err
	}
	if len(pack) > 0 {
		_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: pack}})
	}
	return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{}}})
}

func (s *scriptedExportServer) WriteStdin(context.Context, *processv1.WriteStdinRequest) (*processv1.WriteStdinResponse, error) {
	return &processv1.WriteStdinResponse{}, nil
}

func createTestRemoteAndPackfile(t *testing.T) (string, *git.Repository, *git.Repository, string, string, []byte) {
	t.Helper()
	bareDir := filepath.Join(t.TempDir(), "remote.git")
	bare, err := git.PlainInit(bareDir, true)
	if err != nil {
		t.Fatalf("PlainInit bare: %v", err)
	}
	remoteURL := "file://" + bareDir

	work, err := git.PlainInit(filepath.Join(t.TempDir(), "work"), false)
	if err != nil {
		t.Fatalf("PlainInit work: %v", err)
	}
	wt, err := work.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), "base.txt"), []byte("base"), 0644)
	_, _ = wt.Add("base.txt")
	baseCommit, err := wt.Commit("base", &git.CommitOptions{Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()}})
	if err != nil {
		t.Fatalf("Commit base: %v", err)
	}

	_, _ = work.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
	if err := work.Push(&git.PushOptions{RemoteName: "origin"}); err != nil {
		t.Fatalf("Push base: %v", err)
	}

	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), "feat.txt"), []byte("feat"), 0644)
	_, _ = wt.Add("feat.txt")
	tipCommit, err := wt.Commit("feat", &git.CommitOptions{Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()}})
	if err != nil {
		t.Fatalf("Commit tip: %v", err)
	}
	hashes, err := revlist.Objects(work.Storer, []plumbing.Hash{tipCommit}, []plumbing.Hash{baseCommit})
	if err != nil {
		t.Fatalf("revlist.Objects: %v", err)
	}
	buf := new(bytes.Buffer)
	if _, err := packfile.NewEncoder(buf, work.Storer, false).Encode(hashes, 10); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return remoteURL, bare, work, baseCommit.String(), tipCommit.String(), buf.Bytes()
}

func setupSelectingRun(t *testing.T, phase state.RunPhase, baseSHA string, procSrv processv1.ProcessServiceServer, llmClient llm.ChatClient) (*Orchestrator, state.Store, *fakeagents.Clientset, *state.Repo, *state.Run) {
	t.Helper()
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	repo := &state.Repo{URL: "https://github.com/gke-labs/test-repo", Host: "github.com", Owner: "gke-labs", Name: "test-repo", DefaultBranch: "main"}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}
	runID := "20261030T120000Z-test"
	sandboxName := "run-20261030t120000z-test"
	run := &state.Run{
		ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: phase, BaseCommit: baseSHA, SandboxName: sandboxName,
		Goal: &goals.Goal{Name: "goal", Body: "goal body"},
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, llmClient)
	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}
	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store: store, SessionStore: session.NewFileStore(t.TempDir()), Harness: harness, Clock: func() time.Time { return fixedTime }, MaxTurns: 30,
	})
	putTestRun(t, ctx, orch, run)
	return orch, store, agentsCS, repo, run
}

func TestOrchestrator_Working_OpenPullRequest_TransitionsToSelecting(t *testing.T) {
	ctx := t.Context()
	prLLM := &scriptedOpenPullRequestLLM{
		repo:   "https://github.com/gke-labs/test-repo",
		branch: "auto-optimizer/goal/20261030T120000Z-test",
		title:  "Optimize search index queries",
		body:   "Adds composite index on (repo, goal, run_id).",
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseWorking, "base-sha", &mockProcessServer{}, prLLM)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSelecting || gotRun.PRTitle != prLLM.title || gotRun.PRBody != prLLM.body {
		t.Errorf("got run %+v, want Selecting with PRTitle/Body", gotRun)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var found bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "pull_request_requested" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected pull_request_requested event, got: %+v", events)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); err != nil {
		t.Errorf("expected sandbox to still exist, got: %v", err)
	}
}

func TestOrchestrator_Selecting_PushesBranch_Succeeds(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, bare, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-pushrun"
	sandboxName := "run-20261030t120000z-pushrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
		PRTitle:     "Add feature",
		PRBody:      "Implements feature",
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})
	putTestRun(t, ctx, orch, run)

	req := runRequest(orch, repo.Key(), runID)
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile (Selecting): %v", err)
	}
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile (OpeningPR): %v", err)
	}

	// 1. Verify run ended in Succeeded with Branch set
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("expected phase %s, got %s", state.PhaseSucceeded, gotRun.Phase)
	}
	if gotRun.PRURL != "" {
		t.Errorf("expected empty PRURL, got %q", gotRun.PRURL)
	}
	wantBranch := "auto-optimizer/export-goal/" + runID
	if gotRun.Branch != wantBranch {
		t.Errorf("expected branch %q, got %q", wantBranch, gotRun.Branch)
	}

	// 2. Verify bare remote has branch at tipSHA
	refName := plumbing.ReferenceName("refs/heads/" + wantBranch)
	ref, err := bare.Reference(refName, true)
	if err != nil {
		t.Fatalf("bare.Reference(%s): %v", refName, err)
	}
	if ref.Hash().String() != tipSHA {
		t.Errorf("expected remote ref %s to be %s, got %s", refName, tipSHA, ref.Hash().String())
	}

	// 3. Verify events: commits_exported, branch_pushed, pull_request_not_opened
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundExported, foundPushed, foundPRNotOpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "commits_exported" {
			if m, ok := e.Data.(map[string]any); ok && m["tip"] == tipSHA {
				foundExported = true
			}
		}
		if e.Phase == state.PhaseSelecting && e.Kind == "branch_pushed" {
			if m, ok := e.Data.(map[string]any); ok && m["tip"] == tipSHA && m["branch"] == wantBranch {
				foundPushed = true
			}
		}
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_not_opened" && e.Message == "forge cannot open pull requests" {
			foundPRNotOpened = true
		}
	}
	if !foundExported {
		t.Errorf("expected commits_exported event carrying tip %s, got: %+v", tipSHA, events)
	}
	if !foundPushed {
		t.Errorf("expected branch_pushed event carrying tip %s and branch %s, got: %+v", tipSHA, wantBranch, events)
	}
	if !foundPRNotOpened {
		t.Errorf("expected pull_request_not_opened event, got: %+v", events)
	}

	// 4. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_BranchMoved_Fails(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, bare, work, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-movedrun"
	sandboxName := "run-20261030t120000z-movedrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
		PRTitle:     "Add feature",
		PRBody:      "Implements feature",
	}

	// Advance auto-optimizer/<goal>/<run-id> in the bare repository to another commit before Selecting
	wantBranch := "auto-optimizer/export-goal/" + runID
	wt, err := work.Worktree()
	if err != nil {
		t.Fatalf("Worktree: %v", err)
	}
	_ = os.WriteFile(filepath.Join(wt.Filesystem.Root(), "other.txt"), []byte("other"), 0644)
	_, _ = wt.Add("other.txt")
	otherCommit, err := wt.Commit("other", &git.CommitOptions{Author: &object.Signature{Name: "A", Email: "a@example.com", When: time.Now()}})
	if err != nil {
		t.Fatalf("Commit other: %v", err)
	}
	refName := plumbing.ReferenceName("refs/heads/" + wantBranch)
	if err := work.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec(otherCommit.String() + ":" + refName.String())},
	}); err != nil {
		t.Fatalf("Push other to bare: %v", err)
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Failed
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if gotRun.Error != "remote branch is not at base commit" {
		t.Errorf("expected error %q, got %q", "remote branch is not at base commit", gotRun.Error)
	}

	// 2. Verify events: branch_moved
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundBranchMoved bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "branch_moved" && e.Message == "remote branch is not at base commit" {
			foundBranchMoved = true
			break
		}
	}
	if !foundBranchMoved {
		t.Errorf("expected branch_moved event, got: %+v", events)
	}

	// 3. Verify remote branch was left unchanged
	if ref, err := bare.Reference(refName, true); err != nil || ref.Hash() != otherCommit {
		t.Errorf("expected bare branch to remain at %s, got: %v (err: %v)", otherCommit, ref, err)
	}

	// 4. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_PushFailed(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	_, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	// Point repository URL at a nonexistent path to trigger real push failure
	nonexistentURL := "file://" + filepath.Join(t.TempDir(), "nonexistent.git")

	repoKey, err := state.ParseRepoURL(nonexistentURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           nonexistentURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-pushfailrun"
	sandboxName := "run-20261030t120000z-pushfailrun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return forge.NewGitRemote(), nil
		},
	})
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Failed
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}

	// 2. Verify events: push_failed
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundPushFailed bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "push_failed" {
			foundPushFailed = true
			break
		}
	}
	if !foundPushFailed {
		t.Errorf("expected push_failed event, got events: %+v", events)
	}

	// 3. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_NilForge_Fails(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-nilforgerun"
	sandboxName := "run-20261030t120000z-nilforgerun"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor:     nil, // explicitly nil
	})
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Failed
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
	}

	// 2. Verify events: push_failed
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundPushFailed bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "push_failed" {
			foundPushFailed = true
			break
		}
	}
	if !foundPushFailed {
		t.Errorf("expected push_failed event, got events: %+v", events)
	}

	// 3. Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got: %v", err)
	}
}

func TestOrchestrator_Selecting_Refusals_NoImprovement(t *testing.T) {
	const baseSHA = "1234567890abcdef1234567890abcdef12345678"
	tests := []struct {
		name, diffOutput, wantKind, wantMessage string
	}{
		{name: "no changes against base", diffOutput: "", wantKind: "no_changes", wantMessage: "no changes against base commit"},
		{name: "changes under .goals/ refused", diffOutput: ".goals/goal.md\x00", wantKind: "refused", wantMessage: "changes under .goals/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			procSrv := &scriptedExportServer{
				commands: map[string]string{
					"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
					"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       tt.diffOutput,
				},
			}
			orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, baseSHA, procSrv, nil)

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
				t.Fatalf("orch.Reconcile: %v", err)
			}

			gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
			if err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if gotRun.Phase != state.PhaseNoImprovement {
				t.Errorf("got phase %s, want NoImprovement", gotRun.Phase)
			}

			events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
			var found bool
			for _, e := range events {
				if e.Phase == state.PhaseNoImprovement && e.Kind == tt.wantKind && e.Message == tt.wantMessage {
					found = true
				}
			}
			if !found {
				t.Errorf("expected event kind=%q message=%q, got: %+v", tt.wantKind, tt.wantMessage, events)
			}

			if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
				t.Errorf("expected sandbox deleted, got err: %v", err)
			}
		})
	}
}

func TestOrchestrator_Selecting_Verify_PassingProceedsToExport(t *testing.T) {
	ctx := t.Context()
	_, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
	const verifyCmd = "go test ./..."
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stdout{Stdout: []byte("PASS\nok\n")}})
				return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: 0}}})
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, baseSHA, procSrv, nil)
	run.Goal.Verify = verifyCmd
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed || gotRun.Error != "no forge configured for repository "+repo.URL {
		t.Errorf("got phase %s error %q, want Failed/no forge configured", gotRun.Phase, gotRun.Error)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundStarted, foundPassed, foundExported bool
	var order []string
	for _, e := range events {
		if e.Kind == "verify_started" {
			foundStarted = true
			order = append(order, e.Kind)
			if m, ok := e.Data.(map[string]any); !ok || m["command"] != verifyCmd {
				t.Errorf("verify_started data command = %v, want %q", e.Data, verifyCmd)
			}
		}
		if e.Kind == "verify_passed" {
			foundPassed = true
			order = append(order, e.Kind)
		}
		if e.Kind == "commits_exported" {
			foundExported = true
			order = append(order, e.Kind)
		}
	}
	if !foundStarted || !foundPassed || !foundExported {
		t.Errorf("expected verify_started, verify_passed, and commits_exported events, got: %+v", events)
	}
	wantOrder := []string{"verify_started", "verify_passed", "commits_exported"}
	if !slices.Equal(order, wantOrder) {
		t.Errorf("events order = %v, want %v", order, wantOrder)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_Verify_FailingEndsInVerifyFailed(t *testing.T) {
	ctx := t.Context()
	const verifyCmd = "make test"
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	const tailOutput = "--- FAIL: TestA (0.00s)\nFAIL\nexit status 2\n"
	procSrv := &scriptedExportServer{
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Stderr{Stderr: []byte(tailOutput)}})
				return stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Exit{Exit: &processv1.ExitEvent{ExitCode: 2}}})
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, "base-sha", procSrv, nil)
	run.Goal.Verify = verifyCmd
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseVerifyFailed {
		t.Errorf("got phase %s, want VerifyFailed", gotRun.Phase)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundStarted, foundFailed, foundExported bool
	for _, e := range events {
		if e.Kind == "verify_started" {
			foundStarted = true
		}
		if e.Phase == state.PhaseVerifyFailed && e.Kind == "verify_failed" {
			foundFailed = true
			if !strings.Contains(e.Message, "2") {
				t.Errorf("expected exit code in message, got: %q", e.Message)
			}
			m, ok := e.Data.(map[string]any)
			if !ok {
				t.Fatalf("expected map[string]any in e.Data, got %T", e.Data)
			}
			var exitCode int
			switch v := m["exitCode"].(type) {
			case int:
				exitCode = v
			case float64:
				exitCode = int(v)
			default:
				t.Fatalf("unexpected type for exitCode: %T", m["exitCode"])
			}
			if exitCode != 2 {
				t.Errorf("exitCode = %d, want 2", exitCode)
			}
			if m["tail"] != tailOutput {
				t.Errorf("tail = %q, want %q", m["tail"], tailOutput)
			}
		}
		if e.Kind == "commits_exported" {
			foundExported = true
		}
	}
	if !foundStarted || !foundFailed {
		t.Errorf("expected verify_started and verify_failed events, got: %+v", events)
	}
	if foundExported {
		t.Errorf("unexpected commits_exported event when verify failed")
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_Verify_TimeoutEndsInVerifyFailed(t *testing.T) {
	ctx := t.Context()
	const verifyCmd = "sleep 10"
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	var handlerCalled bool
	var handlerMu sync.Mutex
	procSrv := &scriptedExportServer{
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				handlerMu.Lock()
				handlerCalled = true
				handlerMu.Unlock()
				_ = stream.Send(&processv1.StartResponse{Event: &processv1.StartResponse_Init{Init: &processv1.InitEvent{ProcessId: 42}}})
				<-stream.Context().Done()
				return stream.Context().Err()
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, "base-sha", procSrv, nil)
	orch.verifyTimeout = 50 * time.Millisecond
	run.Goal.Verify = verifyCmd
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	handlerMu.Lock()
	invoked := handlerCalled
	handlerMu.Unlock()
	if !invoked {
		t.Fatalf("expected timeout handler for %q to be invoked, but it was not", expectedVerifyShell)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseVerifyFailed {
		t.Errorf("got phase %s, want VerifyFailed", gotRun.Phase)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundTimeout bool
	for _, e := range events {
		if e.Phase == state.PhaseVerifyFailed && e.Kind == "verify_failed" && strings.Contains(e.Message, "timed out") {
			foundTimeout = true
		}
	}
	if !foundTimeout {
		t.Errorf("expected verify_failed event mentioning timeout, got: %+v", events)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_Verify_ExecutionErrorFailsRun(t *testing.T) {
	ctx := t.Context()
	const verifyCmd = "make test"
	expectedVerifyShell := "sh -c cd '/workspace/repo' && " + verifyCmd
	procSrv := &scriptedExportServer{
		startHandlers: map[string]func(stream processv1.ProcessService_StartServer) error{
			expectedVerifyShell: func(stream processv1.ProcessService_StartServer) error {
				return status.Errorf(codes.Unavailable, "sandbox connection lost")
			},
		},
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, "base-sha", procSrv, nil)
	run.Goal.Verify = verifyCmd
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("got phase %s, want Failed", gotRun.Phase)
	}

	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	var foundError bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "verify_error" {
			foundError = true
		}
	}
	if !foundError {
		t.Errorf("expected verify_error event, got: %+v", events)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func TestOrchestrator_Selecting_GoalWithoutVerify_RunsNoCommandBeforeExport(t *testing.T) {
	ctx := t.Context()
	_, _, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)
	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	orch, store, agentsCS, repo, run := setupSelectingRun(t, state.PhaseSelecting, baseSHA, procSrv, nil)
	run.Goal.Verify = ""
	putTestRun(t, ctx, orch, run)

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// Explicit assertion: goal without verify runs no verify command before export.
	procSrv.mu.Lock()
	for _, call := range procSrv.startCalls {
		if strings.Contains(call, "sh -c") {
			t.Errorf("unexpected command run in sandbox for goal without verify: %s", call)
		}
	}
	const wantExportCall = "git -C /workspace/repo pack-objects --stdout --no-thin -q"
	if len(procSrv.startCalls) != 1 || procSrv.startCalls[0] != wantExportCall {
		t.Errorf("expected only pack-objects Start call for export (%q), got: %v", wantExportCall, procSrv.startCalls)
	}
	procSrv.mu.Unlock()

	// Verify no verify events were appended.
	events, _ := store.ReadEvents(ctx, repo.Key(), run.ID)
	for _, e := range events {
		if strings.HasPrefix(e.Kind, "verify_") {
			t.Errorf("unexpected verify event for goal without verify: %+v", e)
		}
	}

	// Verify commits_exported was recorded.
	var foundExported bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "commits_exported" {
			foundExported = true
			break
		}
	}
	if !foundExported {
		t.Errorf("expected commits_exported event, got: %+v", events)
	}

	// Verify sandbox is deleted.
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, run.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox deleted, got err: %v", err)
	}
}

func setupOpeningPRTest(t *testing.T, forgeFor func(string) (forge.Forge, error)) (*Orchestrator, state.Store, *state.Repo, *state.Run) {
	t.Helper()
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL := "https://github.com/example/repo"
	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261106T120000Z-testpr"
	baseSHA := "1189a319f389802669884696409ba28db20216b0"
	branchName := "auto-optimizer/optimize-speed/" + runID
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "optimize-speed",
		Phase:      state.PhaseOpeningPR,
		BaseCommit: baseSHA,
		Branch:     branchName,
		PRTitle:    "Speed up queries",
		PRBody:     "Replaced linear scan with hash map lookup.",
	}

	harness, _, _ := setupTestHarness(t, nil)
	fixedTime := time.Date(2026, 11, 6, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		PublicURL:    "https://ao.example.com",
		Clock:        func() time.Time { return fixedTime },
		ForgeFor:     forgeFor,
	})
	putTestRun(t, ctx, orch, run)
	return orch, store, repo, run
}

func TestOrchestrator_OpeningPR_OpensPullRequest_Succeeds(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded with PRURL set
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("got phase %s, want Succeeded", gotRun.Phase)
	}

	// 2. Verify FakeForge recorded exactly one PR with expected head, base, title and body
	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected exactly 1 pull request, got %d", len(prs))
	}
	pr := prs[0]
	if pr.Head != run.Branch || pr.Base != "main" || pr.Title != run.PRTitle {
		t.Errorf("unexpected PR details: %+v", pr)
	}
	if gotRun.PRURL != pr.URL {
		t.Errorf("run.PRURL = %q, want %q", gotRun.PRURL, pr.URL)
	}

	// Body must contain goal, base commit (short), branch, run ID, link to run page, and agent's text under heading
	shortSHA := run.BaseCommit[:7]
	for _, substr := range []string{run.GoalName, shortSHA, run.Branch, run.ID, "https://ao.example.com/runs/" + run.ID, "What the agent says", run.PRBody} {
		if !strings.Contains(pr.Body, substr) {
			t.Errorf("expected body to contain %q, got:\n%s", substr, pr.Body)
		}
	}

	// 3. Verify event pull_request_opened
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundOpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_opened" && e.Message == pr.URL {
			foundOpened = true
		}
	}
	if !foundOpened {
		t.Errorf("expected pull_request_opened event carrying %s, got: %+v", pr.URL, events)
	}
}

func TestOrchestrator_OpeningPR_ReusesOpenPullRequest(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	// Pre-create an open pull request for the branch
	existingPR, err := fakeForge.OpenPullRequest(ctx, "https://github.com/example/repo", "auto-optimizer/optimize-speed/20261106T120000Z-testpr", "main", "Existing PR", "Existing body")
	if err != nil {
		t.Fatalf("OpenPullRequest: %v", err)
	}

	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded with existing PR URL recorded
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("got phase %s, want Succeeded", gotRun.Phase)
	}
	if gotRun.PRURL != existingPR.URL {
		t.Errorf("got PRURL %q, want %q", gotRun.PRURL, existingPR.URL)
	}

	// 2. Verify no second PR was opened
	if len(fakeForge.PullRequests(repo.URL)) != 1 {
		t.Errorf("expected no second PR opened, got %d", len(fakeForge.PullRequests(repo.URL)))
	}

	// 3. Verify event pull_request_reused
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundReused bool
	for _, e := range events {
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_reused" && e.Message == existingPR.URL {
			foundReused = true
		}
	}
	if !foundReused {
		t.Errorf("expected pull_request_reused event carrying %s, got: %+v", existingPR.URL, events)
	}
}

func TestOrchestrator_OpeningPR_GitRemote_CannotOpenPR(t *testing.T) {
	ctx := t.Context()
	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return forge.NewGitRemote(), nil
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	// 1. Verify run ended in Succeeded without PRURL
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("got phase %s, want Succeeded", gotRun.Phase)
	}
	if gotRun.PRURL != "" {
		t.Errorf("got PRURL %q, want empty", gotRun.PRURL)
	}

	// 2. Verify event pull_request_not_opened says forge cannot open pull requests
	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundNotOpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_not_opened" && e.Message == "forge cannot open pull requests" {
			foundNotOpened = true
		}
	}
	if !foundNotOpened {
		t.Errorf("expected pull_request_not_opened event with 'forge cannot open pull requests', got: %+v", events)
	}
}

func TestOrchestrator_OpeningPR_OpenPullRequestError_Fails(t *testing.T) {
	ctx := t.Context()
	fakeForge := forge.NewFake()
	fakeForge.SetOpenPullRequestError(errors.New("API rate limit exceeded"))

	orch, store, repo, run := setupOpeningPRTest(t, func(string) (forge.Forge, error) {
		return fakeForge, nil
	})

	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed || gotRun.Error != "API rate limit exceeded" {
		t.Errorf("got phase %s error %q, want Failed/'API rate limit exceeded'", gotRun.Phase, gotRun.Error)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundFailed bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "pull_request_failed" && e.Message == "API rate limit exceeded" {
			foundFailed = true
		}
	}
	if !foundFailed {
		t.Errorf("expected pull_request_failed event with 'API rate limit exceeded', got: %+v", events)
	}
}

func TestOrchestrator_Selecting_PushesAndOpensPullRequest_WithFakeForge(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	remoteURL, bare, _, baseSHA, tipSHA, packBytes := createTestRemoteAndPackfile(t)

	repoKey, err := state.ParseRepoURL(remoteURL)
	if err != nil {
		t.Fatalf("ParseRepoURL: %v", err)
	}

	repo := &state.Repo{
		URL:           remoteURL,
		Host:          repoKey.Host,
		Owner:         repoKey.Owner,
		Name:          repoKey.Name,
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	runID := "20261030T120000Z-pushandpr"
	sandboxName := "run-20261030t120000z-pushandpr"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "export-goal",
		Phase:       state.PhaseSelecting,
		BaseCommit:  baseSHA,
		SandboxName: sandboxName,
		PRTitle:     "Add feature",
		PRBody:      "Implements feature",
	}

	procSrv := &scriptedExportServer{
		commands: map[string]string{
			"git -C /workspace/repo status --porcelain=v2 -z --untracked-files=all": "",
			"git -C /workspace/repo diff --name-only -z " + baseSHA + " HEAD":       "feat.txt\x00",
			"git -C /workspace/repo rev-parse --verify HEAD^{commit}":               tipSHA + "\n",
			"git -C /workspace/repo rev-list --objects " + baseSHA + "..HEAD":       tipSHA + "\n",
		},
		pack: packBytes,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, nil)

	policy := sandboxv1beta1.ShutdownPolicyDelete
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
		Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating sandbox in fake k8s: %v", err)
	}

	// FakeForge implements Forge and ChangeRequestCreator. For file:// remotes,
	// PushPackfile pushes directly to the bare git repository without requiring PushCredentialer.
	fakeForge := forge.NewFake()

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
		ForgeFor: func(repoURL string) (forge.Forge, error) {
			return fakeForge, nil
		},
	})
	putTestRun(t, ctx, orch, run)

	req := runRequest(orch, repo.Key(), runID)
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile (Selecting): %v", err)
	}
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile (OpeningPR): %v", err)
	}

	// 1. Verify run ended in Succeeded with Branch and PRURL set
	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseSucceeded {
		t.Errorf("expected phase %s, got %s", state.PhaseSucceeded, gotRun.Phase)
	}
	wantBranch := "auto-optimizer/export-goal/" + runID
	if gotRun.Branch != wantBranch {
		t.Errorf("expected branch %q, got %q", wantBranch, gotRun.Branch)
	}
	prs := fakeForge.PullRequests(repo.URL)
	if len(prs) != 1 {
		t.Fatalf("expected 1 pull request, got %d", len(prs))
	}
	if gotRun.PRURL != prs[0].URL {
		t.Errorf("expected PRURL %q, got %q", prs[0].URL, gotRun.PRURL)
	}

	// 2. Verify bare remote has branch at tipSHA
	refName := plumbing.ReferenceName("refs/heads/" + wantBranch)
	ref, err := bare.Reference(refName, true)
	if err != nil {
		t.Fatalf("bare.Reference(%s): %v", refName, err)
	}
	if ref.Hash().String() != tipSHA {
		t.Errorf("expected remote ref %s to be %s, got %s", refName, tipSHA, ref.Hash().String())
	}

	// 3. Verify events: commits_exported, branch_pushed, pull_request_opened
	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundExported, foundPushed, foundPROpened bool
	for _, e := range events {
		if e.Phase == state.PhaseSelecting && e.Kind == "commits_exported" {
			foundExported = true
		}
		if e.Phase == state.PhaseSelecting && e.Kind == "branch_pushed" {
			foundPushed = true
		}
		if e.Phase == state.PhaseSucceeded && e.Kind == "pull_request_opened" && e.Message == prs[0].URL {
			foundPROpened = true
		}
	}
	if !foundExported || !foundPushed || !foundPROpened {
		t.Errorf("expected commits_exported, branch_pushed, pull_request_opened events, got: %+v", events)
	}
}

type blockingProcessServer struct {
	processv1.UnimplementedProcessServiceServer
	onCommand func(cmd []string)
}

func (s *blockingProcessServer) Execute(ctx context.Context, req *processv1.ExecuteRequest) (*processv1.ExecuteResponse, error) {
	if s.onCommand != nil && req.Config != nil {
		s.onCommand(req.Config.Command)
	}
	return &processv1.ExecuteResponse{
		ExitCode: 0,
		Stdout:   []byte("ok\n"),
	}, nil
}

type blockingToolLLM struct {
	mu        sync.Mutex
	callCount int
}

func (b *blockingToolLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.callCount++
	if b.callCount == 1 {
		args, _ := json.Marshal(map[string]any{"command": "block-test"})
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-block-1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
				},
			},
		}, nil
	}
	content := "done"
	return &llm.ChatCompletionResponse{
		Choices: []llm.Choice{
			{
				Message: llm.Message{
					Role:    "assistant",
					Content: &content,
				},
			},
		},
	}, nil
}

func TestOrchestrator_TurnWorker_DoesNotBlockOtherRepoWorkingReconcile(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

	repo1 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo1",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo1",
		DefaultBranch: "main",
	}
	repo2 := &state.Repo{
		URL:           "https://github.com/gke-labs/repo2",
		Host:          "github.com",
		Owner:         "gke-labs",
		Name:          "repo2",
		DefaultBranch: "main",
	}
	if err := store.PutRepo(ctx, repo1); err != nil {
		t.Fatalf("PutRepo 1: %v", err)
	}
	if err := store.PutRepo(ctx, repo2); err != nil {
		t.Fatalf("PutRepo 2: %v", err)
	}

	run1ID := "20261030T120000Z-run1"
	sb1Name := "run-20261030t120000z-run1"
	run1 := &state.Run{
		ID:          run1ID,
		Repo:        repo1.Key(),
		GoalName:    "goal1",
		Phase:       state.PhaseWorking,
		BaseCommit:  "commit-base1",
		SandboxName: sb1Name,
		Goal: &goals.Goal{
			Name: "goal1",
			Body: "run1 goal",
		},
	}

	run2ID := "20261030T120000Z-run2"
	sb2Name := "run-20261030t120000z-run2"
	run2 := &state.Run{
		ID:          run2ID,
		Repo:        repo2.Key(),
		GoalName:    "goal2",
		Phase:       state.PhasePreparing,
		BaseCommit:  "commit-base2",
		SandboxName: sb2Name,
	}

	toolEntered := make(chan struct{})
	toolRelease := make(chan struct{})
	var enteredOnce sync.Once

	procSrv := &blockingProcessServer{
		onCommand: func(cmd []string) {
			for _, arg := range cmd {
				if arg == "block-test" {
					enteredOnce.Do(func() { close(toolEntered) })
					<-toolRelease
					return
				}
			}
		},
	}

	blockingLLM := &blockingToolLLM{}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, blockingLLM)

	// Create sandboxes in fake k8s
	policy := sandboxv1beta1.ShutdownPolicyDelete
	for _, sbName := range []string{sb1Name, sb2Name} {
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: "ao-sandboxes"},
			Spec: sandboxv1beta1.SandboxSpec{
				Lifecycle: sandboxv1beta1.Lifecycle{
					ShutdownTime:   &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
					ShutdownPolicy: &policy,
				},
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

	fixedTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run1)
	putTestRun(t, ctx, orch, run2)

	// 1. Reconcile run1 (Working). This creates the root Session CRD without blocking.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo1.Key(), run1ID)); err != nil {
		t.Fatalf("orch.Reconcile run1: %v", err)
	}

	sess1Done := make(chan error, 1)
	go func() {
		sess1Done <- reconcileSessionUntilTerminal(ctx, orch, topLevelSessionID(run1ID, ""))
	}()

	// Wait for run1's session controller to enter the blocking tool call.
	select {
	case <-toolEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run1 to enter blocking tool call")
	}

	// 2. While run1's tool call is blocked, reconcile run2 from Preparing to Working.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo2.Key(), run2ID)); err != nil {
		t.Fatalf("orch.Reconcile run2: %v", err)
	}

	// Verify run2 reached Working phase even though run1 is still blocked in its turn.
	gotRun2, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run2ID)
	if err != nil {
		t.Fatalf("GetRun run2: %v", err)
	}
	if gotRun2.Phase != state.PhaseWorking {
		t.Fatalf("expected run2 to reach Working while run1 was blocked, got %s", gotRun2.Phase)
	}

	// 3. Release run1 and let its Session finish.
	close(toolRelease)
	select {
	case err := <-sess1Done:
		if err != nil {
			t.Fatalf("reconcileSessionUntilTerminal run1: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run1 session to finish")
	}

	// Reconcile run1 to consume terminal Session outcome.
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo1.Key(), run1ID)); err != nil {
		t.Fatalf("orch.Reconcile run1 outcome: %v", err)
	}

	gotRun1, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run1ID)
	if err != nil {
		t.Fatalf("GetRun run1: %v", err)
	}
	if gotRun1.Phase != state.PhaseNoImprovement {
		t.Errorf("expected run1 to reach NoImprovement, got %s", gotRun1.Phase)
	}
}

func TestOrchestrator_HandleMap_EmptyAfterTerminalTransition(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

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

	// Case 1: Terminal transition to PhaseNoImprovement
	t.Run("NoImprovement", func(t *testing.T) {
		runID := "20261030T120000Z-term-noimp"
		sandboxName := "run-term-noimp"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "goal-noimp",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
		}

		harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})
		policy := sandboxv1beta1.ShutdownPolicyDelete
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: sandboxName, Namespace: "ao-sandboxes"},
			Spec:       sandboxv1beta1.SandboxSpec{Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)}, ShutdownPolicy: &policy}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("creating sandbox: %v", err)
		}

		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		putTestRun(t, ctx, orch, run)

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("reconcileWorkingRun: %v", err)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseNoImprovement {
			t.Fatalf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
		}

		// Handle map must be empty after terminal transition
		if count := harness.TrackedSandboxesCount(); count != 0 {
			t.Errorf("expected tracked sandboxes count 0 after terminal transition to NoImprovement, got %d", count)
		}
	})

	// Case 2: Terminal transition to PhaseFailed
	t.Run("Failed", func(t *testing.T) {
		runID := "20261030T120000Z-term-fail"
		sandboxName := "run-term-fail"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "goal-fail",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
		}

		// Use failing LLM so session fails
		harness, _, _ := setupTestHarnessWithLLM(t, nil, &failingChatClient{})
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		putTestRun(t, ctx, orch, run)

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err == nil {
			t.Logf("reconcileWorkingRun completed (errors handled)")
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Fatalf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
		}

		// Handle map must be empty after terminal transition
		if count := harness.TrackedSandboxesCount(); count != 0 {
			t.Errorf("expected tracked sandboxes count 0 after terminal transition to Failed, got %d", count)
		}
	})
}

type scriptedMultiToolLLM struct {
	mu           sync.Mutex
	callCount    int
	step3Entered chan struct{}
	step3Allowed chan struct{}
}

func (s *scriptedMultiToolLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	s.mu.Lock()
	s.callCount++
	count := s.callCount
	s.mu.Unlock()

	switch count {
	case 1:
		args, _ := json.Marshal(map[string]any{"command": "echo step1"})
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-1",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
				},
			},
		}, nil
	case 2:
		args, _ := json.Marshal(map[string]any{"command": "echo step2"})
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role: "assistant",
						ToolCalls: []llm.ToolCall{
							{
								ID:   "call-2",
								Type: "function",
								Function: llm.FunctionCall{
									Name:      "run_command",
									Arguments: string(args),
								},
							},
						},
					},
				},
			},
		}, nil
	default:
		if s.step3Entered != nil {
			select {
			case <-s.step3Entered:
			default:
				close(s.step3Entered)
			}
		}
		if s.step3Allowed != nil {
			<-s.step3Allowed
		}
		content := "done"
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{
				{
					Message: llm.Message{
						Role:    "assistant",
						Content: &content,
					},
				},
			},
		}, nil
	}
}

func findGoroutinesContaining(target string) []string {
	buf := make([]byte, 64*1024)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	var matches []string
	for _, g := range strings.Split(stacks, "\n\n") {
		if strings.Contains(g, target) {
			matches = append(matches, g)
		}
	}
	return matches
}

func TestOrchestrator_PrepareRun_DeadlineAndShutdownTime(t *testing.T) {
	t.Run("goal with budget.maxDuration 30m sets deadline 30m out and shutdownTime with grace", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

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

		currentTime := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
		wantDeadline := currentTime.Add(30 * time.Minute)
		runID := "20261110T100000Z-budget30m"
		run := &state.Run{
			ID:       runID,
			Repo:     repo.Key(),
			GoalName: "budget-30m",
			Phase:    state.PhasePending,
			Deadline: &wantDeadline,
			Goal: &goals.Goal{
				Name: "budget-30m",
				Budget: &goals.Budget{
					MaxDuration: "30m",
				},
			},
		}

		harness, agentsCS, _ := setupTestHarness(t, nil)
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})
		putTestRun(t, ctx, orch, run)

		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhasePreparing {
			t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
		}

		if gotRun.Deadline == nil || !gotRun.Deadline.Equal(wantDeadline) {
			t.Errorf("expected Deadline %v, got %v", wantDeadline, gotRun.Deadline)
		}

		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get sandbox: %v", err)
		}
		wantShutdown := wantDeadline.Add(SandboxShutdownGrace)
		if sb.Spec.ShutdownTime == nil || !sb.Spec.ShutdownTime.Time.Equal(wantShutdown) {
			t.Errorf("expected sandbox shutdown time %v, got %v", wantShutdown, sb.Spec.ShutdownTime)
		}
	})

	t.Run("goal without budget.maxDuration uses default max duration", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

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

		currentTime := time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)
		wantDeadline := currentTime.Add(DefaultMaxDuration)
		runID := "20261110T100000Z-nobudget"
		run := &state.Run{
			ID:       runID,
			Repo:     repo.Key(),
			GoalName: "no-budget",
			Phase:    state.PhasePending,
			Deadline: &wantDeadline,
			Goal: &goals.Goal{
				Name: "no-budget",
			},
		}

		harness, agentsCS, _ := setupTestHarness(t, nil)
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})
		putTestRun(t, ctx, orch, run)

		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhasePreparing {
			t.Errorf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
		}

		if gotRun.Deadline == nil || !gotRun.Deadline.Equal(wantDeadline) {
			t.Errorf("expected Deadline %v, got %v", wantDeadline, gotRun.Deadline)
		}

		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get sandbox: %v", err)
		}
		wantShutdown := wantDeadline.Add(SandboxShutdownGrace)
		if sb.Spec.ShutdownTime == nil || !sb.Spec.ShutdownTime.Time.Equal(wantShutdown) {
			t.Errorf("expected sandbox shutdown time %v, got %v", wantShutdown, sb.Spec.ShutdownTime)
		}
	})

	t.Run("goal with invalid budget.maxDuration fails run with clear event", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

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
			Goal: &goals.Goal{
				Name: "bad-budget",
				Budget: &goals.Budget{
					MaxDuration: "invalid-duration-value",
				},
			},
			BaseCommit: "commit-base123",
		}
		if err := store.PutGoal(ctx, repo.Key(), goalRec); err != nil {
			t.Fatalf("PutGoal: %v", err)
		}

		harness, agentsCS, _ := setupTestHarness(t, nil)
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})

		scheduler, err := NewScheduler(SchedulerOptions{
			Store:        store,
			Orchestrator: orch,
		})
		if err != nil {
			t.Fatalf("NewScheduler: %v", err)
		}

		_, err = scheduler.StartRun(ctx, repo, "bad-budget", StartOptions{Reason: "test"})
		if err == nil || !strings.Contains(err.Error(), "invalid budget.maxDuration") {
			t.Fatalf("expected error containing 'invalid budget.maxDuration', got %v", err)
		}

		// Ensure sandbox was not created
		sbList, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("List sandboxes: %v", err)
		}
		if len(sbList.Items) != 0 {
			t.Errorf("expected 0 sandboxes created, got %d", len(sbList.Items))
		}
	})
}

func TestOrchestrator_Session_NeverUpdatesSandboxShutdownTime(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())

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

	runID := "20261030T120000Z-no-extend-test"
	sandboxName := "run-no-extend-test"
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "no-extend-test",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sandboxName,
		Goal: &goals.Goal{
			Name: "no-extend-test",
			Body: "test goal",
		},
	}

	step1Finished := make(chan struct{})
	step2Entered := make(chan struct{})
	step2Allowed := make(chan struct{})
	step2Finished := make(chan struct{})
	step3Entered := make(chan struct{})
	step3Allowed := make(chan struct{})

	procSrv := &blockingProcessServer{
		onCommand: func(cmd []string) {
			for _, arg := range cmd {
				if strings.Contains(arg, "step1") {
					close(step1Finished)
					return
				}
				if strings.Contains(arg, "step2") {
					close(step2Entered)
					<-step2Allowed
					close(step2Finished)
					return
				}
			}
		},
	}

	scriptedLLM := &scriptedMultiToolLLM{
		step3Entered: step3Entered,
		step3Allowed: step3Allowed,
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, procSrv, scriptedLLM)

	currentTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
	initialShutdown := currentTime.Add(1*time.Hour + SandboxShutdownGrace)
	policy := sandboxv1beta1.ShutdownPolicyDelete
	_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxName,
			Namespace: "ao-sandboxes",
		},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{
				ShutdownTime:   &metav1.Time{Time: initialShutdown},
				ShutdownPolicy: &policy,
			},
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
		t.Fatalf("creating sandbox: %v", err)
	}

	clockMu := sync.Mutex{}
	getClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return currentTime
	}
	advanceClock := func(d time.Duration) time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		currentTime = currentTime.Add(d)
		return currentTime
	}

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		Clock:        getClock,
		MaxTurns:     30,
	})
	putTestRun(t, ctx, orch, run)

	// 1. First reconcile pass creates the root Session CRD. RequeueAfter should be 0.
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("orch.Reconcile start: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got %v", res.RequeueAfter)
	}

	sessDone := make(chan error, 1)
	go func() {
		sessDone <- reconcileSessionUntilTerminal(ctx, orch, topLevelSessionID(runID, ""))
	}()

	// Wait for tool 1 to execute and finish, and tool 2 to be entered and blocked
	select {
	case <-step1Finished:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 1 to execute")
	}

	select {
	case <-step2Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 2 to be entered")
	}

	// While blocked in step 2: advance clock and reconcile while session is running
	advanceClock(5 * time.Minute)
	res1, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("orch.Reconcile after tool 1: %v", err)
	}
	if res1.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got %v", res1.RequeueAfter)
	}

	// Verify sandbox ShutdownTime was NEVER updated during session
	sb1, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if sb1.Spec.ShutdownTime == nil || !sb1.Spec.ShutdownTime.Time.Equal(initialShutdown) {
		t.Errorf("expected sandbox shutdown time to remain %v, got %v", initialShutdown, sb1.Spec.ShutdownTime)
	}

	// 2. Allow tool 2 to complete and wait for step 3 to be entered
	close(step2Allowed)
	select {
	case <-step2Finished:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 2 to finish")
	}

	select {
	case <-step3Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for step 3 to be entered")
	}

	// Advance clock again and reconcile while session is waiting on step3Allowed
	advanceClock(10 * time.Minute)
	res2, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("orch.Reconcile after tool 2: %v", err)
	}
	if res2.RequeueAfter != 0 {
		t.Errorf("expected RequeueAfter 0, got %v", res2.RequeueAfter)
	}

	// Verify sandbox ShutdownTime still unchanged after second tool call
	sb2, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	if sb2.Spec.ShutdownTime == nil || !sb2.Spec.ShutdownTime.Time.Equal(initialShutdown) {
		t.Errorf("expected sandbox shutdown time to remain unchanged %v, got %v", initialShutdown, sb2.Spec.ShutdownTime)
	}

	// 3. Allow LLM call 3 to complete ("done")
	close(step3Allowed)

	select {
	case err := <-sessDone:
		if err != nil {
			t.Fatalf("reconcileSessionUntilTerminal: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session to finish")
	}

	// Reconcile to consume terminal Session outcome
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("orch.Reconcile outcome: %v", err)
	}

	gotRunFinal, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRunFinal.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRunFinal.Phase)
	}

	// Verify sandbox was deleted upon run completion
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sandboxName, metav1.GetOptions{})
	if err == nil {
		t.Errorf("expected sandbox %s to be deleted upon run completion, but still exists", sandboxName)
	} else if !k8serrors.IsNotFound(err) {
		t.Errorf("expected NotFound error, got: %v", err)
	}

	// Verify no goroutine other than test is left after turn ends
	leftoverWorkers := findGoroutinesContaining("runTurnWorker")
	if len(leftoverWorkers) != 0 {
		t.Errorf("expected 0 worker goroutines left after turn ends, got %d: %v", len(leftoverWorkers), leftoverWorkers)
	}
}

func TestOrchestrator_Working_MissingOrExpiredSandboxAtStart_CarriesOn(t *testing.T) {
	t.Run("already expired sandbox at worker start", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

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

		runID := "20261030T120000Z-expired-start"
		sandboxName := "run-expired-start"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "expired-start",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
			Goal: &goals.Goal{
				Name: "expired-start",
				Body: "test goal",
			},
		}

		harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})

		currentTime := time.Date(2026, 10, 30, 12, 10, 0, 0, time.UTC)
		// Shutdown time in the past
		pastShutdown := currentTime.Add(-5 * time.Minute)
		policy := sandboxv1beta1.ShutdownPolicyDelete
		_, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(ctx, &sandboxv1beta1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sandboxName,
				Namespace: "ao-sandboxes",
			},
			Spec: sandboxv1beta1.SandboxSpec{
				Lifecycle: sandboxv1beta1.Lifecycle{
					ShutdownTime:   &metav1.Time{Time: pastShutdown},
					ShutdownPolicy: &policy,
				},
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
			t.Fatalf("creating sandbox: %v", err)
		}

		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			Clock: func() time.Time {
				return currentTime
			},
		})
		putTestRun(t, ctx, orch, run)

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("reconcileWorkingRun: %v", err)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseNoImprovement {
			t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
		}

		events, err := store.ReadEvents(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		for _, e := range events {
			if e.Kind == "sandbox_expired" {
				t.Errorf("unexpected sandbox_expired event: %+v", e)
			}
		}
	})

	t.Run("missing sandbox at worker start", func(t *testing.T) {
		ctx := t.Context()
		store := state.NewFileStore(t.TempDir())
		sessionStore := session.NewFileStore(t.TempDir())

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

		runID := "20261030T120000Z-missing-start"
		sandboxName := "run-missing-start"
		run := &state.Run{
			ID:          runID,
			Repo:        repo.Key(),
			GoalName:    "missing-start",
			Phase:       state.PhaseWorking,
			BaseCommit:  "base-sha",
			SandboxName: sandboxName,
			Goal: &goals.Goal{
				Name: "missing-start",
				Body: "test goal",
			},
		}

		harness, _, _ := setupTestHarnessWithLLM(t, nil, &llm.ElizaClient{})

		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
		})
		putTestRun(t, ctx, orch, run)

		if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("reconcileWorkingRun: %v", err)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseNoImprovement {
			t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
		}

		events, err := store.ReadEvents(ctx, repo.Key(), runID)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		for _, e := range events {
			if e.Kind == "sandbox_expired" {
				t.Errorf("unexpected sandbox_expired event: %+v", e)
			}
		}
	})
}

func TestOrchestratorOptions_Defaults(t *testing.T) {
	var opts OrchestratorOptions
	opts.InitDefaults()

	if opts.Clock == nil {
		t.Fatal("expected Clock to be defaulted, got nil")
	}
	if time.Since(opts.Clock()) > time.Minute {
		t.Errorf("expected Clock to return current time, got %v", opts.Clock())
	}
	if opts.MaxTurns != DefaultMaxTurns {
		t.Errorf("MaxTurns = %d, want %d", opts.MaxTurns, DefaultMaxTurns)
	}
	if opts.MaxConcurrentRuns != DefaultMaxConcurrentRuns {
		t.Errorf("MaxConcurrentRuns = %d, want %d", opts.MaxConcurrentRuns, DefaultMaxConcurrentRuns)
	}
	if opts.ReadyTimeout != DefaultReadyTimeout {
		t.Errorf("ReadyTimeout = %v, want %v", opts.ReadyTimeout, DefaultReadyTimeout)
	}
	if opts.VerifyTimeout != DefaultVerifyTimeout {
		t.Errorf("VerifyTimeout = %v, want %v", opts.VerifyTimeout, DefaultVerifyTimeout)
	}
	if opts.DefaultMaxDuration != DefaultMaxDuration {
		t.Errorf("DefaultMaxDuration = %v, want %v", opts.DefaultMaxDuration, DefaultMaxDuration)
	}
	if opts.WorkspaceSize != DefaultWorkspaceSize {
		t.Errorf("WorkspaceSize = %q, want %q", opts.WorkspaceSize, DefaultWorkspaceSize)
	}
	if opts.WorkspaceStorageClass != "" {
		t.Errorf("WorkspaceStorageClass = %q, want empty", opts.WorkspaceStorageClass)
	}

	// Verify custom non-zero fields are not overwritten
	customTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	customOpts := OrchestratorOptions{
		Clock:                 func() time.Time { return customTime },
		MaxTurns:              15,
		MaxConcurrentRuns:     2,
		ReadyTimeout:          10 * time.Second,
		VerifyTimeout:         2 * time.Minute,
		DefaultMaxDuration:    20 * time.Minute,
		WorkspaceStorageClass: "custom-sc",
		WorkspaceSize:         "25Gi",
	}
	customOpts.InitDefaults()
	if !customOpts.Clock().Equal(customTime) {
		t.Errorf("custom Clock was overwritten")
	}
	if customOpts.MaxTurns != 15 {
		t.Errorf("custom MaxTurns was overwritten: %d", customOpts.MaxTurns)
	}
	if customOpts.MaxConcurrentRuns != 2 {
		t.Errorf("custom MaxConcurrentRuns was overwritten: %d", customOpts.MaxConcurrentRuns)
	}
	if customOpts.ReadyTimeout != 10*time.Second {
		t.Errorf("custom ReadyTimeout was overwritten: %v", customOpts.ReadyTimeout)
	}
	if customOpts.VerifyTimeout != 2*time.Minute {
		t.Errorf("custom VerifyTimeout was overwritten: %v", customOpts.VerifyTimeout)
	}
	if customOpts.DefaultMaxDuration != 20*time.Minute {
		t.Errorf("custom DefaultMaxDuration was overwritten: %v", customOpts.DefaultMaxDuration)
	}
	if customOpts.WorkspaceStorageClass != "custom-sc" {
		t.Errorf("custom WorkspaceStorageClass was overwritten: %q", customOpts.WorkspaceStorageClass)
	}
	if customOpts.WorkspaceSize != "25Gi" {
		t.Errorf("custom WorkspaceSize was overwritten: %q", customOpts.WorkspaceSize)
	}
}

func TestOrchestratorOptions_AddFlags(t *testing.T) {
	var opts OrchestratorOptions
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	opts.AddFlags(fs)

	if err := fs.Parse([]string{
		"--max-turns", "25",
		"--max-concurrent-runs", "3",
		"--public-url", "https://flag.example.com",
		"--ready-timeout", "2m",
		"--verify-timeout", "5m",
		"--max-run-duration", "30m",
		"--workspace-storage-class", "fast-ssd",
		"--workspace-size", "50Gi",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if opts.MaxTurns != 25 {
		t.Errorf("MaxTurns = %d, want 25", opts.MaxTurns)
	}
	if opts.MaxConcurrentRuns != 3 {
		t.Errorf("MaxConcurrentRuns = %d, want 3", opts.MaxConcurrentRuns)
	}
	if opts.PublicURL != "https://flag.example.com" {
		t.Errorf("PublicURL = %q, want 'https://flag.example.com'", opts.PublicURL)
	}
	if opts.ReadyTimeout != 2*time.Minute {
		t.Errorf("ReadyTimeout = %v, want 2m", opts.ReadyTimeout)
	}
	if opts.VerifyTimeout != 5*time.Minute {
		t.Errorf("VerifyTimeout = %v, want 5m", opts.VerifyTimeout)
	}
	if opts.DefaultMaxDuration != 30*time.Minute {
		t.Errorf("DefaultMaxDuration = %v, want 30m", opts.DefaultMaxDuration)
	}
	if opts.WorkspaceStorageClass != "fast-ssd" {
		t.Errorf("WorkspaceStorageClass = %q, want 'fast-ssd'", opts.WorkspaceStorageClass)
	}
	if opts.WorkspaceSize != "50Gi" {
		t.Errorf("WorkspaceSize = %q, want '50Gi'", opts.WorkspaceSize)
	}
}

func TestOrchestratorOptions_Validate(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)

	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	baseValid := func() OrchestratorOptions {
		opts := OrchestratorOptions{
			Store:           store,
			Harness:         harness,
			KubeClient:      fakeClient,
			MaxSessionDepth: DefaultMaxSessionDepth,
		}
		opts.InitDefaults()
		return opts
	}

	base := baseValid()
	if err := base.Validate(); err != nil {
		t.Fatalf("expected valid options to pass, got: %v", err)
	}

	tests := []struct {
		name          string
		mutate        func(o *OrchestratorOptions)
		wantErrSubstr string
	}{
		{
			name: "missing store",
			mutate: func(o *OrchestratorOptions) {
				o.Store = nil
			},
			wantErrSubstr: "store",
		},
		{
			name: "missing harness",
			mutate: func(o *OrchestratorOptions) {
				o.Harness = nil
			},
			wantErrSubstr: "harness",
		},
		{
			name: "missing kube client",
			mutate: func(o *OrchestratorOptions) {
				o.KubeClient = nil
			},
			wantErrSubstr: "kube client",
		},
		{
			name: "negative MaxTurns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxTurns = -1
			},
			wantErrSubstr: "MaxTurns",
		},
		{
			name: "zero MaxTurns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxTurns = 0
			},
			wantErrSubstr: "MaxTurns",
		},
		{
			name: "negative MaxConcurrentRuns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxConcurrentRuns = -1
			},
			wantErrSubstr: "MaxConcurrentRuns",
		},
		{
			name: "zero MaxConcurrentRuns",
			mutate: func(o *OrchestratorOptions) {
				o.MaxConcurrentRuns = 0
			},
			wantErrSubstr: "MaxConcurrentRuns",
		},
		{
			name: "negative ReadyTimeout",
			mutate: func(o *OrchestratorOptions) {
				o.ReadyTimeout = -1 * time.Second
			},
			wantErrSubstr: "ReadyTimeout",
		},
		{
			name: "negative VerifyTimeout",
			mutate: func(o *OrchestratorOptions) {
				o.VerifyTimeout = -1 * time.Second
			},
			wantErrSubstr: "VerifyTimeout",
		},
		{
			name: "negative DefaultMaxDuration",
			mutate: func(o *OrchestratorOptions) {
				o.DefaultMaxDuration = -1 * time.Second
			},
			wantErrSubstr: "DefaultMaxDuration",
		},
		{
			name: "portal binder without policy factory",
			mutate: func(o *OrchestratorOptions) {
				o.PortalBinder = &fakePortalBinder{}
				o.PolicyFactory = nil
			},
			wantErrSubstr: "policy factory",
		},
		{
			name: "invalid WorkspaceSize",
			mutate: func(o *OrchestratorOptions) {
				o.WorkspaceSize = "not-a-valid-size"
			},
			wantErrSubstr: "WorkspaceSize",
		},
		{
			name: "MaxSessionDepth zero rejected",
			mutate: func(o *OrchestratorOptions) {
				o.MaxSessionDepth = 0
			},
			wantErrSubstr: "MaxSessionDepth must be at least 1",
		},
		{
			name: "MaxSessionDepth negative rejected",
			mutate: func(o *OrchestratorOptions) {
				o.MaxSessionDepth = -1
			},
			wantErrSubstr: "MaxSessionDepth must be at least 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseValid()
			tc.mutate(&opts)
			err := opts.Validate()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErrSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Errorf("error %q does not contain expected substring %q", err.Error(), tc.wantErrSubstr)
			}
		})
	}

	t.Run("NewOrchestrator propagates validation error", func(t *testing.T) {
		_, err := NewOrchestrator(OrchestratorOptions{
			Store:           store,
			Harness:         harness,
			KubeClient:      fakeClient,
			MaxSessionDepth: DefaultMaxSessionDepth,
			ReadyTimeout:    -1 * time.Minute,
		})
		if err == nil || !strings.Contains(err.Error(), "ReadyTimeout") {
			t.Fatalf("expected ReadyTimeout error from NewOrchestrator, got: %v", err)
		}
	})

	t.Run("valid with portal binder and policy factory", func(t *testing.T) {
		opts := baseValid()
		opts.PortalBinder = &fakePortalBinder{}
		opts.PolicyFactory = portal.HostListPolicyFactory(nil)
		if err := opts.Validate(); err != nil {
			t.Fatalf("expected valid options to pass, got: %v", err)
		}
	})
}

type midTurnBlockingLLM struct {
	started   chan struct{}
	release   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	cancOnce  sync.Once
}

func newMidTurnBlockingLLM() *midTurnBlockingLLM {
	return &midTurnBlockingLLM{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
}

func (m *midTurnBlockingLLM) CreateChatCompletion(ctx context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	m.once.Do(func() { close(m.started) })
	select {
	case <-ctx.Done():
		m.cancOnce.Do(func() { close(m.cancelled) })
		return nil, ctx.Err()
	case <-m.release:
		done := "done"
		return &llm.ChatCompletionResponse{
			Choices: []llm.Choice{{Message: llm.Message{Role: "assistant", Content: &done}}},
		}, nil
	}
}

func setupWorkingTestRun(t *testing.T, runID, sbName string, deadline *time.Time, llmClient llm.ChatClient) (*state.FileStore, *session.FileStore, *state.Repo, *state.Run, *session.Harness, *fakeagents.Clientset, client.Client) {
	t.Helper()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())
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
	run := &state.Run{
		ID:          runID,
		Repo:        repo.Key(),
		GoalName:    "test-goal",
		Phase:       state.PhaseWorking,
		BaseCommit:  "base-sha",
		SandboxName: sbName,
		Deadline:    deadline,
		Goal:        &goals.Goal{Name: "test-goal", Body: "body"},
	}
	harness, agentsCS, _ := setupTestHarnessWithLLM(t, nil, llmClient)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	fakeClient := newTestFakeClientBuilder(scheme).Build()
	putTestHeroRun(t, t.Context(), fakeClient, harness.Namespace(), repo.URL, run)
	policy := sandboxv1beta1.ShutdownPolicyDelete
	shutdownTime := time.Now().Add(time.Hour)
	if deadline != nil {
		shutdownTime = deadline.Add(15 * time.Minute)
	}
	_, err = agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Create(t.Context(), &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: sbName, Namespace: "ao-sandboxes", Labels: map[string]string{LabelRun: runID}},
		Spec: sandboxv1beta1.SandboxSpec{
			Lifecycle: sandboxv1beta1.Lifecycle{ShutdownTime: &metav1.Time{Time: shutdownTime}, ShutdownPolicy: &policy},
		},
		Status: sandboxv1beta1.SandboxStatus{Conditions: []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionTrue}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating sandbox: %v", err)
	}
	return store, sessionStore, repo, run, harness, agentsCS, fakeClient
}

func TestOrchestrator_Working_DeadlineExceededMidTurn(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261030T120000Z-deadline", "run-deadline"
	startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	deadline := startTime.Add(10 * time.Minute)

	blockingLLM := newMidTurnBlockingLLM()
	store, sessionStore, repo, run, harness, agentsCS, fakeClient := setupWorkingTestRun(t, runID, sbName, &deadline, blockingLLM)
	run.SetPhase(state.PhaseWorking, startTime)

	currentTime := startTime
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store: store, SessionStore: sessionStore, Harness: harness,
		Clock:      func() time.Time { return currentTime },
		KubeClient: fakeClient,
	})
	putTestRun(t, ctx, orch, run)

	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil || res.RequeueAfter != 10*time.Minute {
		t.Fatalf("Reconcile got res=%v err=%v, want RequeueAfter=10m", res, err)
	}

	sessName := topLevelSessionID(runID, "")
	sessCtx, cancelSess := context.WithCancel(ctx)
	defer cancelSess()
	sessDone := make(chan error, 1)
	go func() {
		sessDone <- reconcileSessionUntilTerminal(sessCtx, orch, sessName)
	}()

	select {
	case <-blockingLLM.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session to start LLM call")
	}

	currentTime = deadline.Add(time.Minute)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile after deadline: %v", err)
	}

	var sess herorunv1alpha1.Session
	if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: orch.namespace(), Name: sessName}, &sess); err != nil {
		t.Fatalf("Get Session: %v", err)
	}
	if sess.Status.Phase != herorunv1alpha1.SessionPhaseCanceled {
		t.Errorf("expected root Session phase %q, got %q", herorunv1alpha1.SessionPhaseCanceled, sess.Status.Phase)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil || gotRun.Phase != state.PhaseFailed {
		t.Fatalf("expected phase Failed, got %v (err %v)", gotRun.Phase, err)
	}

	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted, got err: %v", err)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundEvent bool
	for _, e := range events {
		if e.Phase == state.PhaseFailed && e.Kind == "deadline_exceeded" {
			foundEvent = true
			m, ok := e.Data.(map[string]any)
			if !ok || m["deadline"] != deadline.Format(time.RFC3339) || m["elapsed"] == "" {
				t.Errorf("expected deadline and elapsed in data, got: %+v", e.Data)
			}
			break
		}
	}
	if !foundEvent {
		t.Errorf("expected deadline_exceeded event, got events: %+v", events)
	}

	// Cancel in-flight session goroutine and verify later Reconcile keeps Failed.
	cancelSess()
	<-sessDone
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile on terminal run: %v", err)
	}
	gotRunAfter, _ := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if gotRunAfter.Phase != state.PhaseFailed {
		t.Errorf("expected phase to remain Failed, got %s", gotRunAfter.Phase)
	}
}

func TestOrchestrator_Working_SandboxDeletedMidTurn(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(ctx context.Context, cs *fakeagents.Clientset, sbName string) error
		wantMsg string
	}{
		{
			name: "deleted",
			mutate: func(ctx context.Context, cs *fakeagents.Clientset, sbName string) error {
				return cs.AgentsV1beta1().Sandboxes("ao-sandboxes").Delete(ctx, sbName, metav1.DeleteOptions{})
			},
			wantMsg: "deleted",
		},
		{
			name: "failed condition",
			mutate: func(ctx context.Context, cs *fakeagents.Clientset, sbName string) error {
				sbObj, err := cs.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{})
				if err != nil {
					return err
				}
				sbObj.Status.Conditions = append(sbObj.Status.Conditions, metav1.Condition{
					Type:    string(sandboxv1beta1.SandboxConditionFinished),
					Status:  metav1.ConditionTrue,
					Reason:  sandboxv1beta1.SandboxReasonPodFailed,
					Message: "pod terminated with error",
				})
				_, err = cs.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sbObj, metav1.UpdateOptions{})
				return err
			},
			wantMsg: "pod terminated with error",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			runID, sbName := "20261030T120000Z-"+tc.name, "run-"+tc.name
			startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
			deadline := startTime.Add(30 * time.Minute)

			blockingLLM := newMidTurnBlockingLLM()
			store, sessionStore, repo, _, harness, agentsCS, fakeClient := setupWorkingTestRun(t, runID, sbName, &deadline, blockingLLM)

			orch := newTestOrchestrator(t, OrchestratorOptions{
				Store: store, SessionStore: sessionStore, Harness: harness,
				Clock:      func() time.Time { return startTime },
				KubeClient: fakeClient,
			})

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			sessDone := make(chan error, 1)
			go func() {
				sessDone <- reconcileSessionUntilTerminal(ctx, orch, topLevelSessionID(runID, ""))
			}()

			select {
			case <-blockingLLM.started:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for session to start LLM call")
			}

			if err := tc.mutate(ctx, agentsCS, sbName); err != nil {
				t.Fatalf("mutating sandbox: %v", err)
			}

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
				t.Fatalf("Reconcile after sandbox change: %v", err)
			}

			select {
			case <-blockingLLM.cancelled:
				t.Fatal("session context should not be cancelled on sandbox deletion")
			default:
			}

			close(blockingLLM.release)
			select {
			case err := <-sessDone:
				if err != nil {
					t.Fatalf("reconcileSessionUntilTerminal: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for session to finish")
			}

			if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
				t.Fatalf("Reconcile after session finish: %v", err)
			}

			gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
			if err != nil || gotRun.Phase != state.PhaseNoImprovement {
				t.Fatalf("expected phase NoImprovement, got %v (err %v)", gotRun.Phase, err)
			}

			events, err := store.ReadEvents(ctx, repo.Key(), runID)
			if err != nil {
				t.Fatalf("ReadEvents: %v", err)
			}
			for _, e := range events {
				if e.Kind == "sandbox_deleted" {
					t.Errorf("unexpected sandbox_deleted event: %+v", e)
				}
			}
		})
	}
}

func TestOrchestrator_Working_RequeueRemainingDeadline(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261030T120000Z-requeue", "run-requeue"
	startTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	deadline := startTime.Add(25 * time.Minute)

	blockingLLM := newMidTurnBlockingLLM()
	store, sessionStore, repo, _, harness, _, fakeClient := setupWorkingTestRun(t, runID, sbName, &deadline, blockingLLM)

	currentTime := startTime
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store: store, SessionStore: sessionStore, Harness: harness,
		Clock:      func() time.Time { return currentTime },
		KubeClient: fakeClient,
	})

	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil || res.RequeueAfter != 25*time.Minute {
		t.Fatalf("Reconcile got res=%v err=%v, want RequeueAfter=25m", res, err)
	}

	// Reconcile while root Session is non-terminal should return remaining time
	currentTime = startTime.Add(5 * time.Minute)
	res2, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil || res2.RequeueAfter != 20*time.Minute {
		t.Fatalf("Reconcile active got res2=%v err=%v, want RequeueAfter=20m", res2, err)
	}
}

type fakePortalBinder struct {
	mu       sync.Mutex
	calls    []portalBindCall
	bindFunc func(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error
}

type portalBindCall struct {
	PodUID types.UID
	Policy *portalsv1.EgressPolicy
}

func (f *fakePortalBinder) Bind(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, portalBindCall{PodUID: podUID, Policy: policy})
	if f.bindFunc != nil {
		return f.bindFunc(ctx, podUID, policy)
	}
	return nil
}

func TestOrchestrator_PortalBinder_BindBeforeCommands(t *testing.T) {
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

	runID := "20261030T120000Z-bind1"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "success",
	}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	binder := &fakePortalBinder{}
	binder.bindFunc = func(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
		procSrv.mu.Lock()
		defer procSrv.mu.Unlock()
		if len(procSrv.commands) > 0 {
			t.Errorf("expected 0 commands executed before bind, got %d", len(procSrv.commands))
		}
		return nil
	}

	fixedTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	egressHosts := []string{"github.com", "golang.org"}
	var factoryCalls []portal.PolicyRequest
	policyFactory := func(ctx context.Context, req portal.PolicyRequest) (*portalsv1.EgressPolicy, error) {
		factoryCalls = append(factoryCalls, req)
		return portal.HostListPolicyFactory(egressHosts)(ctx, req)
	}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		Clock:         func() time.Time { return fixedTime },
		PortalBinder:  binder,
		PolicyFactory: policyFactory,
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Reconcile Pending -> Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Fatalf("expected phase %s, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Verify sandbox pod template has the interception label
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	podLabels := sb.Spec.SandboxBlueprint.PodTemplate.ObjectMeta.Labels
	if podLabels[session.PortalInterceptLabel] != "true" {
		t.Errorf("expected sandbox template label %s=true, got %+v", session.PortalInterceptLabel, podLabels)
	}

	// Ensure no bind was called yet
	binder.mu.Lock()
	callsBeforeReady := len(binder.calls)
	binder.mu.Unlock()
	if callsBeforeReady != 0 {
		t.Fatalf("expected 0 bind calls before ready, got %d", callsBeforeReady)
	}

	// Create backing pod in fake k8s
	podUID := types.UID("pod-uid-test-12345")
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       podUID,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Reconcile Preparing -> Working (binds, then clones and checkouts)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 2: %v", err)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun after step 2: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}

	if len(factoryCalls) != 1 {
		t.Fatalf("expected 1 factory call, got %d", len(factoryCalls))
	}
	if factoryCalls[0].RepoURL != repo.URL {
		t.Errorf("factory req RepoURL = %q, want %q", factoryCalls[0].RepoURL, repo.URL)
	}
	if factoryCalls[0].GoalName != run.GoalName {
		t.Errorf("factory req GoalName = %q, want %q", factoryCalls[0].GoalName, run.GoalName)
	}
	if factoryCalls[0].RunID != run.ID {
		t.Errorf("factory req RunID = %q, want %q", factoryCalls[0].RunID, run.ID)
	}
	if factoryCalls[0].SandboxName != gotRun.SandboxName {
		t.Errorf("factory req SandboxName = %q, want %q", factoryCalls[0].SandboxName, gotRun.SandboxName)
	}

	binder.mu.Lock()
	numCalls := len(binder.calls)
	var call portalBindCall
	if numCalls > 0 {
		call = binder.calls[0]
	}
	binder.mu.Unlock()

	if numCalls != 1 {
		t.Fatalf("expected exactly 1 bind call, got %d", numCalls)
	}
	if call.PodUID != podUID {
		t.Errorf("expected pod UID %q, got %q", podUID, call.PodUID)
	}
	wantPolicy, err := portal.PolicyForHosts(egressHosts)
	if err != nil {
		t.Fatalf("PolicyForHosts: %v", err)
	}
	if call.Policy == nil || len(call.Policy.Rules) != len(wantPolicy.Rules) || call.Policy.Rules[0].Https == nil {
		t.Fatalf("expected policy matching %+v, got %+v", wantPolicy, call.Policy)
	}
	if !slices.Equal(call.Policy.Rules[0].Https.Hostnames, wantPolicy.Rules[0].Https.Hostnames) {
		t.Errorf("hostnames = %v, want %v", call.Policy.Rules[0].Https.Hostnames, wantPolicy.Rules[0].Https.Hostnames)
	}

	procSrv.mu.Lock()
	cmds := len(procSrv.commands)
	procSrv.mu.Unlock()
	if cmds != 3 {
		t.Errorf("expected 3 commands executed after bind, got %d", cmds)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundPortalBound bool
	for _, e := range events {
		if e.Kind == "portal_bound" {
			foundPortalBound = true
			if m, ok := e.Data.(map[string]any); ok {
				if hosts, ok := m["hosts"].([]any); ok {
					var gotHosts []string
					for _, h := range hosts {
						if s, ok := h.(string); ok {
							gotHosts = append(gotHosts, s)
						}
					}
					if !slices.Equal(gotHosts, egressHosts) {
						t.Errorf("portal_bound hosts = %v, want %v", gotHosts, egressHosts)
					}
				} else if gotHosts, ok := m["hosts"].([]string); ok {
					if !slices.Equal(gotHosts, egressHosts) {
						t.Errorf("portal_bound hosts = %v, want %v", gotHosts, egressHosts)
					}
				}
			}
		}
	}
	if !foundPortalBound {
		t.Errorf("expected portal_bound event, got events: %+v", events)
	}
}

func TestOrchestrator_PortalBinder_FailsAfterDeadline(t *testing.T) {
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

	runID := "20261030T120000Z-bindfail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}

	procSrv := &mockProcessServer{}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	bindErr := errors.New("portal controller unreachable")
	binder := &fakePortalBinder{
		bindFunc: func(ctx context.Context, podUID types.UID, policy *portalsv1.EgressPolicy) error {
			return bindErr
		},
	}

	clockMu := sync.Mutex{}
	currentTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	getClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return currentTime
	}

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		Clock:         getClock,
		PortalBinder:  binder,
		PolicyFactory: portal.HostListPolicyFactory(nil),
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Pending -> Preparing (sets ReadyDeadline)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline set")
	}

	// Create backing pod in fake k8s
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       types.UID("pod-uid-fail"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
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

	// Reconcile pass 1: before deadline, bind fails -> should requeue for retry
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("expected Reconcile to handle retry cleanly, got err: %v", err)
	}
	if res.RequeueAfter != portalBindRetryInterval {
		t.Errorf("expected RequeueAfter == %v for retry, got %v", portalBindRetryInterval, res.RequeueAfter)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s on retry, got %s", state.PhasePreparing, gotRun.Phase)
	}

	// Advance clock past ReadyDeadline
	clockMu.Lock()
	currentTime = gotRun.ReadyDeadline.Add(time.Minute)
	clockMu.Unlock()

	// Reconcile pass 2: after deadline, bind fails -> should fail the run with portal_bind_failed
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
		t.Logf("Reconcile returned nil error (as terminal failures are logged)")
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s after deadline, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "portal controller unreachable") {
		t.Errorf("expected error to mention 'portal controller unreachable', got %q", gotRun.Error)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundBindFailed bool
	for _, e := range events {
		if e.Kind == "portal_bind_failed" {
			foundBindFailed = true
			if !strings.Contains(e.Error, "portal controller unreachable") {
				t.Errorf("expected portal_bind_failed error %q to contain 'portal controller unreachable'", e.Error)
			}
		}
	}
	if !foundBindFailed {
		t.Errorf("expected portal_bind_failed event, got events: %+v", events)
	}

	// Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted, got err: %v", err)
	}
}

func TestOrchestrator_PortalBinder_FactoryErrorFailsAfterDeadline(t *testing.T) {
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

	runID := "20261030T120000Z-factoryfail"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}

	procSrv := &mockProcessServer{}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	binder := &fakePortalBinder{}
	factoryErr := errors.New("egress policy generation failed")
	policyFactory := func(ctx context.Context, req portal.PolicyRequest) (*portalsv1.EgressPolicy, error) {
		return nil, factoryErr
	}

	clockMu := sync.Mutex{}
	currentTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	getClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return currentTime
	}

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:         store,
		Harness:       harness,
		Clock:         getClock,
		PortalBinder:  binder,
		PolicyFactory: policyFactory,
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Pending -> Preparing (sets ReadyDeadline)
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.ReadyDeadline == nil {
		t.Fatal("expected ReadyDeadline set")
	}

	// Create backing pod in fake k8s
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       types.UID("pod-uid-factoryfail"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
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

	// Reconcile pass 1: before deadline, factory fails -> should requeue for retry
	res, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID))
	if err != nil {
		t.Fatalf("expected Reconcile to handle retry cleanly, got err: %v", err)
	}
	if res.RequeueAfter != portalBindRetryInterval {
		t.Errorf("expected RequeueAfter == %v for retry, got %v", portalBindRetryInterval, res.RequeueAfter)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhasePreparing {
		t.Errorf("expected phase %s on retry, got %s", state.PhasePreparing, gotRun.Phase)
	}
	binder.mu.Lock()
	bindCalls := len(binder.calls)
	binder.mu.Unlock()
	if bindCalls != 0 {
		t.Errorf("expected 0 bind calls when factory fails, got %d", bindCalls)
	}

	// Advance clock past ReadyDeadline
	clockMu.Lock()
	currentTime = gotRun.ReadyDeadline.Add(time.Minute)
	clockMu.Unlock()

	// Reconcile pass 2: after deadline, factory fails -> should fail the run with portal_bind_failed
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err == nil {
		t.Logf("Reconcile returned nil error (as terminal failures are logged)")
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseFailed {
		t.Errorf("expected phase %s after deadline, got %s", state.PhaseFailed, gotRun.Phase)
	}
	if !strings.Contains(gotRun.Error, "egress policy generation failed") {
		t.Errorf("expected error to mention 'egress policy generation failed', got %q", gotRun.Error)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundBindFailed bool
	for _, e := range events {
		if e.Kind == "portal_bind_failed" {
			foundBindFailed = true
			if !strings.Contains(e.Error, "egress policy generation failed") {
				t.Errorf("expected portal_bind_failed error %q to contain 'egress policy generation failed'", e.Error)
			}
		}
	}
	if !foundBindFailed {
		t.Errorf("expected portal_bind_failed event, got events: %+v", events)
	}

	// Verify sandbox was deleted
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("expected sandbox to be deleted, got err: %v", err)
	}
}

func TestOrchestrator_PortalBinder_DisabledWithoutBinder(t *testing.T) {
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

	runID := "20261030T120000Z-nobinder"
	run := &state.Run{
		ID:         runID,
		Repo:       repo.Key(),
		GoalName:   "test-goal",
		BaseCommit: "abc1234",
		Phase:      state.PhasePending,
		Goal: &goals.Goal{
			Name: "test-goal",
		},
	}

	procSrv := &mockProcessServer{
		exitCode: 0,
		stdout:   "success",
	}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	fixedTime := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		Harness:      harness,
		Clock:        func() time.Time { return fixedTime },
		PortalBinder: nil, // disabled
	})
	putTestRun(t, ctx, orch, run)

	// Step 1: Reconcile Pending -> Preparing
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 1: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}

	// Verify sandbox does NOT have the interception label
	sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, gotRun.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting sandbox: %v", err)
	}
	podLabels := sb.Spec.SandboxBlueprint.PodTemplate.ObjectMeta.Labels
	if podLabels != nil && podLabels[session.PortalInterceptLabel] != "" {
		t.Errorf("expected no interception label, got %q", podLabels[session.PortalInterceptLabel])
	}

	// Create backing pod in fake k8s
	_, err = coreCS.CoreV1().Pods("ao-sandboxes").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gotRun.SandboxName,
			Namespace: "ao-sandboxes",
			UID:       types.UID("pod-uid-nobinder"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating backing pod: %v", err)
	}

	// Mark sandbox Ready
	sb.Status.Conditions = []metav1.Condition{
		{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		},
	}
	if _, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Update(ctx, sb, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating sandbox to Ready: %v", err)
	}

	// Step 2: Reconcile Preparing -> Working
	if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
		t.Fatalf("Reconcile step 2: %v", err)
	}

	gotRun, err = GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun after step 2: %v", err)
	}
	if gotRun.Phase != state.PhaseWorking {
		t.Errorf("expected phase %s, got %s", state.PhaseWorking, gotRun.Phase)
	}

	events, err := store.ReadEvents(ctx, repo.Key(), runID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for _, e := range events {
		if e.Kind == "portal_bound" || e.Kind == "portal_bind_failed" {
			t.Errorf("unexpected portal event %q when binder is nil", e.Kind)
		}
	}
}

func TestOrchestrator_WorkspaceClaim(t *testing.T) {
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
	harness, agentsCS, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	getSandbox := func(t *testing.T, sbName string) *sandboxv1beta1.Sandbox {
		t.Helper()
		sb, err := agentsCS.AgentsV1beta1().Sandboxes("ao-sandboxes").Get(ctx, sbName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting sandbox %s: %v", sbName, err)
		}
		return sb
	}

	getWorkspaceVol := func(t *testing.T, sbName string) *corev1.Volume {
		t.Helper()
		sb := getSandbox(t, sbName)
		for i := range sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes {
			if sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes[i].Name == "workspace" {
				return &sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes[i]
			}
		}
		t.Fatal("expected workspace volume on sandbox")
		return nil
	}

	t.Run("with StorageClass set claim exists and sandbox mounts it", func(t *testing.T) {
		runID := "20261030T120000Z-scset"
		fakeClient := newTestFakeClientBuilder(scheme).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store: store, Harness: harness, KubeClient: fakeClient,
			WorkspaceStorageClass: "standard", WorkspaceSize: "20Gi",
		})
		putTestRun(t, ctx, orch, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		name := sandboxNameForRun(runID)
		var claim corev1.PersistentVolumeClaim
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &claim); err != nil {
			t.Fatalf("fakeClient.Get claim %s: %v", name, err)
		}
		if claim.Spec.StorageClassName == nil || *claim.Spec.StorageClassName != "standard" {
			t.Errorf("expected StorageClassName 'standard', got %v", claim.Spec.StorageClassName)
		}
		if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOncePod {
			t.Errorf("expected AccessModes [ReadWriteOncePod], got %v", claim.Spec.AccessModes)
		}
		storageQty := claim.Spec.Resources.Requests[corev1.ResourceStorage]
		if !storageQty.Equal(resource.MustParse("20Gi")) {
			t.Errorf("expected storage size '20Gi', got %s", storageQty.String())
		}

		var hr herorunv1alpha1.HeroRun
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &hr); err != nil {
			t.Fatalf("fakeClient.Get herorun %s: %v", name, err)
		}
		if len(claim.OwnerReferences) != 1 {
			t.Fatalf("expected 1 owner reference on claim, got %d", len(claim.OwnerReferences))
		}
		owner := claim.OwnerReferences[0]
		if owner.Kind != "HeroRun" || owner.Name != hr.Name || owner.UID != hr.UID || owner.Controller == nil || !*owner.Controller {
			t.Errorf("unexpected claim owner reference: %+v", owner)
		}

		vol := getWorkspaceVol(t, name)
		if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != name || vol.EmptyDir != nil {
			t.Errorf("expected workspace volume to mount claim %s, got %+v", name, vol)
		}

		sb := getSandbox(t, name)
		for _, v := range sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes {
			if v.Name == "home" {
				t.Errorf("expected no separate 'home' volume when claim is set, got %+v", v)
			}
		}
		var wsMount, homeMount *corev1.VolumeMount
		for i := range sb.Spec.SandboxBlueprint.PodTemplate.Spec.Containers[0].VolumeMounts {
			m := &sb.Spec.SandboxBlueprint.PodTemplate.Spec.Containers[0].VolumeMounts[i]
			if m.MountPath == "/workspace" {
				wsMount = m
			}
			if m.MountPath == session.DefaultHomeDir {
				homeMount = m
			}
		}
		if wsMount == nil || wsMount.Name != "workspace" || wsMount.SubPath != "workspace" {
			t.Errorf("expected /workspace mount with name 'workspace' and subPath 'workspace', got %+v", wsMount)
		}
		if homeMount == nil || homeMount.Name != "workspace" || homeMount.SubPath != "home" {
			t.Errorf("expected home mount with name 'workspace' and subPath 'home', got %+v", homeMount)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.WorkspaceClaimName != name {
			t.Errorf("expected WorkspaceClaimName %q, got %q", name, gotRun.WorkspaceClaimName)
		}
	})

	t.Run("second pass reuses existing claim", func(t *testing.T) {
		runID := "20261030T120000Z-reuse"
		name := sandboxNameForRun(runID)
		const existingUID = types.UID("existing-claim-uid-123")
		scName := "standard"
		existingClaim := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ao-sandboxes", UID: existingUID},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
			},
		}
		fakeClient := newTestFakeClientBuilder(scheme).WithObjects(existingClaim).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store: store, Harness: harness, KubeClient: fakeClient,
			WorkspaceStorageClass: "standard",
		})
		putTestRun(t, ctx, orch, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		var claim corev1.PersistentVolumeClaim
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &claim); err != nil {
			t.Fatalf("fakeClient.Get: %v", err)
		}
		if claim.UID != existingUID {
			t.Errorf("expected existing claim UID %q, got %q", existingUID, claim.UID)
		}

		vol := getWorkspaceVol(t, name)
		if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != name {
			t.Errorf("expected workspace volume to mount claim %s, got %+v", name, vol)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.WorkspaceClaimName != name {
			t.Errorf("expected WorkspaceClaimName %q, got %q", name, gotRun.WorkspaceClaimName)
		}
	})

	t.Run("with no StorageClass no claim is created and sandbox uses emptyDir", func(t *testing.T) {
		runID := "20261030T120000Z-noclaim"
		fakeClient := newTestFakeClientBuilder(scheme).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store: store, Harness: harness, KubeClient: fakeClient,
		})
		putTestRun(t, ctx, orch, &state.Run{ID: runID, Repo: repo.Key(), GoalName: "goal", Phase: state.PhasePending})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		var claimList corev1.PersistentVolumeClaimList
		if err := fakeClient.List(ctx, &claimList, client.InNamespace("ao-sandboxes")); err != nil {
			t.Fatalf("fakeClient.List: %v", err)
		}
		if len(claimList.Items) != 0 {
			t.Errorf("expected 0 claims, got %d", len(claimList.Items))
		}

		name := sandboxNameForRun(runID)
		vol := getWorkspaceVol(t, name)
		if vol.EmptyDir == nil || vol.PersistentVolumeClaim != nil {
			t.Errorf("expected emptyDir volume for workspace, got %+v", vol)
		}

		sb := getSandbox(t, name)
		var foundHomeVol bool
		for _, v := range sb.Spec.SandboxBlueprint.PodTemplate.Spec.Volumes {
			if v.Name == "home" && v.EmptyDir != nil {
				foundHomeVol = true
			}
		}
		if !foundHomeVol {
			t.Error("expected emptyDir volume for home")
		}
		var wsMount, homeMount *corev1.VolumeMount
		for i := range sb.Spec.SandboxBlueprint.PodTemplate.Spec.Containers[0].VolumeMounts {
			m := &sb.Spec.SandboxBlueprint.PodTemplate.Spec.Containers[0].VolumeMounts[i]
			if m.MountPath == "/workspace" {
				wsMount = m
			}
			if m.MountPath == session.DefaultHomeDir {
				homeMount = m
			}
		}
		if wsMount == nil || wsMount.Name != "workspace" || wsMount.SubPath != "" {
			t.Errorf("expected /workspace mount with name 'workspace' and empty subPath, got %+v", wsMount)
		}
		if homeMount == nil || homeMount.Name != "home" || homeMount.SubPath != "" {
			t.Errorf("expected home mount with name 'home' and empty subPath, got %+v", homeMount)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.WorkspaceClaimName != "" {
			t.Errorf("expected empty WorkspaceClaimName, got %q", gotRun.WorkspaceClaimName)
		}
	})

	t.Run("with StorageClass set fails when HeroRun owner is unavailable", func(t *testing.T) {
		runID := "20261030T120000Z-noowner"
		name := sandboxNameForRun(runID)
		hrWithoutUID := &herorunv1alpha1.HeroRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "ao-sandboxes",
				UID:       "", // No UID means heroRunOwnerReferences returns nil (unowned)
			},
			Spec: herorunv1alpha1.HeroRunSpec{
				RunID:      runID,
				Repository: repo.URL,
			},
			Status: herorunv1alpha1.HeroRunStatus{
				Phase: string(state.PhasePending),
			},
		}
		fakeClient := newTestFakeClientBuilder(scheme).WithObjects(hrWithoutUID).Build()
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:                 store,
			Harness:               harness,
			KubeClient:            fakeClient,
			WorkspaceStorageClass: "standard",
		})
		if _, err := orch.Reconcile(ctx, runRequest(orch, repo.Key(), runID)); err != nil {
			t.Fatalf("orch.Reconcile: %v", err)
		}

		gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Phase != state.PhaseFailed {
			t.Errorf("expected phase %s, got %s", state.PhaseFailed, gotRun.Phase)
		}
		if !strings.Contains(gotRun.Error, "HeroRun owner") {
			t.Errorf("expected run.Error containing 'HeroRun owner', got %q", gotRun.Error)
		}

		var claim corev1.PersistentVolumeClaim
		if err := fakeClient.Get(ctx, client.ObjectKey{Namespace: "ao-sandboxes", Name: name}, &claim); !k8serrors.IsNotFound(err) {
			t.Errorf("expected claim not found, got %v", err)
		}
	})
}

type scriptedStartSessionLLM struct {
	task        string
	afterResult func(toolMsg llm.Message)
}

func (s *scriptedStartSessionLLM) CreateChatCompletion(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	toolCallResp := func(id, callID, name string, args map[string]string) (*llm.ChatCompletionResponse, error) {
		raw, _ := json.Marshal(args)
		return &llm.ChatCompletionResponse{
			ID: id,
			Choices: []llm.Choice{{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{{
						ID:   callID,
						Type: "function",
						Function: llm.FunctionCall{
							Name:      name,
							Arguments: string(raw),
						},
					}},
				},
			}},
		}, nil
	}

	var startResult *llm.Message
	var sawWaitResult bool
	var isChild bool
	for i := range req.Messages {
		m := &req.Messages[i]
		switch m.Role {
		case "user":
			if m.Content != nil && *m.Content == s.task {
				isChild = true
			}
		case "tool":
			switch m.ToolCallID {
			case "call-start-sess-1":
				startResult = m
			case "call-wait-1":
				sawWaitResult = true
			}
		}
	}
	if isChild {
		return toolCallResp("child-sess-finish", "call-child-finish-1", "finish", map[string]string{"summary": "child finished"})
	}
	if sawWaitResult {
		return toolCallResp("start-sess-finish", "call-finish-1", "finish", map[string]string{"summary": "child started successfully"})
	}
	if startResult != nil {
		if s.afterResult != nil {
			s.afterResult(*startResult)
		}
		// Wait for the child session to run and stop before finishing, so
		// cascading cancellation does not cancel it before reconciliation.
		return toolCallResp("start-sess-wait", "call-wait-1", "wait", map[string]string{"timeout": "30s"})
	}
	return toolCallResp("start-sess-llm", "call-start-sess-1", "start_session", map[string]string{"task": s.task})
}

func TestOrchestrator_Working_StartSession_CreatesChildSession(t *testing.T) {
	ctx := t.Context()
	var toolResultContent string
	startLLM := &scriptedStartSessionLLM{
		task: "check disk usage and record findings",
		afterResult: func(toolMsg llm.Message) {
			if toolMsg.Content != nil {
				toolResultContent = *toolMsg.Content
			}
		},
	}

	orch, store, _, repo, run := setupSelectingRun(t, state.PhaseWorking, "base-sha", &mockProcessServer{}, startLLM)

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	if !strings.Contains(toolResultContent, "Started child session") {
		t.Errorf("toolResultContent = %q, want containing 'Started child session'", toolResultContent)
	}

	parts := strings.Fields(toolResultContent)
	if len(parts) < 4 {
		t.Fatalf("unexpected toolResultContent format: %q", toolResultContent)
	}
	childName := strings.TrimSuffix(parts[3], ".")

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	expectedParent := gotRun.SessionID
	if expectedParent == "" {
		t.Fatalf("expected non-empty SessionID on run")
	}

	childSess, err := orch.harness.LoadSession(ctx, testSessionStore(orch), childName)
	if err != nil {
		t.Fatalf("LoadSession(childName=%q): %v", childName, err)
	}
	if childSess.Metadata.Parent != expectedParent {
		t.Errorf("child Parent = %q, want %q", childSess.Metadata.Parent, expectedParent)
	}
	if len(childSess.Metadata.Ancestors) != 1 || childSess.Metadata.Ancestors[0] != expectedParent {
		t.Errorf("child Ancestors = %v, want [%s]", childSess.Metadata.Ancestors, expectedParent)
	}
	if childSess.Metadata.RunID != run.ID {
		t.Errorf("child RunID = %q, want %q", childSess.Metadata.RunID, run.ID)
	}
	if childSess.Metadata.Depth() != 1 {
		t.Errorf("child Depth = %d, want 1", childSess.Metadata.Depth())
	}

	events, err := store.ReadEvents(ctx, repo.Key(), run.ID)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	var foundChildEvent bool
	for _, e := range events {
		if e.Kind == "session_started" && strings.Contains(e.Message, childName) {
			foundChildEvent = true
			if m, ok := e.Data.(map[string]any); ok {
				if m["session_id"] != childName || m["parent"] != expectedParent {
					t.Errorf("unexpected event Data: %+v", m)
				}
			}
		}
	}
	if !foundChildEvent {
		t.Errorf("expected session_started event for child session %s", childName)
	}
}

func TestOrchestratorOptions_Validate_MaxSessionDepth(t *testing.T) {
	store := state.NewFileStore(t.TempDir())
	harness, _, _ := setupTestHarness(t, nil)
	scheme, _ := NewScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&herorunv1alpha1.HeroRun{}, &herorunv1alpha1.Session{}, &herorunv1alpha1.SessionMessage{}).Build()

	for _, d := range []int{-2, -1, 0} {
		opts := OrchestratorOptions{
			Store:             store,
			Harness:           harness,
			KubeClient:        fakeClient,
			MaxTurns:          30,
			MaxConcurrentRuns: 4,
			MaxSessionDepth:   d,
		}
		err := opts.Validate()
		if err == nil || !strings.Contains(err.Error(), "MaxSessionDepth must be at least 1") {
			t.Errorf("Validate with MaxSessionDepth=%d: got %v, want error containing 'MaxSessionDepth must be at least 1'", d, err)
		}
	}
}

type scriptedListSessionsLLM struct {
	afterResult func(toolMsg llm.Message)
	callCount   atomic.Int32
}

func (s *scriptedListSessionsLLM) CreateChatCompletion(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	n := s.callCount.Add(1)
	if n == 1 {
		return &llm.ChatCompletionResponse{
			ID: "list-sess-llm",
			Choices: []llm.Choice{{
				Message: llm.Message{
					Role: "assistant",
					ToolCalls: []llm.ToolCall{{
						ID:   "call-list-sess-1",
						Type: "function",
						Function: llm.FunctionCall{
							Name:      "list_sessions",
							Arguments: "{}",
						},
					}},
				},
			}},
		}, nil
	}
	for _, m := range req.Messages {
		if m.Role == "tool" && m.ToolCallID == "call-list-sess-1" {
			if s.afterResult != nil {
				s.afterResult(m)
			}
		}
	}
	finishArgs, _ := json.Marshal(map[string]string{"summary": "listing completed"})
	return &llm.ChatCompletionResponse{
		ID: "list-sess-finish",
		Choices: []llm.Choice{{
			Message: llm.Message{
				Role: "assistant",
				ToolCalls: []llm.ToolCall{{
					ID:   "call-finish-1",
					Type: "function",
					Function: llm.FunctionCall{
						Name:      "finish",
						Arguments: string(finishArgs),
					},
				}},
			},
		}},
	}, nil
}

func TestOrchestrator_Working_ListSessions_RootAndChildren(t *testing.T) {
	ctx := t.Context()
	var toolResultContent string
	listLLM := &scriptedListSessionsLLM{
		afterResult: func(toolMsg llm.Message) {
			if toolMsg.Content != nil {
				toolResultContent = *toolMsg.Content
			}
		},
	}

	orch, _, _, repo, run := setupSelectingRun(t, state.PhaseWorking, "base-sha", &mockProcessServer{}, listLLM)

	// Ensure run has a session ID
	run.SessionID = generateSessionID(run.ID)
	putTestRun(t, ctx, orch, run)

	// Pre-create a stopped child Session CRD
	stoppedChild := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "childstopped",
			Namespace: orch.namespace(),
			Labels:    map[string]string{LabelRun: run.ID},
		},
		Spec: herorunv1alpha1.SessionSpec{
			RunID:  run.ID,
			Parent: run.SessionID,
		},
	}
	if err := orch.kubeClient.Create(ctx, stoppedChild); err != nil {
		t.Fatalf("Create childstopped Session: %v", err)
	}
	stoppedChild.Status.Phase = herorunv1alpha1.SessionPhaseSucceeded
	if err := orch.kubeClient.Status().Update(ctx, stoppedChild); err != nil {
		t.Fatalf("Status().Update childstopped Session: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, runRequest(orch, repo.Key(), run.ID)); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	if toolResultContent == "" {
		t.Fatal("expected non-empty toolResultContent from list_sessions call")
	}

	var parsed []ao.SessionInfo
	if err := json.Unmarshal([]byte(toolResultContent), &parsed); err != nil {
		t.Fatalf("failed to unmarshal list_sessions content %q: %v", toolResultContent, err)
	}

	if len(parsed) != 2 {
		t.Fatalf("expected 2 sessions, got %d: %+v", len(parsed), parsed)
	}

	byName := make(map[string]ao.SessionInfo, len(parsed))
	for _, s := range parsed {
		byName[s.Name] = s
	}

	// 1. Root session (calling session) is running
	rootInfo, ok := byName[run.SessionID]
	if !ok || rootInfo.Parent != "" || rootInfo.State != "running" {
		t.Errorf("unexpected root session in list: %+v, want name=%q parent='' state='running'", rootInfo, run.SessionID)
	}

	// 2. Child stopped
	childInfo, ok := byName["childstopped"]
	if !ok || childInfo.Parent != run.SessionID || childInfo.State != "stopped" {
		t.Errorf("unexpected child in list: %+v, want name='childstopped' parent=%q state='stopped'", childInfo, run.SessionID)
	}
}

func TestOrchestrator_ListSessions_NoRun(t *testing.T) {
	ctx := t.Context()
	tool := ao.NewListSessionsTool(nil)
	_, err := tool.Run(ctx, nil)
	if err == nil || !strings.Contains(err.Error(), "session belongs to no run") {
		t.Errorf("expected 'session belongs to no run' error, got %v", err)
	}
}

func TestOrchestrator_PostMessage_Validation(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261123T120000Z-postval", "run-postval"
	store, sessionStore, repo, run, harness, _, fakeClient := setupWorkingTestRun(t, runID, sbName, nil, &llm.ElizaClient{})

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	// 1. Empty message text
	if err := orch.PostMessage(ctx, repo.Key(), runID, "   "); err == nil {
		t.Error("expected error for empty message text, got nil")
	}

	// 2. Non-existent run
	if err := orch.PostMessage(ctx, repo.Key(), "non-existent-run", "hello"); err == nil {
		t.Error("expected error for non-existent run, got nil")
	}

	// 3. Terminal run
	run.SetPhase(state.PhaseSucceeded, time.Now().UTC())
	var hr herorunv1alpha1.HeroRun
	if err := orch.kubeClient.Get(ctx, client.ObjectKey{Namespace: harness.Namespace(), Name: sandboxNameForRun(runID)}, &hr); err != nil {
		t.Fatalf("getting HeroRun: %v", err)
	}
	if err := orch.saveHeroRun(ctx, &hr, run); err != nil {
		t.Fatalf("saveHeroRun: %v", err)
	}
	if err := orch.PostMessage(ctx, repo.Key(), runID, "hello"); !errors.Is(err, ErrRunNotWorking) {
		t.Errorf("expected ErrRunNotWorking for terminal run, got %v", err)
	}
}

func TestOrchestrator_PostMessage_RunJSONDeletedSucceeds(t *testing.T) {
	ctx := t.Context()
	runID := "20261123T120000Z-postnojson"
	tempDir := t.TempDir()
	store := state.NewFileStore(tempDir)
	sessionStore := session.NewFileStore(tempDir)
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

	harness, _, _ := setupTestHarness(t, nil)
	scheme, err := NewScheme()
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}

	targetSessID := topLevelSessionID(runID, "")
	hr := &herorunv1alpha1.HeroRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sandboxNameForRun(runID),
			Namespace: harness.Namespace(),
			UID:       types.UID("uid-" + runID),
		},
		Spec: herorunv1alpha1.HeroRunSpec{
			RunID:      runID,
			Repository: repo.URL,
			Goal:       "optimize-latency",
			BaseCommit: "commit-base123",
		},
		Status: herorunv1alpha1.HeroRunStatus{
			Phase:     string(state.PhaseWorking),
			SessionID: targetSessID,
		},
	}
	sess := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetSessID,
			Namespace: harness.Namespace(),
			UID:       types.UID("uid-" + targetSessID),
		},
		Spec: herorunv1alpha1.SessionSpec{
			RunID: runID,
		},
		Status: herorunv1alpha1.SessionStatus{
			Phase: herorunv1alpha1.SessionPhaseRunning,
		},
	}

	fakeClient := newTestFakeClientBuilder(scheme).
		WithObjects(hr, sess).
		Build()

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	// Ensure run.json does NOT exist on disk
	runJSONPath := filepath.Join(tempDir, "repos", repo.Host, repo.Owner, repo.Name, "runs", runID, "run.json")
	if _, err := os.Stat(runJSONPath); !os.IsNotExist(err) {
		t.Fatalf("expected run.json to not exist on disk")
	}

	// PostMessage must succeed by reading HeroRun and Session CRDs rather than run.json
	customMsg := "Instruction delivered with deleted run.json"
	if err := orch.PostMessage(ctx, repo.Key(), runID, customMsg); err != nil {
		t.Fatalf("PostMessage failed with deleted run.json: %v", err)
	}

	var list herorunv1alpha1.SessionMessageList
	if err := fakeClient.List(ctx, &list,
		client.InNamespace(orch.namespace()),
		client.MatchingLabels{herorunv1alpha1.LabelSession: targetSessID},
	); err != nil {
		t.Fatalf("List SessionMessages: %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].Spec.Text != customMsg {
		t.Fatalf("expected 1 SessionMessage %q, got %+v", customMsg, list.Items)
	}
}

func TestOrchestrator_PostMessage_DeliveredOnNextTurn(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261123T120000Z-postnext", "run-postnext"
	store, sessionStore, repo, _, harness, _, fakeClient := setupWorkingTestRun(t, runID, sbName, nil, &llm.ElizaClient{})

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	req := runRequest(orch, repo.Key(), runID)
	// Pass 1 creates the root Session CRD.
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("orch.Reconcile: %v", err)
	}

	customMsg := "Please optimize the database queries."
	if err := orch.PostMessage(ctx, repo.Key(), runID, customMsg); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	if _, err := reconcileWorkingRun(ctx, orch, req); err != nil {
		t.Fatalf("reconcileWorkingRun: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) < 3 {
		t.Fatalf("expected at least 3 messages (system, user, assistant), got %d: %+v", len(msgs), msgs)
	}
	var foundCustomMsg bool
	for _, m := range msgs {
		if m.Role == "user" && m.Content != nil && *m.Content == customMsg {
			foundCustomMsg = true
			break
		}
	}
	if !foundCustomMsg {
		t.Errorf("expected user message %q in session messages: %+v", customMsg, msgs)
	}
}

func TestOrchestrator_PostMessage_DeliveredBeforeNextModelCall(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261123T120000Z-postcall", "run-postcall"

	var modelCalls []llm.ChatCompletionRequest
	var mu sync.Mutex

	toolStarted := make(chan struct{})
	toolRelease := make(chan struct{})

	// LLM mock that records incoming requests:
	// Call 1: returns tool call to "run_command"
	// Call 2: returns final assistant reply
	mockLLM := &mockMultiCallLLM{
		callFn: func(req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
			mu.Lock()
			callNum := len(modelCalls) + 1
			modelCalls = append(modelCalls, req)
			mu.Unlock()

			if callNum == 1 {
				return &llm.ChatCompletionResponse{
					Choices: []llm.Choice{
						{
							Message: llm.Message{
								Role: "assistant",
								ToolCalls: []llm.ToolCall{
									{
										ID:   "call-1",
										Type: "function",
										Function: llm.FunctionCall{
											Name:      "run_command",
											Arguments: `{"command":"uname -a"}`,
										},
									},
								},
							},
						},
					},
				}, nil
			}
			done := "done after user instruction"
			return &llm.ChatCompletionResponse{
				Choices: []llm.Choice{
					{
						Message: llm.Message{
							Role:    "assistant",
							Content: &done,
						},
						FinishReason: "stop",
					},
				},
			}, nil
		},
	}

	store, sessionStore, repo, _, harness, _, fakeClient := setupWorkingTestRun(t, runID, sbName, nil, mockLLM)

	// Override sandbox to block on tool call
	fakeSB := &blockingToolSandbox{
		started: toolStarted,
		release: toolRelease,
	}
	harness.SetSandboxOverride(fakeSB)

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	req := runRequest(orch, repo.Key(), runID)

	// Step 1: Create root Session CRD and start Session reconciliation.
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile pass 1: %v", err)
	}
	targetSessID := generateSessionID(runID)
	sessDone := make(chan error, 1)
	go func() {
		sessDone <- reconcileSessionUntilTerminal(ctx, orch, targetSessID)
	}()

	// Wait until sandbox tool executes and blocks
	select {
	case <-toolStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sandbox tool to start")
	}

	// Step 2: Post message while tool is executing
	interruptedMsg := "stop, do this instead"
	if err := orch.PostMessage(ctx, repo.Key(), runID, interruptedMsg); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	// Verify SessionMessage was created
	var msgList herorunv1alpha1.SessionMessageList
	if err := fakeClient.List(ctx, &msgList,
		client.InNamespace(orch.namespace()),
		client.MatchingLabels{herorunv1alpha1.LabelSession: targetSessID},
	); err != nil || len(msgList.Items) != 1 {
		t.Fatalf("expected 1 SessionMessage, got %v (err: %v)", msgList.Items, err)
	}

	// Step 3: Unblock sandbox tool and wait for session to complete
	close(toolRelease)
	select {
	case err := <-sessDone:
		if err != nil {
			t.Fatalf("reconcileSessionUntilTerminal: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session to complete")
	}

	// Step 4: Reconcile pass 2 consumes terminal Session outcome
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile pass 2: %v", err)
	}

	// Verify model call 2 received the posted message before running!
	mu.Lock()
	callsCount := len(modelCalls)
	var call2Msgs []llm.Message
	if callsCount >= 2 {
		call2Msgs = modelCalls[1].Messages
	}
	mu.Unlock()

	if callsCount != 2 {
		t.Fatalf("expected 2 model calls in this turn, got %d", callsCount)
	}

	// In Call 2: verify the posted user message was delivered before the model call
	var foundPostedMsg bool
	for _, m := range call2Msgs {
		if m.Role == "user" && m.Content != nil && *m.Content == interruptedMsg {
			foundPostedMsg = true
			break
		}
	}
	if !foundPostedMsg {
		t.Fatalf("model call 2 did not contain posted user message; messages = %+v", call2Msgs)
	}

	// Verify SessionMessage status is marked delivered
	if err := fakeClient.List(ctx, &msgList,
		client.InNamespace(orch.namespace()),
		client.MatchingLabels{herorunv1alpha1.LabelSession: targetSessID},
	); err != nil {
		t.Fatalf("List SessionMessages after delivery: %v", err)
	}
	if len(msgList.Items) != 1 || msgList.Items[0].Status.DeliveredAt == nil {
		t.Errorf("expected SessionMessage to be marked delivered, got %+v", msgList.Items)
	}
}

func TestOrchestrator_PostMessage_SurvivesRestart(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261123T120000Z-postres", "run-postres"
	store, sessionStore, repo, _, harness, _, fakeClient := setupWorkingTestRun(t, runID, sbName, nil, &llm.ElizaClient{})

	orch1 := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	req1 := runRequest(orch1, repo.Key(), runID)
	if _, err := orch1.Reconcile(ctx, req1); err != nil {
		t.Fatalf("orch1.Reconcile: %v", err)
	}

	targetSessID := generateSessionID(runID)
	surviveMsg := "Instruction that survives restart"
	if err := orch1.PostMessage(ctx, repo.Key(), runID, surviveMsg); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}

	// SessionMessage is persisted in kube-apiserver
	var msgList herorunv1alpha1.SessionMessageList
	if err := fakeClient.List(ctx, &msgList,
		client.InNamespace(orch1.namespace()),
		client.MatchingLabels{herorunv1alpha1.LabelSession: targetSessID},
	); err != nil || len(msgList.Items) != 1 || msgList.Items[0].Spec.Text != surviveMsg {
		t.Fatalf("expected 1 SessionMessage %q, got %v (err: %v)", surviveMsg, msgList.Items, err)
	}

	// Simulate control plane restart by creating a new Orchestrator instance
	orch2 := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   orch1.kubeClient,
	})

	if _, err := reconcileWorkingRun(ctx, orch2, runRequest(orch2, repo.Key(), runID)); err != nil {
		t.Fatalf("reconcileWorkingRun on orch2: %v", err)
	}

	gotRun, err := GetRun(ctx, orch2.kubeClient, orch2.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	var sawMsg bool
	for _, m := range msgs {
		if m.Role == "user" && m.Content != nil && *m.Content == surviveMsg {
			sawMsg = true
			break
		}
	}
	if !sawMsg {
		t.Fatalf("expected session transcript to contain survived message, got %+v", msgs)
	}

	// SessionMessage is marked delivered after restart
	if err := fakeClient.List(ctx, &msgList,
		client.InNamespace(orch2.namespace()),
		client.MatchingLabels{herorunv1alpha1.LabelSession: targetSessID},
	); err != nil {
		t.Fatalf("List SessionMessages after restart: %v", err)
	}
	if len(msgList.Items) != 1 || msgList.Items[0].Status.DeliveredAt == nil {
		t.Errorf("expected SessionMessage marked delivered after restart, got %+v", msgList.Items)
	}
}

type mockMultiCallLLM struct {
	callFn func(req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error)
}

func (m *mockMultiCallLLM) CreateChatCompletion(_ context.Context, req llm.ChatCompletionRequest) (*llm.ChatCompletionResponse, error) {
	return m.callFn(req)
}

type blockingToolSandbox struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingToolSandbox) ExecCommand(_ context.Context, _ tools.ExecCommandOptions) (*tools.ExecCommandResult, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return &tools.ExecCommandResult{
		Stdout:   "Linux mock-node\n",
		ExitCode: 0,
	}, nil
}

func TestOrchestrator_PostMessage_WhileTurnActive_DeliveredOnSubsequentTurn(t *testing.T) {
	ctx := t.Context()
	runID, sbName := "20261123T120000Z-postmid", "run-postmid"

	blockingLLM := newMidTurnBlockingLLM()
	store, sessionStore, repo, _, harness, _, fakeClient := setupWorkingTestRun(t, runID, sbName, nil, blockingLLM)

	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	req := runRequest(orch, repo.Key(), runID)

	// Step 1: Create root Session CRD and run turn 1 via session controller.
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile pass 1: %v", err)
	}
	sessName := generateSessionID(runID)
	sessDone := make(chan error, 1)
	go func() {
		sessDone <- reconcileSessionUntilTerminal(ctx, orch, sessName)
	}()

	// Wait until session controller starts and blocks in LLM.
	select {
	case <-blockingLLM.started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for turn 1 to start")
	}

	running := orch.RunningSessions(runID)
	if !running[sessName] {
		t.Fatalf("expected session %q to be running during turn 1, got %v", sessName, running)
	}

	// Step 2: Post message while turn 1 is active.
	userMsg := "Please check performance afterwards."
	if err := orch.PostMessage(ctx, repo.Key(), runID, userMsg); err != nil {
		t.Fatalf("PostMessage while turn active: %v", err)
	}

	// Step 3: Unblock turn 1; sessioncontroller continues for the pending SessionMessage and completes.
	close(blockingLLM.release)
	select {
	case err := <-sessDone:
		if err != nil {
			t.Fatalf("reconcileSessionUntilTerminal: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session to complete")
	}

	// Step 4: Reconcile HeroRun to consume terminal Session outcome.
	if _, err := orch.Reconcile(ctx, req); err != nil {
		t.Fatalf("Reconcile pass 2: %v", err)
	}

	gotRun, err := GetRun(ctx, orch.kubeClient, orch.namespace(), runID)
	if err != nil {
		t.Fatalf("GetRun after completion: %v", err)
	}
	if gotRun.Phase != state.PhaseNoImprovement {
		t.Errorf("expected phase %s after turn 2, got %s", state.PhaseNoImprovement, gotRun.Phase)
	}

	// Verify transcript has:
	// 0: system
	// 1: user ("Please work on the goal.")
	// 2: assistant ("done")
	// 3: user ("Please check performance afterwards.")
	// 4: assistant ("done")
	msgs, _, err := sessionStore.LoadSession(ctx, gotRun.SessionID)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages in transcript, got %d: %+v", len(msgs), msgs)
	}
	if msgs[1].Role != "user" || msgs[1].Content == nil || *msgs[1].Content != "Please work on the goal." {
		t.Errorf("msg[1] = %+v, want 'Please work on the goal.'", msgs[1])
	}
	if msgs[3].Role != "user" || msgs[3].Content == nil || *msgs[3].Content != userMsg {
		t.Errorf("msg[3] = %+v, want %q", msgs[3], userMsg)
	}
}

func TestOrchestrator_RunningSessions(t *testing.T) {
	ctx := t.Context()
	store, sessionStore, _, run, harness, _, fakeClient := setupWorkingTestRun(t, "run-live-status", "sb-live-status", nil, &llm.ElizaClient{})
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:        store,
		SessionStore: sessionStore,
		Harness:      harness,
		KubeClient:   fakeClient,
	})

	if got := orch.RunningSessions(run.ID); len(got) != 0 {
		t.Fatalf("RunningSessions before sessions exist = %v, want empty", got)
	}

	createSession := func(name, runID string, phase herorunv1alpha1.SessionPhase, finalizer bool) *herorunv1alpha1.Session {
		s := &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: orch.namespace(),
				Labels:    map[string]string{LabelRun: runID},
			},
			Spec: herorunv1alpha1.SessionSpec{
				RunID: runID,
			},
		}
		if finalizer {
			s.Finalizers = []string{"test.auto-optimizer.labs.gke.io/hold"}
		}
		if err := fakeClient.Create(ctx, s); err != nil {
			t.Fatalf("Create Session %s: %v", name, err)
		}
		if phase != "" {
			s.Status.Phase = phase
			if err := fakeClient.Status().Update(ctx, s); err != nil {
				t.Fatalf("Status().Update Session %s: %v", name, err)
			}
		}
		return s
	}

	createSession("root-running", run.ID, herorunv1alpha1.SessionPhaseRunning, false)
	createSession("child-pending", run.ID, herorunv1alpha1.SessionPhasePending, false)
	createSession("child-waiting", run.ID, herorunv1alpha1.SessionPhaseWaiting, false)
	createSession("child-new", run.ID, "", false)
	createSession("child-succeeded", run.ID, herorunv1alpha1.SessionPhaseSucceeded, false)
	createSession("child-failed", run.ID, herorunv1alpha1.SessionPhaseFailed, false)
	createSession("child-canceled", run.ID, herorunv1alpha1.SessionPhaseCanceled, false)
	deleting := createSession("child-deleting", run.ID, herorunv1alpha1.SessionPhaseRunning, true)
	if err := fakeClient.Delete(ctx, deleting); err != nil {
		t.Fatalf("Delete Session child-deleting: %v", err)
	}
	createSession("other-run-running", "other-run", herorunv1alpha1.SessionPhaseRunning, false)

	got := orch.RunningSessions(run.ID)
	want := map[string]bool{
		"root-running":  true,
		"child-pending": true,
		"child-waiting": true,
		"child-new":     true,
	}
	if len(got) != len(want) {
		t.Fatalf("RunningSessions = %v, want %v", got, want)
	}
	for name := range want {
		if !got[name] {
			t.Errorf("expected %q in RunningSessions, got %v", name, got)
		}
	}
}

func TestOrchestrator_SandboxOwnerForSession(t *testing.T) {
	ctx := t.Context()
	store := state.NewFileStore(t.TempDir())
	sessionStore := session.NewFileStore(t.TempDir())
	repo := &state.Repo{
		URL:  "https://github.com/gke-labs/test-repo",
		Host: "github.com", Owner: "gke-labs", Name: "test-repo",
	}
	if err := store.PutRepo(ctx, repo); err != nil {
		t.Fatalf("PutRepo: %v", err)
	}

	deadline := time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC)
	run := &state.Run{
		ID:                 "run-sb-hook",
		Repo:               repo.Key(),
		BaseCommit:         "abc1234",
		Phase:              state.PhaseWorking,
		SandboxName:        "ao-run-sb-hook",
		WorkspaceClaimName: "ao-run-sb-hook",
		Deadline:           &deadline,
	}

	procSrv := &mockProcessServer{exitCode: 0, stdout: "abc1234\n"}
	harness, agentsCS, coreCS := setupTestHarness(t, procSrv)

	// Mark newly created sandboxes ready and create their backing pod for portal binding.
	agentsCS.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		sb := action.(k8stesting.CreateAction).GetObject().(*sandboxv1beta1.Sandbox).DeepCopy()
		sb.Status.Conditions = []metav1.Condition{{
			Type:   string(sandboxv1beta1.SandboxConditionReady),
			Status: metav1.ConditionTrue,
		}}
		sb.Status.PodIPs = []string{"10.0.0.42"}
		sb.UID = types.UID("uid-" + sb.Name)
		_ = agentsCS.Tracker().Create(action.GetResource(), sb, action.GetNamespace())
		_, _ = coreCS.CoreV1().Pods(sb.Namespace).Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      sb.Name,
				Namespace: sb.Namespace,
				UID:       types.UID("pod-uid-" + sb.Name),
			},
		}, metav1.CreateOptions{})
		return true, sb, nil
	})

	binder := &fakePortalBinder{}
	orch := newTestOrchestrator(t, OrchestratorOptions{
		Store:         store,
		SessionStore:  sessionStore,
		Harness:       harness,
		PortalBinder:  binder,
		PolicyFactory: portal.HostListPolicyFactory([]string{"github.com"}),
	})
	putTestRun(t, ctx, orch, run)

	// Record a UID on the HeroRun so OwnerReferences can be verified.
	var hr herorunv1alpha1.HeroRun
	if err := orch.kubeClient.Get(ctx, client.ObjectKey{Namespace: orch.namespace(), Name: sandboxNameForRun(run.ID)}, &hr); err != nil {
		t.Fatalf("Get HeroRun: %v", err)
	}
	hr.UID = "hr-uid-hook"
	if err := orch.kubeClient.Update(ctx, &hr); err != nil {
		t.Fatalf("Update HeroRun UID: %v", err)
	}

	// Standalone sessions return (nil, nil).
	standaloneOwner, err := orch.SandboxOwnerForSession(ctx, &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone", Namespace: orch.namespace()},
	})
	if err != nil || standaloneOwner != nil {
		t.Fatalf("SandboxOwnerForSession(standalone) = (%v, %v), want (nil, nil)", standaloneOwner, err)
	}

	// Root and child sessions sharing RunID receive the exact same SandboxOwner,
	// even before Orchestrator.Reconcile has populated runStates.
	rootSess := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "rootsess", Namespace: orch.namespace()},
		Spec: herorunv1alpha1.SessionSpec{
			RunID:              run.ID,
			HeroRunName:        hr.Name,
			SandboxName:        run.SandboxName,
			WorkspaceClaimName: run.WorkspaceClaimName,
		},
	}
	childSess := &herorunv1alpha1.Session{
		ObjectMeta: metav1.ObjectMeta{Name: "childsess", Namespace: orch.namespace()},
		Spec: herorunv1alpha1.SessionSpec{
			RunID:              run.ID,
			HeroRunName:        hr.Name,
			Parent:             "rootsess",
			Ancestors:          []string{"rootsess"},
			SandboxName:        run.SandboxName,
			WorkspaceClaimName: run.WorkspaceClaimName,
		},
	}

	owner1, err := orch.SandboxOwnerForSession(ctx, rootSess)
	if err != nil {
		t.Fatalf("SandboxOwnerForSession(root): %v", err)
	}
	owner2, err := orch.SandboxOwnerForSession(ctx, childSess)
	if err != nil {
		t.Fatalf("SandboxOwnerForSession(child): %v", err)
	}
	if owner1 == nil || owner1 != owner2 {
		t.Fatalf("expected root and child to share the same SandboxOwner, got %p and %p", owner1, owner2)
	}
	if owner1.ExtendLifecycleEnabled() {
		t.Error("expected ExtendLifecycleEnabled=false for run SandboxOwner")
	}
	if !owner1.HasWorkspaceClaim() {
		t.Error("expected HasWorkspaceClaim=true for run SandboxOwner")
	}
	if active := owner1.ActiveSandbox(); active == nil || active.SandboxName() != run.SandboxName {
		t.Fatalf("ActiveSandbox = %v, want %q", active, run.SandboxName)
	}

	// Simulate the initial sandbox disappearing and verify on-demand replacement
	// preserves full run options and runs workspace + portal preparation.
	owner1.DropDeadSandbox(ctx, owner1.ActiveSandbox())
	if _, err := owner1.EnsureSandbox(ctx); err != nil {
		t.Fatalf("EnsureSandbox replacement: %v", err)
	}

	sb, err := agentsCS.AgentsV1beta1().Sandboxes(orch.namespace()).Get(ctx, run.SandboxName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get replacement Sandbox: %v", err)
	}
	if sb.Labels[LabelRun] != run.ID {
		t.Errorf("Sandbox label %s = %q, want %q", LabelRun, sb.Labels[LabelRun], run.ID)
	}
	if sb.Annotations[AnnotationRepo] != repo.Key().String() {
		t.Errorf("Sandbox annotation %s = %q, want %q", AnnotationRepo, sb.Annotations[AnnotationRepo], repo.Key().String())
	}
	if len(sb.OwnerReferences) != 1 || sb.OwnerReferences[0].Kind != "HeroRun" || sb.OwnerReferences[0].UID != hr.UID {
		t.Errorf("Sandbox OwnerReferences = %+v, want HeroRun UID %q", sb.OwnerReferences, hr.UID)
	}
	if sb.Spec.PodTemplate.ObjectMeta.Labels[session.PortalInterceptLabel] != "true" {
		t.Errorf("expected PortalIntercept label on replacement sandbox, got %v", sb.Spec.PodTemplate.ObjectMeta.Labels)
	}
	if wantShutdown := deadline.Add(SandboxShutdownGrace); sb.Spec.ShutdownTime == nil || !sb.Spec.ShutdownTime.Time.Equal(wantShutdown) {
		t.Errorf("Sandbox ShutdownTime = %v, want %v", sb.Spec.ShutdownTime, wantShutdown)
	}
	binder.mu.Lock()
	bindCalls := len(binder.calls)
	binder.mu.Unlock()
	if bindCalls != 1 {
		t.Errorf("portal bind calls = %d, want 1", bindCalls)
	}
}

func TestOrchestrator_PostMessage_CreatesSessionMessage(t *testing.T) {
	ctx := t.Context()
	fixedTime := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)

	newFixture := func(t *testing.T, runID string, sess *herorunv1alpha1.Session) (*Orchestrator, state.Store, state.RepoKey, client.Client) {
		t.Helper()
		store, sessionStore, repo, run, harness, _, fakeClient := setupWorkingTestRun(t, runID, "run-"+runID, nil, &llm.ElizaClient{})
		sess.Namespace = harness.Namespace()
		statusCopy := sess.Status
		if err := fakeClient.Create(ctx, sess); err != nil {
			t.Fatalf("creating Session: %v", err)
		}
		if statusCopy.Phase != "" {
			sess.Status = statusCopy
			if err := fakeClient.Status().Update(ctx, sess); err != nil {
				t.Fatalf("updating Session status: %v", err)
			}
		}
		run.SessionID = sess.Name
		putTestHeroRun(t, ctx, fakeClient, harness.Namespace(), repo.URL, run)
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			KubeClient:   fakeClient,
			Clock:        func() time.Time { return fixedTime },
		})
		return orch, store, repo.Key(), fakeClient
	}

	t.Run("active session creates owned labeled SessionMessages without file pending messages", func(t *testing.T) {
		const runID = "20261125T120000Z-smsgok"
		sessName := generateSessionID(runID)
		sess := &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{
				Name: sessName,
				UID:  types.UID("sess-uid-123"),
			},
			Spec: herorunv1alpha1.SessionSpec{
				RunID: runID,
			},
			Status: herorunv1alpha1.SessionStatus{
				Phase: herorunv1alpha1.SessionPhaseRunning,
			},
		}
		orch, store, repoKey, fakeClient := newFixture(t, runID, sess)

		if err := orch.PostMessage(ctx, repoKey, runID, "first guidance"); err != nil {
			t.Fatalf("PostMessage(first): %v", err)
		}
		if err := orch.PostMessage(ctx, repoKey, runID, "second guidance"); err != nil {
			t.Fatalf("PostMessage(second): %v", err)
		}

		// File-based pending messages must not be written when Session CRD exists.
		pending, err := store.ReadPendingMessages(ctx, repoKey, runID, sessName)
		if err != nil {
			t.Fatalf("ReadPendingMessages: %v", err)
		}
		if len(pending) != 0 {
			t.Fatalf("expected 0 file pending messages, got %+v", pending)
		}

		var list herorunv1alpha1.SessionMessageList
		if err := fakeClient.List(ctx, &list,
			client.InNamespace(orch.namespace()),
			client.MatchingLabels{herorunv1alpha1.LabelSession: sessName},
		); err != nil {
			t.Fatalf("List SessionMessages: %v", err)
		}
		if len(list.Items) != 2 {
			t.Fatalf("expected 2 SessionMessages, got %d", len(list.Items))
		}
		slices.SortFunc(list.Items, func(a, b herorunv1alpha1.SessionMessage) int {
			return strings.Compare(a.Name, b.Name)
		})

		wantTexts := []string{"first guidance", "second guidance"}
		for i, msg := range list.Items {
			if msg.Spec.SessionName != sessName {
				t.Errorf("msg[%d].Spec.SessionName = %q, want %q", i, msg.Spec.SessionName, sessName)
			}
			if msg.Spec.RunID != runID {
				t.Errorf("msg[%d].Spec.RunID = %q, want %q", i, msg.Spec.RunID, runID)
			}
			if msg.Spec.From != "" {
				t.Errorf("msg[%d].Spec.From = %q, want empty", i, msg.Spec.From)
			}
			if msg.Spec.Text != wantTexts[i] {
				t.Errorf("msg[%d].Spec.Text = %q, want %q", i, msg.Spec.Text, wantTexts[i])
			}
			if msg.Labels[herorunv1alpha1.LabelSession] != sessName {
				t.Errorf("msg[%d] label %s = %q, want %q", i, herorunv1alpha1.LabelSession, msg.Labels[herorunv1alpha1.LabelSession], sessName)
			}
			if msg.Labels[herorunv1alpha1.LabelRun] != runID {
				t.Errorf("msg[%d] label %s = %q, want %q", i, herorunv1alpha1.LabelRun, msg.Labels[herorunv1alpha1.LabelRun], runID)
			}
			if len(msg.OwnerReferences) != 1 {
				t.Fatalf("msg[%d] OwnerReferences = %+v, want 1", i, msg.OwnerReferences)
			}
			ref := msg.OwnerReferences[0]
			if ref.Kind != "Session" || ref.Name != sessName || ref.UID != sess.UID || ref.Controller == nil || !*ref.Controller {
				t.Errorf("msg[%d] unexpected OwnerReference: %+v", i, ref)
			}
		}
	})

	t.Run("terminal session returns ErrRunNotWorking", func(t *testing.T) {
		const runID = "20261125T120000Z-smsgterm"
		sessName := generateSessionID(runID)
		sess := &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{Name: sessName},
			Spec:       herorunv1alpha1.SessionSpec{RunID: runID},
			Status:     herorunv1alpha1.SessionStatus{Phase: herorunv1alpha1.SessionPhaseSucceeded},
		}
		orch, _, repoKey, fakeClient := newFixture(t, runID, sess)

		if err := orch.PostMessage(ctx, repoKey, runID, "too late"); !errors.Is(err, ErrRunNotWorking) {
			t.Fatalf("PostMessage on terminal session = %v, want ErrRunNotWorking", err)
		}
		var list herorunv1alpha1.SessionMessageList
		if err := fakeClient.List(ctx, &list); err != nil {
			t.Fatalf("List SessionMessages: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 SessionMessages, got %d", len(list.Items))
		}
	})

	t.Run("deleting session returns ErrRunNotWorking", func(t *testing.T) {
		const runID = "20261125T120000Z-smsgdel"
		sessName := generateSessionID(runID)
		sess := &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{
				Name:       sessName,
				Finalizers: []string{"test.auto-optimizer.labs.gke.io/hold"},
			},
			Spec:   herorunv1alpha1.SessionSpec{RunID: runID},
			Status: herorunv1alpha1.SessionStatus{Phase: herorunv1alpha1.SessionPhaseRunning},
		}
		orch, _, repoKey, fakeClient := newFixture(t, runID, sess)
		if err := fakeClient.Delete(ctx, sess); err != nil {
			t.Fatalf("Delete Session: %v", err)
		}

		if err := orch.PostMessage(ctx, repoKey, runID, "deleting"); !errors.Is(err, ErrRunNotWorking) {
			t.Fatalf("PostMessage on deleting session = %v, want ErrRunNotWorking", err)
		}
		var list herorunv1alpha1.SessionMessageList
		if err := fakeClient.List(ctx, &list); err != nil {
			t.Fatalf("List SessionMessages: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 SessionMessages, got %d", len(list.Items))
		}
	})

	t.Run("mismatched session RunID returns error", func(t *testing.T) {
		const runID = "20261125T120000Z-smsgmis"
		sessName := generateSessionID(runID)
		sess := &herorunv1alpha1.Session{
			ObjectMeta: metav1.ObjectMeta{Name: sessName},
			Spec:       herorunv1alpha1.SessionSpec{RunID: "different-run"},
			Status:     herorunv1alpha1.SessionStatus{Phase: herorunv1alpha1.SessionPhaseRunning},
		}
		orch, _, repoKey, fakeClient := newFixture(t, runID, sess)

		if err := orch.PostMessage(ctx, repoKey, runID, "wrong run"); err == nil {
			t.Fatal("expected error for mismatched session RunID, got nil")
		}
		var list herorunv1alpha1.SessionMessageList
		if err := fakeClient.List(ctx, &list); err != nil {
			t.Fatalf("List SessionMessages: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 SessionMessages, got %d", len(list.Items))
		}
	})

	t.Run("missing session returns ErrRunNotWorking without file fallback", func(t *testing.T) {
		const runID = "20261125T120000Z-smsgmissing"
		store, sessionStore, repo, _, harness, _, fakeClient := setupWorkingTestRun(t, runID, "run-"+runID, nil, &llm.ElizaClient{})
		orch := newTestOrchestrator(t, OrchestratorOptions{
			Store:        store,
			SessionStore: sessionStore,
			Harness:      harness,
			KubeClient:   fakeClient,
			Clock:        func() time.Time { return fixedTime },
		})

		if err := orch.PostMessage(ctx, repo.Key(), runID, "no session yet"); !errors.Is(err, ErrRunNotWorking) {
			t.Fatalf("PostMessage with missing session = %v, want ErrRunNotWorking", err)
		}
		pending, err := store.ReadPendingMessages(ctx, repo.Key(), runID, generateSessionID(runID))
		if err != nil {
			t.Fatalf("ReadPendingMessages: %v", err)
		}
		if len(pending) != 0 {
			t.Errorf("expected 0 file pending messages, got %+v", pending)
		}
	})
}
