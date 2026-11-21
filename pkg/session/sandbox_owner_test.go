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
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"crypto/ed25519"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakek8s "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	fakeagents "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"

	"github.com/gke-labs/auto-optimizer/pkg/llm"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
)

func setupTestOwnerHarness(t *testing.T, ns string) (*Harness, *fakeagents.Clientset, *int) {
	t.Helper()

	agentsCS := fakeagents.NewSimpleClientset()
	coreCS := fakek8s.NewSimpleClientset()
	client := NewSandboxClientWithClients(agentsCS, coreCS.CoreV1())

	var createdMu sync.Mutex
	createdCount := 0

	agentsCS.PrependReactor("create", "sandboxes", func(action k8stesting.Action) (bool, apiruntime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		sb := createAction.GetObject().(*sandboxv1beta1.Sandbox).DeepCopy()
		sb.Status.Conditions = []metav1.Condition{
			{
				Type:   string(sandboxv1beta1.SandboxConditionReady),
				Status: metav1.ConditionTrue,
			},
		}
		createdMu.Lock()
		createdCount++
		c := createdCount
		createdMu.Unlock()
		sb.UID = types.UID(fmt.Sprintf("uid-sb-%d", c))
		_ = agentsCS.Tracker().Create(action.GetResource(), sb, action.GetNamespace())
		return true, sb, nil
	})

	_, privKey, err := ed25519.GenerateKey(cryptorand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	harness := NewHarness(nil, client, nil, RunOptions{
		Namespace:             ns,
		ControlPlaneNamespace: "ao-system",
		SigningKey:            privKey,
	})

	return harness, agentsCS, &createdCount
}

func TestSandboxOwner_ConcurrentToolCalls_OneReplacement(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"
	sandboxName := "shared-run-sb"

	harness, agentsCS, createdCount := setupTestOwnerHarness(t, ns)
	sessionStore := NewFileStore(t.TempDir())

	sess1, err := harness.NewSession(ctx, sessionStore, "sesschild1", Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession 1: %v", err)
	}
	sess2, err := harness.NewSession(ctx, sessionStore, "sesschild2", Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession 2: %v", err)
	}

	owner := NewSandboxOwner(harness)
	owner.SetCreateSandboxOptions(CreateSandboxOptions{
		Name:            sandboxName,
		ExtendLifecycle: false,
	})

	var prepareMu sync.Mutex
	prepareCount := 0
	owner.SetSandboxPreparer(func(ctx context.Context, sb *Sandbox) error {
		prepareMu.Lock()
		prepareCount++
		current := prepareCount
		prepareMu.Unlock()
		if current == 2 {
			// Small delay on replacement preparation so the second goroutine
			// arrives while the replacement is being prepared under lock.
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	})

	sess1.SetSandboxOwner(owner)
	sess2.SetSandboxOwner(owner)

	// Obtain initial sandbox
	sb1, err := harness.ensureSandbox(ctx, sess1)
	if err != nil {
		t.Fatalf("ensureSandbox initial: %v", err)
	}
	if sb1 == nil {
		t.Fatal("expected sb1 != nil")
	}

	if *createdCount != 1 {
		t.Fatalf("expected 1 initial sandbox created, got %d", *createdCount)
	}
	if prepareCount != 1 {
		t.Fatalf("expected 1 initial prepare, got %d", prepareCount)
	}

	// Delete sandbox externally in Kubernetes so it is detected as dead
	if err := agentsCS.AgentsV1beta1().Sandboxes(ns).Delete(ctx, sandboxName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting sandbox: %v", err)
	}

	var (
		attemptsMu sync.Mutex
		call1Count int
		call2Count int
		tool1RanIn tools.Sandbox
		tool2RanIn tools.Sandbox
		err1       error
		err2       error
	)

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-barrier
		_, err1 = harness.callWithSandboxRetry(ctx, sess1, "toolA", sb1, func(s tools.Sandbox) (llm.Message, error) {
			attemptsMu.Lock()
			call1Count++
			attempt := call1Count
			attemptsMu.Unlock()

			if attempt == 1 {
				// Tool call fails because sandbox died
				return llm.Message{}, errors.New("connection failed: sandbox is dead")
			}
			tool1RanIn = s
			msg := "success A"
			return llm.Message{Role: "tool", Content: &msg}, nil
		})
	}()

	go func() {
		defer wg.Done()
		<-barrier
		_, err2 = harness.callWithSandboxRetry(ctx, sess2, "toolB", sb1, func(s tools.Sandbox) (llm.Message, error) {
			attemptsMu.Lock()
			call2Count++
			attempt := call2Count
			attemptsMu.Unlock()

			if attempt == 1 {
				// Tool call fails because sandbox died
				return llm.Message{}, errors.New("connection failed: sandbox is dead")
			}
			tool2RanIn = s
			msg := "success B"
			return llm.Message{Role: "tool", Content: &msg}, nil
		})
	}()

	close(barrier)
	wg.Wait()

	if err1 != nil {
		t.Fatalf("tool call 1 unexpected error: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("tool call 2 unexpected error: %v", err2)
	}

	// Exactly one replacement is created and prepared!
	if *createdCount != 2 {
		t.Errorf("expected exactly 2 sandboxes created (1 initial + 1 replacement), got %d", *createdCount)
	}
	if prepareCount != 2 {
		t.Errorf("expected exactly 2 prepare calls (1 initial + 1 replacement), got %d", prepareCount)
	}

	// Both tool calls ran again in the replacement sandbox
	if tool1RanIn == nil {
		t.Fatal("expected tool 1 to run in replacement sandbox")
	}
	if tool2RanIn == nil {
		t.Fatal("expected tool 2 to run in replacement sandbox")
	}
	if tool1RanIn == sb1 {
		t.Fatal("expected tool 1 to run in replacement sandbox, not dead sandbox")
	}
	if tool2RanIn == sb1 {
		t.Fatal("expected tool 2 to run in replacement sandbox, not dead sandbox")
	}
	if tool1RanIn != tool2RanIn {
		t.Errorf("expected both tool calls to run in the exact same replacement sandbox, got %v and %v", tool1RanIn, tool2RanIn)
	}
}

func TestSandboxOwner_AloneSession_Defaults(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"

	harness, _, _ := setupTestOwnerHarness(t, ns)
	sessionStore := NewFileStore(t.TempDir())

	sess, err := harness.NewSession(ctx, sessionStore, "alonesession", Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	owner := sess.SandboxOwner()
	if owner == nil {
		t.Fatal("expected non-nil SandboxOwner for alone session")
	}
	if !owner.ExtendLifecycleEnabled() {
		t.Error("expected ExtendLifecycleEnabled to be true by default for alone session")
	}
	if sess.ActiveSandbox() != nil {
		t.Errorf("expected ActiveSandbox to be nil initially, got %v", sess.ActiveSandbox())
	}
}

func TestSandboxOwner_ConcurrentEnsure_CreatesOnlyOne(t *testing.T) {
	ctx := t.Context()
	ns := "test-sandboxes"

	harness, _, createdCount := setupTestOwnerHarness(t, ns)
	sessionStore := NewFileStore(t.TempDir())

	sess1, err := harness.NewSession(ctx, sessionStore, "ensureone", Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession 1: %v", err)
	}
	sess2, err := harness.NewSession(ctx, sessionStore, "ensuretwo", Metadata{CreatedAt: time.Now()})
	if err != nil {
		t.Fatalf("NewSession 2: %v", err)
	}

	owner := NewSandboxOwner(harness)
	owner.SetCreateSandboxOptions(CreateSandboxOptions{
		Name:            "concurrent-ensure-sb",
		ExtendLifecycle: false,
	})
	sess1.SetSandboxOwner(owner)
	sess2.SetSandboxOwner(owner)

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	var sb1, sb2 tools.Sandbox
	var err1, err2 error

	go func() {
		defer wg.Done()
		<-barrier
		sb1, err1 = harness.ensureSandbox(ctx, sess1)
	}()

	go func() {
		defer wg.Done()
		<-barrier
		sb2, err2 = harness.ensureSandbox(ctx, sess2)
	}()

	close(barrier)
	wg.Wait()

	if err1 != nil {
		t.Fatalf("ensure 1: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("ensure 2: %v", err2)
	}

	if *createdCount != 1 {
		t.Errorf("expected exactly 1 sandbox created concurrently, got %d", *createdCount)
	}
	if sb1 == nil || sb2 == nil {
		t.Fatal("expected non-nil sandboxes")
	}
	if sb1 != sb2 {
		t.Errorf("expected both sessions to receive identical sandbox, got %v and %v", sb1, sb2)
	}
}
