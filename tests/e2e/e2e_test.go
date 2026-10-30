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

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gke-labs/auto-optimizer/pkg/session"
)

func getSandboxdImage(t *testing.T, subH *Harness) string {
	t.Helper()
	if img := os.Getenv("SANDBOXD_IMAGE"); img != "" {
		return img
	}
	out, err := subH.Kubectl("get", "configmap", "-n", "ao-system", "ao-config", "-o", "jsonpath={.data.SANDBOXD_IMAGE}")
	if err == nil && strings.TrimSpace(out) != "" {
		return strings.TrimSpace(out)
	}
	return session.DefaultSandboxdImage
}

func TestSmoke(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e smoke tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	t.Run("SandboxHealth", func(t *testing.T) {
		subH := h.WithT(t)
		sandboxName := "smoke-test-sandbox"
		subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sandboxName)

		sandboxdImage := getSandboxdImage(t, subH)
		shutdownTime := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
		sandboxManifest := fmt.Sprintf(`
apiVersion: agents.x-k8s.io/v1beta1
kind: Sandbox
metadata:
  name: %s
  namespace: ao-sandboxes
spec:
  shutdownPolicy: Delete
  shutdownTime: "%s"
  podTemplate:
    spec:
      containers:
        - name: sandboxd
          image: %s
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /v1/health
              port: 8080
`, sandboxName, shutdownTime, sandboxdImage)

		subH.ApplyManifest(sandboxManifest)

		// Wait for sandbox Ready condition
		subH.WaitForCondition("ao-sandboxes", "sandbox/"+sandboxName, "condition=Ready", 120*time.Second)

		// Port-forward to sandbox pod
		localPort, stop := subH.PortForward("ao-sandboxes", "pod/"+sandboxName, 8080)
		defer stop()

		// Query health endpoint
		client := &http.Client{Timeout: 2 * time.Second}
		healthURL := fmt.Sprintf("http://127.0.0.1:%d/v1/health", localPort)
		healthOK := false
		var lastBody string

		start := time.Now()
		for time.Since(start) < 30*time.Second {
			resp, err := client.Get(healthURL)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			body, readErr := io.ReadAll(resp.Body)
			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Logf("warning: failed to close response body: %v", closeErr)
			}
			if readErr != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			lastBody = string(body)
			if strings.Contains(lastBody, `"status":"ok"`) {
				healthOK = true
				break
			}
			time.Sleep(1 * time.Second)
		}

		if !healthOK {
			t.Fatalf("Sandbox health check failed on port %d. Expected {\"status\":\"ok\"}, got: %s", localPort, lastBody)
		}
		t.Logf("PASS: Sandbox %s reported healthy status.", sandboxName)
	})

	t.Run("ObjectFSPVCWrite", func(t *testing.T) {
		subH := h.WithT(t)
		pvcName := "smoke-test-objectfs-pvc"
		podName := "smoke-test-objectfs-pod"

		t.Cleanup(func() {
			subH.Delete("default", "pod", podName)
			subH.Delete("default", "pvc", pvcName)
		})

		storageManifest := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: default
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
spec:
  restartPolicy: Never
  volumes:
    - name: objectfs-vol
      persistentVolumeClaim:
        claimName: %s
  containers:
    - name: writer
      image: busybox:latest
      command: ["sh", "-c", "echo 'smoke test data' > /workspace/test.txt && cat /workspace/test.txt && exit 0"]
      volumeMounts:
        - name: objectfs-vol
          mountPath: /workspace
`, pvcName, podName, pvcName)

		subH.ApplyManifest(storageManifest)

		// Wait for pod to complete
		subH.WaitForPodSuccess("default", podName, 120*time.Second)

		// Verify pod logs
		logs := subH.GetPodLogs("default", podName)
		if !strings.Contains(logs, "smoke test data") {
			t.Fatalf("Unexpected pod logs from %s: %s", podName, logs)
		}
		t.Logf("PASS: ObjectFS PVC read/write verified successfully.")
	})

	t.Run("ServicePortalsImages", func(t *testing.T) {
		subH := h.WithT(t)
		podName := "smoke-test-service-portals"
		subH.DeleteWithCleanup("default", "pod", podName)

		imagePrefix := os.Getenv("IMAGE_PREFIX")
		if imagePrefix == "" {
			imagePrefix = "auto-optimizer.local"
		}
		imageTag := os.Getenv("IMAGE_TAG")
		if imageTag == "" {
			imageTag = "dev"
		}
		image := fmt.Sprintf("%s/init-service-portals:%s", imagePrefix, imageTag)

		manifest := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
spec:
  restartPolicy: Never
  containers:
    - name: init-service-portals
      image: %s
      command: ["init-service-portals", "--help"]
`, podName, image)

		subH.ApplyManifest(manifest)

		// Wait for pod to complete
		subH.WaitForPodSuccess("default", podName, 60*time.Second)

		// Verify pod logs
		logs := subH.GetPodLogs("default", podName)
		if !strings.Contains(logs, "Usage of") && !strings.Contains(logs, "-cert-dir") {
			t.Fatalf("Unexpected pod logs from %s: %s", podName, logs)
		}
		t.Logf("PASS: service-portals init-service-portals ran --help and exited 0.")
	})
}

func TestSession(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e session tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	sessionName := "e2esession"
	stateDir := t.TempDir()

	subH := h.WithT(t)
	subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sessionName)

	runCmd := exec.Command(aoBin, "session", "run",
		"--session", sessionName,
		"--prompt", "run: uname -a",
		"--yolo",
		"--state-dir", stateDir,
	)
	runCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var stdout, stderr bytes.Buffer
	runCmd.Stdout = &stdout
	runCmd.Stderr = &stderr

	t.Logf("Running %s session run...", aoBin)
	if err := runCmd.Run(); err != nil {
		t.Fatalf("ao session run failed: %v\nStdout: %s\nStderr: %s", err, stdout.String(), stderr.String())
	}

	output := stdout.String()
	t.Logf("ao session run output:\n%s", output)

	if !strings.Contains(output, "Linux") {
		t.Fatalf("expected output to contain 'Linux', got:\n%s", output)
	}

	// Verify sandbox was created in ao-sandboxes and expires (has shutdownPolicy: Delete and shutdownTime)
	sbJSON, err := subH.Kubectl("get", "sandbox", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.shutdownPolicy} {.spec.shutdownTime}")
	if err != nil {
		t.Fatalf("failed to get sandbox %s in ao-sandboxes: %v", sessionName, err)
	}

	parts := strings.Fields(sbJSON)
	if len(parts) < 2 {
		t.Fatalf("expected shutdownPolicy and shutdownTime, got: %q", sbJSON)
	}
	shutdownPolicy := parts[0]
	shutdownTime := parts[1]

	if shutdownPolicy != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", shutdownPolicy)
	}
	if shutdownTime == "" {
		t.Errorf("shutdownTime is empty")
	}

	// Verify the sandbox pod ran sandboxd via topology B (install-sandboxd init container)
	initContainerName, err := subH.Kubectl("get", "pod", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.initContainers[0].name}")
	if err != nil {
		t.Fatalf("failed to get pod initContainers: %v", err)
	}
	if initContainerName != "install-sandboxd" {
		t.Errorf("expected initContainer 'install-sandboxd', got %q", initContainerName)
	}
	initContainerImage, err := subH.Kubectl("get", "pod", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.initContainers[0].image}")
	if err != nil {
		t.Fatalf("failed to get pod initContainer image: %v", err)
	}
	expectedSandboxdImage := getSandboxdImage(t, subH)
	if initContainerImage != expectedSandboxdImage {
		t.Errorf("expected initContainer image %q, got %q", expectedSandboxdImage, initContainerImage)
	}

	// Verify pod logs show sandboxd running
	podLogs, err := subH.Kubectl("logs", "-n", "ao-sandboxes", sessionName, "-c", "sandbox")
	if err != nil {
		t.Fatalf("failed to get sandbox pod logs: %v", err)
	}
	t.Logf("sandbox pod logs:\n%s", podLogs)
	if !strings.Contains(podLogs, "sandboxd") {
		t.Errorf("expected sandbox pod logs to show sandboxd, got:\n%s", podLogs)
	}

	// Verify control plane RBAC does NOT include pods/exec or pods/portforward
	canExec, err := subH.Kubectl("auth", "can-i", "create", "pods", "--subresource=exec", "--as=system:serviceaccount:ao-system:ao", "-n", "ao-sandboxes")
	if err != nil && !strings.Contains(canExec, "no") {
		t.Fatalf("kubectl auth can-i failed: %v (output: %s)", err, canExec)
	}
	if strings.TrimSpace(canExec) != "no" {
		t.Errorf("expected control plane RBAC to not include pods/exec, got auth can-i result: %q", canExec)
	}

	canPortForward, err := subH.Kubectl("auth", "can-i", "create", "pods", "--subresource=portforward", "--as=system:serviceaccount:ao-system:ao", "-n", "ao-sandboxes")
	if err != nil && !strings.Contains(canPortForward, "no") {
		t.Fatalf("kubectl auth can-i failed: %v (output: %s)", err, canPortForward)
	}
	if strings.TrimSpace(canPortForward) != "no" {
		t.Errorf("expected control plane RBAC to not include pods/portforward, got auth can-i result: %q", canPortForward)
	}

	t.Logf("PASS: ao session run successfully executed in sandbox over sandboxd, verified output contains Linux, shutdownPolicy=%s, shutdownTime=%s, no pods/exec or pods/portforward RBAC", shutdownPolicy, shutdownTime)
}

func TestSessionUnpullableImageFailsFast(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e session tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	sessionName := "e2eunpullable"
	stateDir := t.TempDir()

	subH := h.WithT(t)
	subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sessionName)

	runCmd := exec.Command(aoBin, "session", "run",
		"--session", sessionName,
		"--prompt", "run: uname -a",
		"--yolo",
		"--state-dir", stateDir,
	)
	runCmd.Env = append(os.Environ(),
		"OPENAI_MODEL=fake-eliza",
		"SANDBOX_IMAGE=nonexistent.example.com/doesnotexist/unpullable:latest",
	)

	var stdout, stderr bytes.Buffer
	runCmd.Stdout = &stdout
	runCmd.Stderr = &stderr

	t.Logf("Running %s session run with unpullable image...", aoBin)
	start := time.Now()
	err = runCmd.Run()
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("expected ao session run to fail with unpullable image, but it succeeded\nStdout: %s\nStderr: %s", stdout.String(), stderr.String())
	}

	combinedOutput := stdout.String() + "\n" + stderr.String()
	t.Logf("ao session run finished in %v with output:\n%s", duration, combinedOutput)

	if duration > 30*time.Second {
		t.Errorf("expected session run with unpullable image to fail within 30s, took %v", duration)
	}

	if !strings.Contains(combinedOutput, "failed to pull image") && !strings.Contains(combinedOutput, "ErrImagePull") && !strings.Contains(combinedOutput, "ImagePullBackOff") {
		t.Errorf("expected error output to contain image pull error details, got:\n%s", combinedOutput)
	}

	if !t.Failed() {
		t.Logf("PASS: ao session run with unpullable image failed fast in %v with pull error.", duration)
	}
}

func TestSessionAPI(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e session API tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	// Allocate a free port for ao serve.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	serverAddr := listener.Addr().String()
	_ = listener.Close()

	sessionName := "e2eapi"
	stateDir := t.TempDir()

	subH := h.WithT(t)
	subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sessionName)

	serveCmd := exec.Command(aoBin, "serve",
		"--listen", serverAddr,
		"--state-dir", stateDir,
	)
	serveCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var serveStdout, serveStderr bytes.Buffer
	serveCmd.Stdout = &serveStdout
	serveCmd.Stderr = &serveStderr

	t.Logf("Starting ao serve on %s...", serverAddr)
	if err := serveCmd.Start(); err != nil {
		t.Fatalf("failed to start ao serve: %v", err)
	}
	t.Cleanup(func() {
		if serveCmd.Process != nil {
			_ = serveCmd.Process.Signal(syscall.SIGTERM)
			_ = serveCmd.Wait()
		}
	})

	// Wait for server to accept connections.
	serverReady := false
	for range 50 {
		conn, err := net.DialTimeout("tcp", serverAddr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			serverReady = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !serverReady {
		t.Fatalf("timed out waiting for ao serve to become ready on %s\nStderr: %s", serverAddr, serveStderr.String())
	}

	// 1. ao session send e2eapi "run: uname -a"
	sendCmd := exec.Command(aoBin, "session", "send", sessionName, "run: uname -a",
		"--server", serverAddr,
	)
	sendCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var sendStdout, sendStderr bytes.Buffer
	sendCmd.Stdout = &sendStdout
	sendCmd.Stderr = &sendStderr

	t.Logf("Running ao session send %s...", sessionName)
	if err := sendCmd.Run(); err != nil {
		t.Fatalf("ao session send failed: %v\nStdout: %s\nStderr: %s\nServer Stderr: %s",
			err, sendStdout.String(), sendStderr.String(), serveStderr.String())
	}

	sendOutput := sendStdout.String()
	t.Logf("ao session send output:\n%s", sendOutput)

	if !strings.Contains(sendOutput, "Linux") {
		t.Fatalf("expected output to contain 'Linux', got:\n%s", sendOutput)
	}

	initContainerImage, err := subH.Kubectl("get", "pod", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.initContainers[0].image}")
	if err != nil {
		t.Fatalf("failed to get pod initContainer image: %v", err)
	}
	expectedSandboxdImage := getSandboxdImage(t, subH)
	if initContainerImage != expectedSandboxdImage {
		t.Errorf("expected initContainer image %q, got %q", expectedSandboxdImage, initContainerImage)
	}

	// 2. ao session list --server <serverAddr>
	listCmd := exec.Command(aoBin, "session", "list",
		"--server", serverAddr,
	)
	var listStdout, listStderr bytes.Buffer
	listCmd.Stdout = &listStdout
	listCmd.Stderr = &listStderr

	t.Logf("Running ao session list...")
	if err := listCmd.Run(); err != nil {
		t.Fatalf("ao session list failed: %v\nStdout: %s\nStderr: %s", err, listStdout.String(), listStderr.String())
	}

	listOutput := listStdout.String()
	t.Logf("ao session list output:\n%s", listOutput)
	if !strings.Contains(listOutput, sessionName) {
		t.Fatalf("expected ao session list to contain %q, got:\n%s", sessionName, listOutput)
	}

	// 3. ao session get e2eapi --server <serverAddr>
	getCmd := exec.Command(aoBin, "session", "get", sessionName,
		"--server", serverAddr,
	)
	var getStdout, getStderr bytes.Buffer
	getCmd.Stdout = &getStdout
	getCmd.Stderr = &getStderr

	t.Logf("Running ao session get %s...", sessionName)
	if err := getCmd.Run(); err != nil {
		t.Fatalf("ao session get failed: %v\nStdout: %s\nStderr: %s", err, getStdout.String(), getStderr.String())
	}

	getOutput := getStdout.String()
	t.Logf("ao session get output:\n%s", getOutput)
	if !strings.Contains(getOutput, "run: uname -a") || !strings.Contains(getOutput, "Linux") {
		t.Fatalf("expected ao session get output to contain messages with 'run: uname -a' and 'Linux', got:\n%s", getOutput)
	}

	// 3b. Verify ao session get --json produces valid JSON
	getJSONCmd := exec.Command(aoBin,
		"session", "get", sessionName,
		"--server", serverAddr,
		"--json",
	)
	var getJSONStdout, getJSONStderr bytes.Buffer
	getJSONCmd.Stdout = &getJSONStdout
	getJSONCmd.Stderr = &getJSONStderr

	t.Logf("Running ao session get %s --json...", sessionName)
	if err := getJSONCmd.Run(); err != nil {
		t.Fatalf("ao session get --json failed: %v\nStdout: %s\nStderr: %s", err, getJSONStdout.String(), getJSONStderr.String())
	}
	getJSONOutput := getJSONStdout.String()
	if !strings.Contains(getJSONOutput, `"messages"`) || !strings.Contains(getJSONOutput, `"role"`) {
		t.Fatalf("expected JSON output from ao session get --json, got:\n%s", getJSONOutput)
	}

	// 4. Verify sandbox was created in ao-sandboxes with shutdownPolicy: Delete and shutdownTime
	sbJSON, err := subH.Kubectl("get", "sandbox", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.shutdownPolicy} {.spec.shutdownTime}")
	if err != nil {
		t.Fatalf("failed to get sandbox %s in ao-sandboxes: %v", sessionName, err)
	}

	parts := strings.Fields(sbJSON)
	if len(parts) < 2 {
		t.Fatalf("expected shutdownPolicy and shutdownTime, got: %q", sbJSON)
	}
	shutdownPolicy := parts[0]
	shutdownTime := parts[1]

	if shutdownPolicy != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", shutdownPolicy)
	}
	if shutdownTime == "" {
		t.Errorf("shutdownTime is empty")
	}

	t.Logf("PASS: ao serve + session send/list/get successfully verified in kind, shutdownPolicy=%s, shutdownTime=%s", shutdownPolicy, shutdownTime)
}

func TestSessionAPIInCluster(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e in-cluster session API tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	sessionName := "e2eincluster"
	subH := h.WithT(t)
	subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sessionName)

	testStart := time.Now().UTC()

	serverAddr, stop := subH.PortForwardService("ao-system", "svc/ao", 8081)
	defer stop()

	// 1. ao session send e2eincluster "run: uname -a" --server <addr>
	sendCmd := exec.Command(aoBin, "session", "send", sessionName, "run: uname -a",
		"--server", serverAddr,
	)
	var sendStdout, sendStderr bytes.Buffer
	sendCmd.Stdout = &sendStdout
	sendCmd.Stderr = &sendStderr

	t.Logf("Running ao session send %s against in-cluster server %s...", sessionName, serverAddr)
	if err := sendCmd.Run(); err != nil {
		t.Fatalf("ao session send failed: %v\nStdout: %s\nStderr: %s",
			err, sendStdout.String(), sendStderr.String())
	}

	sendOutput := sendStdout.String()
	t.Logf("ao session send output:\n%s", sendOutput)

	if !strings.Contains(sendOutput, "Linux") {
		t.Fatalf("expected output to contain 'Linux', got:\n%s", sendOutput)
	}

	// 2. ao session list --server <serverAddr>
	listCmd := exec.Command(aoBin, "session", "list",
		"--server", serverAddr,
	)
	var listStdout, listStderr bytes.Buffer
	listCmd.Stdout = &listStdout
	listCmd.Stderr = &listStderr

	t.Logf("Running ao session list...")
	if err := listCmd.Run(); err != nil {
		t.Fatalf("ao session list failed: %v\nStdout: %s\nStderr: %s", err, listStdout.String(), listStderr.String())
	}

	listOutput := listStdout.String()
	t.Logf("ao session list output:\n%s", listOutput)
	if !strings.Contains(listOutput, sessionName) {
		t.Fatalf("expected ao session list to contain %q, got:\n%s", sessionName, listOutput)
	}

	// 3. ao session get e2eincluster --server <serverAddr>
	getCmd := exec.Command(aoBin, "session", "get", sessionName,
		"--server", serverAddr,
	)
	var getStdout, getStderr bytes.Buffer
	getCmd.Stdout = &getStdout
	getCmd.Stderr = &getStderr

	t.Logf("Running ao session get %s...", sessionName)
	if err := getCmd.Run(); err != nil {
		t.Fatalf("ao session get failed: %v\nStdout: %s\nStderr: %s", err, getStdout.String(), getStderr.String())
	}

	getOutput := getStdout.String()
	t.Logf("ao session get output:\n%s", getOutput)
	if !strings.Contains(getOutput, "run: uname -a") || !strings.Contains(getOutput, "Linux") {
		t.Fatalf("expected ao session get output to contain messages with 'run: uname -a' and 'Linux', got:\n%s", getOutput)
	}

	// 4. Verify web UI serves session list containing sessionName via HTTP port 8080
	httpAddr, stopHTTP := subH.PortForwardService("ao-system", "svc/ao", 8080)
	defer stopHTTP()

	httpClient := &http.Client{Timeout: 5 * time.Second}
	uiURL := fmt.Sprintf("http://%s/", httpAddr)
	var uiBody string
	var lastStatus int
	uiOK := false

	start := time.Now()
	for time.Since(start) < 30*time.Second {
		resp, err := httpClient.Get(uiURL)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastStatus = resp.StatusCode
		bodyBytes, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		uiBody = string(bodyBytes)
		if lastStatus == http.StatusOK && strings.Contains(uiBody, sessionName) {
			uiOK = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !uiOK {
		t.Fatalf("web UI verification failed: status=%d, expected 200 OK and body to contain session name %q; got body:\n%s",
			lastStatus, sessionName, uiBody)
	}
	t.Logf("PASS: web UI returned 200 OK and contained session name %q", sessionName)

	// 5. Verify sandbox exists in ao-sandboxes with shutdownPolicy: Delete and shutdownTime
	sbJSON, err := subH.Kubectl("get", "sandbox", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.shutdownPolicy} {.metadata.creationTimestamp}")
	if err != nil {
		t.Fatalf("failed to get sandbox %s in ao-sandboxes: %v", sessionName, err)
	}

	parts := strings.Fields(sbJSON)
	if len(parts) < 2 {
		t.Fatalf("expected shutdownPolicy and creationTimestamp, got: %q", sbJSON)
	}
	shutdownPolicy := parts[0]
	creationTimestampStr := parts[1]

	if shutdownPolicy != "Delete" {
		t.Errorf("shutdownPolicy = %q, want Delete", shutdownPolicy)
	}

	// 6. Assert the in-cluster server did the work:
	// The sandbox's creation must postdate the test start.
	creationTime, err := time.Parse(time.RFC3339, creationTimestampStr)
	if err != nil {
		t.Fatalf("failed to parse sandbox creationTimestamp %q: %v", creationTimestampStr, err)
	}
	if creationTime.Before(testStart.Add(-1 * time.Second)) {
		t.Fatalf("sandbox creation timestamp %v predates test start %v", creationTime, testStart)
	}

	// The ao pod's logs must contain the session name.
	aoPodName, err := subH.Kubectl("get", "pod", "-n", "ao-system", "-l", "app=ao", "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil || aoPodName == "" {
		t.Fatalf("failed to find ao pod in ao-system: %v", err)
	}
	aoLogs := subH.GetPodLogs("ao-system", aoPodName)
	if !strings.Contains(aoLogs, sessionName) {
		t.Fatalf("expected ao pod logs in ao-system to contain session name %q, got:\n%s", sessionName, aoLogs)
	}

	t.Logf("PASS: in-cluster ao server successfully handled turn for session %s (sandbox created at %s, found in pod %s logs)", sessionName, creationTimestampStr, aoPodName)
}

func TestRunLifecycle(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e run lifecycle tests")
	}

	clusterName := os.Getenv("KIND_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "auto-optimizer"
	}

	h := NewHarness(t, clusterName)
	h.Setup()

	gitRoot, err := h.getGitRoot()
	if err != nil {
		t.Fatalf("failed to get git root: %v", err)
	}

	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}

	// 1. Create a bare git repository with a goal in .goals/hello.md
	gitBaseDir := t.TempDir()
	bareRepoDir := filepath.Join(gitBaseDir, "hello-repo.git")
	if out, err := exec.Command("git", "init", "--bare", bareRepoDir).CombinedOutput(); err != nil {
		t.Fatalf("failed to init bare git repo: %v\n%s", err, string(out))
	}

	workDir := t.TempDir()
	if out, err := exec.Command("git", "clone", bareRepoDir, workDir).CombinedOutput(); err != nil {
		t.Fatalf("failed to clone bare repo: %v\n%s", err, string(out))
	}

	goalsDir := filepath.Join(workDir, ".goals")
	if err := os.MkdirAll(goalsDir, 0755); err != nil {
		t.Fatalf("failed to create .goals dir: %v", err)
	}
	goalContent := "# Hello\n\nAdd a line to README\n"
	if err := os.WriteFile(filepath.Join(goalsDir, "hello.md"), []byte(goalContent), 0644); err != nil {
		t.Fatalf("failed to write goal file: %v", err)
	}

	runGit := func(dir string, args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\nOutput: %s", args, dir, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	runGit(workDir, "add", ".")
	runGit(workDir, "-c", "user.name=test", "-c", "user.email=test@test.com", "commit", "-m", "add hello goal")
	baseCommit := runGit(workDir, "rev-parse", "HEAD")
	runGit(workDir, "push", "origin", "HEAD:main")
	runGit(bareRepoDir, "symbolic-ref", "HEAD", "refs/heads/main")

	// 2. Locate git-http-backend via git --exec-path and kind IPv4 gateway via docker network inspect kind
	execPathOut, err := exec.Command("git", "--exec-path").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to locate git exec-path: %v\nOutput: %s", err, string(execPathOut))
	}
	execPath := strings.TrimSpace(string(execPathOut))
	gitHTTPBackend := filepath.Join(execPath, "git-http-backend")
	if _, err := os.Stat(gitHTTPBackend); err != nil {
		t.Fatalf("git-http-backend binary not found at %s (checked git --exec-path): %v", gitHTTPBackend, err)
	}

	dockerCmd := exec.Command("docker", "network", "inspect", "kind")
	var netInspectErr bytes.Buffer
	dockerCmd.Stderr = &netInspectErr
	netInspectOut, err := dockerCmd.Output()
	if err != nil {
		t.Fatalf("failed to inspect docker network kind: %v\nStderr: %s", err, netInspectErr.String())
	}
	if idx := bytes.IndexByte(netInspectOut, '['); idx >= 0 {
		netInspectOut = netInspectOut[idx:]
	}

	type dockerNetConfig struct {
		IPAM struct {
			Config []struct {
				Subnet  string `json:"Subnet"`
				Gateway string `json:"Gateway"`
			} `json:"Config"`
		} `json:"IPAM"`
	}
	var dockerNets []dockerNetConfig
	if err := json.Unmarshal(netInspectOut, &dockerNets); err != nil || len(dockerNets) == 0 {
		t.Fatalf("failed to parse docker network inspect kind output: %v\nOutput: %s", err, string(netInspectOut))
	}

	var kindGatewayIPv4 string
	for _, cfg := range dockerNets[0].IPAM.Config {
		ip := net.ParseIP(cfg.Gateway)
		if ip != nil && ip.To4() != nil && cfg.Gateway != "" {
			kindGatewayIPv4 = cfg.Gateway
			break
		}
	}
	if kindGatewayIPv4 == "" {
		t.Fatalf("could not find IPv4 gateway in docker network inspect kind: %s", string(netInspectOut))
	}

	// Start HTTP git server accessible to kind nodes at kindGatewayIPv4
	gitListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("failed to listen for git HTTP server: %v", err)
	}
	defer gitListener.Close()

	gitPort := gitListener.Addr().(*net.TCPAddr).Port
	gitHandler := &cgi.Handler{
		Path: gitHTTPBackend,
		Env: []string{
			"GIT_PROJECT_ROOT=" + gitBaseDir,
			"GIT_HTTP_EXPORT_ALL=1",
		},
	}
	gitServer := &http.Server{Handler: gitHandler}
	go func() { _ = gitServer.Serve(gitListener) }()
	t.Cleanup(func() { _ = gitServer.Close() })

	repoURL := fmt.Sprintf("http://%s:%d/hello-repo.git", kindGatewayIPv4, gitPort)
	t.Logf("Git repository available at %s (base commit %s, backend %s)", repoURL, baseCommit, gitHTTPBackend)

	// 3. Start ao serve with the repository registered
	aoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port for ao serve: %v", err)
	}
	serverAddr := aoListener.Addr().String()
	_ = aoListener.Close()

	stateDir := t.TempDir()
	serveCmd := exec.Command(aoBin, "serve",
		"--listen", serverAddr,
		"--state-dir", stateDir,
		"--repo", repoURL,
	)
	serveCmd.Env = append(os.Environ(), "OPENAI_MODEL=fake-eliza")

	var serveStdout, serveStderr bytes.Buffer
	serveCmd.Stdout = &serveStdout
	serveCmd.Stderr = &serveStderr

	t.Logf("Starting ao serve on %s with registered repo %s...", serverAddr, repoURL)
	if err := serveCmd.Start(); err != nil {
		t.Fatalf("failed to start ao serve: %v", err)
	}
	t.Cleanup(func() {
		if serveCmd.Process != nil {
			_ = serveCmd.Process.Signal(syscall.SIGTERM)
			_ = serveCmd.Wait()
		}
	})

	// Wait for server to accept connections
	serverReady := false
	for range 50 {
		conn, err := net.DialTimeout("tcp", serverAddr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			serverReady = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !serverReady {
		t.Fatalf("timed out waiting for ao serve on %s\nStderr: %s", serverAddr, serveStderr.String())
	}

	// 4. Poll for run to reach Working phase
	type jsonRun struct {
		ID          string `json:"id"`
		Phase       string `json:"phase"`
		BaseCommit  string `json:"baseCommit"`
		SandboxName string `json:"sandboxName"`
		Error       string `json:"error"`
	}
	type jsonGetRunResponse struct {
		Run jsonRun `json:"run"`
	}
	type jsonListRunsResponse struct {
		Runs []jsonRun `json:"runs"`
	}

	var runID string
	var workingRun *jsonRun

	t.Log("Waiting for run to reach Working phase...")
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		// List runs
		listCmd := exec.Command(aoBin, "run", "list",
			"--server", serverAddr,
			"--repo", repoURL,
			"--json",
		)
		out, err := listCmd.CombinedOutput()
		if err == nil {
			var listResp jsonListRunsResponse
			if jsonErr := json.Unmarshal(out, &listResp); jsonErr == nil && len(listResp.Runs) > 0 {
				runID = listResp.Runs[0].ID
				// Get run details
				getCmd := exec.Command(aoBin, "run", "get", runID,
					"--server", serverAddr,
					"--json",
				)
				getOut, getErr := getCmd.CombinedOutput()
				if getErr == nil {
					var getResp jsonGetRunResponse
					if unmarshalErr := json.Unmarshal(getOut, &getResp); unmarshalErr == nil {
						if getResp.Run.Phase == "Working" {
							workingRun = &getResp.Run
							break
						} else if getResp.Run.Phase == "Failed" {
							t.Fatalf("run failed prematurely with error: %s", getResp.Run.Error)
						}
					}
				}
			}
		}
		time.Sleep(1 * time.Second)
	}

	if workingRun == nil {
		t.Fatalf("timed out waiting for run to reach Working phase. Serve stderr:\n%s", serveStderr.String())
	}

	t.Logf("PASS: Run %s reached phase %s with sandbox %s", workingRun.ID, workingRun.Phase, workingRun.SandboxName)
	if workingRun.BaseCommit != baseCommit {
		t.Errorf("expected base commit %s, got %s", baseCommit, workingRun.BaseCommit)
	}

	sandboxName := workingRun.SandboxName
	subH := h.WithT(t)
	t.Cleanup(func() {
		subH.DeleteWithCleanup("ao-sandboxes", "sandbox", sandboxName)
	})

	// 5. Verify sandbox in Kubernetes is Ready
	subH.WaitForCondition("ao-sandboxes", "sandbox/"+sandboxName, "condition=Ready", 60*time.Second)

	// 6. Verify sandbox contains clone at baseCommit using kubectl exec
	headOut, err := subH.Kubectl("exec", "-n", "ao-sandboxes", sandboxName, "-c", "sandbox", "--", "git", "-C", "/workspace/repo", "rev-parse", "HEAD")
	if err != nil {
		t.Fatalf("failed to get cloned repo HEAD inside sandbox: %v\nOutput: %s", err, headOut)
	}
	gotHead := strings.TrimSpace(headOut)
	if gotHead != baseCommit {
		t.Errorf("cloned repository inside sandbox has commit %s, want base commit %s", gotHead, baseCommit)
	}

	// Verify goal file is in cloned repository inside sandbox
	goalCatOut, err := subH.Kubectl("exec", "-n", "ao-sandboxes", sandboxName, "-c", "sandbox", "--", "cat", "/workspace/repo/.goals/hello.md")
	if err != nil {
		t.Fatalf("failed to read .goals/hello.md inside sandbox: %v\nOutput: %s", err, goalCatOut)
	}
	if !strings.Contains(goalCatOut, "Add a line to README") {
		t.Errorf("unexpected content in cloned .goals/hello.md: %s", goalCatOut)
	}

	// 7. Verify ao run get prints formatted run and events
	getFmtCmd := exec.Command(aoBin, "run", "get", runID, "--server", serverAddr)
	getFmtOut, err := getFmtCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ao run get failed: %v\nOutput: %s", err, string(getFmtOut))
	}
	fmtStr := string(getFmtOut)
	if !strings.Contains(fmtStr, "Working") || !strings.Contains(fmtStr, sandboxName) || !strings.Contains(fmtStr, "Events:") {
		t.Errorf("expected formatted run output to contain Working, sandbox name, and Events, got:\n%s", fmtStr)
	}

	t.Logf("PASS: sandbox %s contains clone at base commit %s with .goals/hello.md", sandboxName, baseCommit)
}
