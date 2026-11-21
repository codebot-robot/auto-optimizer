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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	herorunv1alpha1 "github.com/gke-labs/auto-optimizer/api/herorun/v1alpha1"
	"github.com/gke-labs/auto-optimizer/pkg/session"
	"github.com/gke-labs/auto-optimizer/pkg/tools"
	corev1 "k8s.io/api/core/v1"
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

// assertSandboxdAuthAndNoSecretsRBAC asserts:
// 1. The sandbox pod's init container command/args do NOT contain --auth-token-sha256.
// 2. The sandbox pod's sandbox container command/args contain --auth-public-key and --auth-audience, and no --auth-token-sha256.
// 3. The control plane service account has no permissions to create secrets in ao-system.
func assertSandboxdAuthAndNoSecretsRBAC(t *testing.T, subH *Harness, sessionName string) {
	t.Helper()

	// Verify init container command/args do NOT contain --auth-token-sha256
	initArgs, err := subH.Kubectl("get", "pod", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.initContainers[0].command} {.spec.initContainers[0].args}")
	if err != nil {
		t.Fatalf("failed to get pod initContainer command/args: %v", err)
	}
	if strings.Contains(initArgs, "--auth-token-sha256") {
		t.Errorf("expected initContainer command/args to not contain '--auth-token-sha256', got: %s", initArgs)
	}

	// Verify sandbox container command/args contain --auth-public-key and --auth-audience, and no --auth-token-sha256
	sandboxArgs, err := subH.Kubectl("get", "pod", "-n", "ao-sandboxes", sessionName, "-o", "jsonpath={.spec.containers[0].command} {.spec.containers[0].args}")
	if err != nil {
		t.Fatalf("failed to get sandbox container command/args: %v", err)
	}
	if !strings.Contains(sandboxArgs, "--auth-public-key") {
		t.Errorf("expected sandbox container command/args to contain '--auth-public-key', got: %s", sandboxArgs)
	}
	if !strings.Contains(sandboxArgs, "--auth-audience") {
		t.Errorf("expected sandbox container command/args to contain '--auth-audience', got: %s", sandboxArgs)
	}
	if strings.Contains(sandboxArgs, "--auth-token-sha256") {
		t.Errorf("expected sandbox container command/args to not contain '--auth-token-sha256', got: %s", sandboxArgs)
	}

	// Verify control plane RBAC does NOT include create secrets in ao-system
	canCreateSecrets, err := subH.Kubectl("auth", "can-i", "create", "secrets", "--as=system:serviceaccount:ao-system:ao", "-n", "ao-system")
	if err != nil && !strings.Contains(canCreateSecrets, "no") {
		t.Fatalf("kubectl auth can-i create secrets failed: %v (output: %s)", err, canCreateSecrets)
	}
	if strings.TrimSpace(canCreateSecrets) != "no" {
		t.Errorf("expected control plane RBAC to not include create secrets in ao-system, got auth can-i result: %q", canCreateSecrets)
	}
}

// startFixtureGitServer starts an HTTP git server for the bare repository at bareRepoDir
// accessible to kind cluster nodes via a Kubernetes Service and EndpointSlice, and starts
// an in-process HTTP forward proxy on localhost so host-side commands (git and ao serve)
// can resolve the Service name without privileges.
func startFixtureGitServer(t *testing.T, subH *Harness, bareRepoDir string) string {
	t.Helper()

	bareRepoDir = filepath.Clean(bareRepoDir)
	if out, err := exec.Command("git", "-C", bareRepoDir, "config", "http.receivepack", "true").CombinedOutput(); err != nil {
		t.Fatalf("failed to set http.receivepack on bare repo: %v\nOutput: %s", err, string(out))
	}

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

	gitListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("failed to listen for git HTTP server: %v", err)
	}

	gitPort := gitListener.Addr().(*net.TCPAddr).Port
	gitHandler := &cgi.Handler{
		Path: gitHTTPBackend,
		Env: []string{
			"GIT_PROJECT_ROOT=" + filepath.Dir(bareRepoDir),
			"GIT_HTTP_EXPORT_ALL=1",
		},
	}
	gitServer := &http.Server{Handler: gitHandler}
	go func() { _ = gitServer.Serve(gitListener) }()
	t.Cleanup(func() { _ = gitServer.Close() })

	svcName := fmt.Sprintf("git-fixture-%d", gitPort)
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: ao-sandboxes
spec:
  ports:
    - name: http
      port: 80
      targetPort: %d
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: %s
  namespace: ao-sandboxes
  labels:
    kubernetes.io/service-name: %s
addressType: IPv4
ports:
  - name: http
    port: %d
endpoints:
  - addresses:
      - "%s"
`, svcName, gitPort, svcName, svcName, gitPort, kindGatewayIPv4)

	if _, err := subH.KubectlWithStdin(manifest, "apply", "-f", "-"); err != nil {
		t.Fatalf("failed to create fixture git service and endpointslice: %v", err)
	}
	t.Cleanup(func() {
		_, _ = subH.Kubectl("delete", "service", svcName, "-n", "ao-sandboxes", "--ignore-not-found")
		_, _ = subH.Kubectl("delete", "endpointslice", svcName, "-n", "ao-sandboxes", "--ignore-not-found")
	})

	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for forward HTTP proxy: %v", err)
	}
	proxyPort := proxyListener.Addr().(*net.TCPAddr).Port
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			if strings.Contains(req.Host, ".svc.cluster.local") || strings.Contains(req.URL.Host, ".svc.cluster.local") {
				req.URL.Scheme = "http"
				req.URL.Host = fmt.Sprintf("127.0.0.1:%d", gitPort)
			}
		},
	}
	proxyServer := &http.Server{Handler: proxy}
	go func() { _ = proxyServer.Serve(proxyListener) }()
	t.Cleanup(func() { _ = proxyServer.Close() })

	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", proxyPort)
	t.Setenv("HTTP_PROXY", proxyURL)
	t.Setenv("http_proxy", proxyURL)
	t.Setenv("NO_PROXY", "127.0.0.1,localhost")
	t.Setenv("no_proxy", "127.0.0.1,localhost")

	repoHost := fmt.Sprintf("%s.ao-sandboxes.svc.cluster.local", svcName)
	repoURL := fmt.Sprintf("http://%s:80/%s", repoHost, filepath.Base(bareRepoDir))
	t.Logf("Git repository available at %s (backend %s)", repoURL, gitHTTPBackend)
	return repoURL
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
  securityContext:
    runAsUser: 1000
    fsGroup: 1000
  volumes:
    - name: objectfs-vol
      persistentVolumeClaim:
        claimName: %s
  containers:
    - name: writer
      image: busybox:latest
      command: ["sh", "-c", "mkdir /workspace/testdir && echo 'smoke test data' > /workspace/testdir/test.txt && cat /workspace/testdir/test.txt && exit 0"]
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

	t.Run("ServicePortals", func(t *testing.T) {
		subH := h.WithT(t)
		subH.WaitForCondition("service-portal-system", "deployment/service-portal-controller", "condition=Available", 60*time.Second)
		if out, err := subH.Kubectl("rollout", "status", "daemonset/portal-node-agent", "-n", "service-portal-system", "--timeout=60s"); err != nil {
			t.Fatalf("portal-node-agent daemonset not ready: %v, output: %s", err, out)
		}
		t.Logf("PASS: service-portals controller and node agent are ready.")
	})

	t.Run("SandboxGoImage", func(t *testing.T) {
		subH := h.WithT(t)
		podName := "smoke-test-sandbox-go"
		subH.DeleteWithCleanup("default", "pod", podName)

		imagePrefix := os.Getenv("IMAGE_PREFIX")
		if imagePrefix == "" {
			imagePrefix = "auto-optimizer.local"
		}
		imageTag := os.Getenv("IMAGE_TAG")
		if imageTag == "" {
			imageTag = "dev"
		}
		image := fmt.Sprintf("%s/sandbox-go:%s", imagePrefix, imageTag)

		manifest := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
spec:
  restartPolicy: Never
  securityContext:
    runAsUser: 1000
    runAsGroup: 1000
    runAsNonRoot: true
  containers:
    - name: sandbox-go
      image: %s
      command: ["sh", "-c", "go version && cd $(mktemp -d) && printf 'package main\nfunc main() {}\n' > main.go && go build main.go"]
`, podName, image)

		subH.ApplyManifest(manifest)

		// Wait for pod to complete
		subH.WaitForPodSuccess("default", podName, 60*time.Second)

		// Verify pod logs
		logs := subH.GetPodLogs("default", podName)
		if !strings.Contains(logs, "go version") {
			t.Fatalf("Unexpected pod logs from %s: %s", podName, logs)
		}
		t.Logf("PASS: sandbox-go ran go version and go build as UID 1000 and exited 0.")
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

	// Verify sandboxd asymmetric auth container args and control plane Secrets RBAC
	assertSandboxdAuthAndNoSecretsRBAC(t, subH, sessionName)

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
		"--insecure-no-auth",
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

	// Verify sandboxd asymmetric auth container args and control plane Secrets RBAC
	assertSandboxdAuthAndNoSecretsRBAC(t, subH, sessionName)

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

	// 7. Verify ao-config ConfigMap has AO_REPOS and AO_PUBLIC_URL keys, and deployment/ao has ao-forge secretRef in envFrom
	reposVal, err := subH.Kubectl("get", "configmap", "-n", "ao-system", "ao-config", "-o", "jsonpath={.data.AO_REPOS}")
	if err != nil {
		t.Fatalf("failed to read AO_REPOS from ao-config: %v", err)
	}
	if reposVal != "" {
		t.Errorf("expected AO_REPOS in ao-config to default to empty, got: %q", reposVal)
	}

	publicURLVal, err := subH.Kubectl("get", "configmap", "-n", "ao-system", "ao-config", "-o", "jsonpath={.data.AO_PUBLIC_URL}")
	if err != nil {
		t.Fatalf("failed to read AO_PUBLIC_URL from ao-config: %v", err)
	}
	if publicURLVal != "" {
		t.Errorf("expected AO_PUBLIC_URL in ao-config to default to empty, got: %q", publicURLVal)
	}

	envFromVal, err := subH.Kubectl("get", "deployment", "-n", "ao-system", "ao", "-o", "jsonpath={.spec.template.spec.containers[0].envFrom}")
	if err != nil {
		t.Fatalf("failed to read envFrom from deployment ao: %v", err)
	}
	if !strings.Contains(envFromVal, "ao-forge") {
		t.Errorf("expected deployment envFrom to contain ao-forge, got: %s", envFromVal)
	}
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s failed: %v\nOutput: %s", args, dir, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

func buildAOBinary(t *testing.T, gitRoot string) string {
	t.Helper()
	aoBin := filepath.Join(t.TempDir(), "ao")
	buildCmd := exec.Command("go", "build", "-o", aoBin, "./cmd/ao")
	buildCmd.Dir = gitRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build ao binary: %v\nOutput: %s", err, string(out))
	}
	return aoBin
}

func setupHelloRepoFixture(t *testing.T, subH *Harness) (bareRepoDir, repoURL, baseCommit string) {
	t.Helper()
	gitBaseDir := t.TempDir()
	bareRepoDir = filepath.Join(gitBaseDir, "hello-repo.git")
	if out, err := exec.Command("git", "init", "--bare", bareRepoDir).CombinedOutput(); err != nil {
		t.Fatalf("failed to init bare git repo: %v\n%s", err, string(out))
	}

	workDir := t.TempDir()
	if out, err := exec.Command("git", "clone", bareRepoDir, workDir).CombinedOutput(); err != nil {
		t.Fatalf("failed to clone bare repo: %v\n%s", err, string(out))
	}

	readmeContent := "# Hello Repo\n\nThis is a sample README to read and summarise.\n"
	if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte(readmeContent), 0644); err != nil {
		t.Fatalf("failed to write README.md: %v", err)
	}

	goalsDir := filepath.Join(workDir, ".goals")
	if err := os.MkdirAll(goalsDir, 0755); err != nil {
		t.Fatalf("failed to create .goals dir: %v", err)
	}
	goalContent := "# Hello\n\nread the README and summarise it\n"
	if err := os.WriteFile(filepath.Join(goalsDir, "hello.md"), []byte(goalContent), 0644); err != nil {
		t.Fatalf("failed to write goal file: %v", err)
	}

	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "-c", "user.name=test", "-c", "user.email=test@test.com", "commit", "-m", "add hello goal")
	baseCommit = runGit(t, workDir, "rev-parse", "HEAD")
	runGit(t, workDir, "push", "origin", "HEAD:main")
	runGit(t, bareRepoDir, "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(t, bareRepoDir, "config", "http.receivepack", "true")

	repoURL = startFixtureGitServer(t, subH, bareRepoDir)
	t.Logf("Git repository available at %s (base commit %s)", repoURL, baseCommit)

	// Assert the fixture git server accepts pushes
	runGit(t, workDir, "push", repoURL, "HEAD:probe")
	if got := runGit(t, bareRepoDir, "rev-parse", "refs/heads/probe"); got != baseCommit {
		t.Fatalf("expected bare repo to have branch probe at %s, got %s", baseCommit, got)
	}
	runGit(t, bareRepoDir, "branch", "-D", "probe")
	return bareRepoDir, repoURL, baseCommit
}

func startAOServe(t *testing.T, subH *Harness, aoBin, repoURL string, env ...string) (serverAddr, httpAddr, stateDir string, serveStderr *bytes.Buffer) {
	t.Helper()
	aoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port for ao serve: %v", err)
	}
	serverAddr = aoListener.Addr().String()
	_ = aoListener.Close()

	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port for ao serve http: %v", err)
	}
	httpAddr = httpListener.Addr().String()
	_ = httpListener.Close()

	stateDir = t.TempDir()

	args := []string{
		"serve",
		"--listen", serverAddr,
		"--http", httpAddr,
		"--state-dir", stateDir,
		"--repo", repoURL,
		"--insecure-no-auth",
		"--workspace-storage-class=objectfs-exclusive-local",
	}

	if subH != nil {
		portalAddr, stopPortal := subH.PortForwardService("service-portal-system", "svc/service-portal-controller", 8443)
		t.Cleanup(stopPortal)

		caCertB64, err := subH.Kubectl("get", "secret", "-n", "ao-system", "service-portal-controller-ca", "-o", "jsonpath={.data.tls\\.crt}")
		if err != nil {
			t.Fatalf("failed to read service-portal-controller-ca secret: %v", err)
		}
		caCertBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(caCertB64))
		if err != nil {
			t.Fatalf("failed to decode service-portal-controller-ca: %v", err)
		}
		caFile := filepath.Join(stateDir, "portal-ca.crt")
		if err := os.WriteFile(caFile, caCertBytes, 0600); err != nil {
			t.Fatalf("failed to write portal-ca.crt: %v", err)
		}

		token, err := subH.Kubectl("create", "token", "ao", "-n", "ao-system", "--audience=https://service-portal-controller")
		if err != nil {
			t.Fatalf("failed to create portal token for ao: %v", err)
		}
		tokenFile := filepath.Join(stateDir, "portal-token")
		if err := os.WriteFile(tokenFile, []byte(strings.TrimSpace(token)), 0600); err != nil {
			t.Fatalf("failed to write portal-token: %v", err)
		}

		parsedURL, err := url.Parse(repoURL)
		if err != nil {
			t.Fatalf("failed to parse repo URL %s: %v", repoURL, err)
		}
		hosts := "github.com,http://" + parsedURL.Host

		args = append(args,
			"--portal-controller-addr="+portalAddr,
			"--portal-controller-ca-file="+caFile,
			"--portal-token-file="+tokenFile,
			"--sandbox-egress-hosts="+hosts,
		)
	}

	serveCmd := exec.Command(aoBin, args...)
	serveCmd.Env = append(os.Environ(), env...)

	var serveStdout bytes.Buffer
	serveStderr = &bytes.Buffer{}
	serveCmd.Stdout = &serveStdout
	serveCmd.Stderr = serveStderr

	t.Logf("Starting ao serve on %s (http %s) with registered repo %s...", serverAddr, httpAddr, repoURL)
	if err := serveCmd.Start(); err != nil {
		t.Fatalf("failed to start ao serve: %v", err)
	}
	t.Cleanup(func() {
		if serveCmd.Process != nil {
			_ = serveCmd.Process.Signal(syscall.SIGTERM)
			_ = serveCmd.Wait()
		}
	})

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
	return serverAddr, httpAddr, stateDir, serveStderr
}

type jsonRun struct {
	ID          string `json:"id"`
	Phase       string `json:"phase"`
	BaseCommit  string `json:"baseCommit"`
	Branch      string `json:"branch"`
	SessionID   string `json:"sessionId"`
	SandboxName string `json:"sandboxName"`
	Error       string `json:"error"`
}

type jsonEvent struct {
	Phase   string `json:"phase"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

type jsonGetRunResponse struct {
	Run    jsonRun     `json:"run"`
	Events []jsonEvent `json:"events"`
}

type jsonListRunsResponse struct {
	Runs []jsonRun `json:"runs"`
}

func pollRunPhase(t *testing.T, aoBin, serverAddr, repoURL, targetPhase string, timeout time.Duration, serveStderr *bytes.Buffer, onRun ...func(run *jsonRun)) (*jsonRun, []jsonEvent) {
	t.Helper()
	var runID string
	var finalRun *jsonRun
	var finalEvents []jsonEvent

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
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
				getCmd := exec.Command(aoBin, "run", "get", runID,
					"--server", serverAddr,
					"--json",
				)
				getOut, getErr := getCmd.CombinedOutput()
				if getErr == nil {
					var getResp jsonGetRunResponse
					if unmarshalErr := json.Unmarshal(getOut, &getResp); unmarshalErr == nil {
						for _, fn := range onRun {
							fn(&getResp.Run)
						}
						if getResp.Run.Phase == targetPhase {
							finalRun = &getResp.Run
							finalEvents = getResp.Events
							break
						} else if getResp.Run.Phase == "Failed" {
							t.Fatalf("run failed prematurely with error: %s\nServe stderr:\n%s", getResp.Run.Error, serveStderr.String())
						}
					}
				}
			}
		}
		time.Sleep(1 * time.Second)
	}

	if finalRun == nil {
		t.Fatalf("timed out waiting for run to reach %s phase. Serve stderr:\n%s", targetPhase, serveStderr.String())
	}
	return finalRun, finalEvents
}

func assertSandboxPodObjectFS(t *testing.T, subH *Harness, sandboxName string) {
	t.Helper()

	var pod corev1.Pod
	var podErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		out, err := subH.Kubectl("get", "pod", "-n", "ao-sandboxes", sandboxName, "-o", "json")
		if err != nil {
			podErr = err
		} else if jsonErr := json.Unmarshal([]byte(out), &pod); jsonErr != nil {
			podErr = fmt.Errorf("parsing pod JSON: %w", jsonErr)
		} else if len(pod.Spec.Containers) == 0 {
			podErr = fmt.Errorf("pod %s has no containers", sandboxName)
		} else {
			podErr = nil
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if podErr != nil {
		t.Fatalf("failed to get pod %s in ao-sandboxes: %v", sandboxName, podErr)
	}

	var workspaceVolName string
	for _, c := range pod.Spec.Containers {
		for _, m := range c.VolumeMounts {
			if m.MountPath == "/workspace" {
				workspaceVolName = m.Name
				break
			}
		}
		if workspaceVolName != "" {
			break
		}
	}
	if workspaceVolName == "" {
		t.Fatalf("expected pod %s to mount a volume at /workspace", sandboxName)
	}

	var claimName string
	for _, v := range pod.Spec.Volumes {
		if v.Name == workspaceVolName {
			if v.PersistentVolumeClaim == nil {
				t.Fatalf("expected workspace volume %q on pod %s to be a PersistentVolumeClaim, got %+v", v.Name, sandboxName, v)
			}
			claimName = v.PersistentVolumeClaim.ClaimName
			break
		}
	}
	if claimName == "" {
		t.Fatalf("expected workspace volume %q on pod %s to have a ClaimName", workspaceVolName, sandboxName)
	}

	var pvc corev1.PersistentVolumeClaim
	var pvcErr error
	pvcDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(pvcDeadline) {
		out, err := subH.Kubectl("get", "pvc", "-n", "ao-sandboxes", claimName, "-o", "json")
		if err == nil {
			if jsonErr := json.Unmarshal([]byte(out), &pvc); jsonErr == nil {
				if pvc.Status.Phase == corev1.ClaimBound {
					pvcErr = nil
					break
				}
				pvcErr = fmt.Errorf("pvc %s phase is %s (want Bound)", claimName, pvc.Status.Phase)
			} else {
				pvcErr = jsonErr
			}
		} else {
			pvcErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	if pvcErr != nil {
		t.Fatalf("failed to verify bound PVC %s: %v", claimName, pvcErr)
	}

	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "objectfs-exclusive-local" {
		t.Fatalf("expected claim %s StorageClassName to be 'objectfs-exclusive-local', got %v", claimName, pvc.Spec.StorageClassName)
	}

	t.Logf("PASS: Sandbox pod %s mounts claim %s bound through StorageClass %s at /workspace", sandboxName, claimName, *pvc.Spec.StorageClassName)
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

	aoBin := buildAOBinary(t, gitRoot)

	subH := h.WithT(t)
	_, _ = subH.Kubectl("delete", "heroruns", "--all", "-n", "ao-sandboxes")
	t.Cleanup(func() {
		_, _ = subH.Kubectl("delete", "heroruns", "--all", "-n", "ao-sandboxes")
	})

	_, repoURL, baseCommit := setupHelloRepoFixture(t, subH)
	serverAddr, _, stateDir, serveStderr := startAOServe(t, subH, aoBin, repoURL, "OPENAI_MODEL=fake-eliza")

	var assertedObjectFS bool
	t.Log("Waiting for run to reach Working and then NoImprovement phase...")
	finalRun, _ := pollRunPhase(t, aoBin, serverAddr, repoURL, "NoImprovement", 90*time.Second, serveStderr, func(r *jsonRun) {
		if !assertedObjectFS && r.SandboxName != "" {
			assertSandboxPodObjectFS(t, subH, r.SandboxName)
			assertedObjectFS = true
		}
	})
	if !assertedObjectFS {
		t.Fatal("expected to assert Sandbox pod ObjectFS mount during run")
	}
	runID := finalRun.ID

	t.Logf("PASS: Run %s reached phase %s with session %s", finalRun.ID, finalRun.Phase, finalRun.SessionID)
	if finalRun.BaseCommit != baseCommit {
		t.Errorf("expected base commit %s, got %s", baseCommit, finalRun.BaseCommit)
	}
	if finalRun.SessionID == "" {
		t.Errorf("expected non-empty SessionID on run")
	}

	sandboxName := finalRun.SandboxName

	// 5. Verify sandbox was deleted in Kubernetes after session ended
	deleteDeadline := time.Now().Add(30 * time.Second)
	var sbDeleted bool
	for time.Now().Before(deleteDeadline) {
		_, getErr := subH.Kubectl("get", "-n", "ao-sandboxes", "sandbox", sandboxName)
		if getErr != nil {
			sbDeleted = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !sbDeleted {
		t.Errorf("expected sandbox %s to be deleted after session ended, but it still exists", sandboxName)
	}

	// 6. Verify session transcript in stateDir shows the agent reading the cloned repository
	sessStore := session.NewFileStore(stateDir)
	msgs, _, err := sessStore.LoadSession(t.Context(), finalRun.SessionID)
	if err != nil {
		t.Fatalf("failed to load session %s from stateDir %s: %v", finalRun.SessionID, stateDir, err)
	}
	var foundReadToolCall, foundToolResult bool
	for _, msg := range msgs {
		for _, tc := range msg.ToolCalls {
			if strings.Contains(tc.Function.Arguments, "README.md") {
				foundReadToolCall = true
			}
		}
		if msg.Role == "tool" && msg.Content != nil && strings.Contains(*msg.Content, "This is a sample README to read and summarise.") {
			foundToolResult = true
		}
	}
	if !foundReadToolCall {
		t.Errorf("expected session transcript to contain tool call reading README.md, messages: %+v", msgs)
	}
	if !foundToolResult {
		t.Errorf("expected session transcript to contain tool result with README contents, messages: %+v", msgs)
	}

	// 7. Verify ao run get prints formatted run and events
	getFmtCmd := exec.Command(aoBin, "run", "get", runID, "--server", serverAddr)
	getFmtOut, err := getFmtCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ao run get failed: %v\nOutput: %s", err, string(getFmtOut))
	}
	fmtStr := string(getFmtOut)
	if !strings.Contains(fmtStr, "NoImprovement") || !strings.Contains(fmtStr, "session ended without a pull request") || !strings.Contains(fmtStr, "Events:") {
		t.Errorf("expected formatted run output to contain NoImprovement, event message, and Events, got:\n%s", fmtStr)
	}

	// 8. Verify exactly one HeroRun exists in the sandbox namespace whose spec.runID matches the run,
	// and wait for status.phase to reflect the terminal phase after the flush interval.
	var finalHRPhase string
	var hrList herorunv1alpha1.HeroRunList
	flushDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(flushDeadline) {
		hrOut, err := subH.Kubectl("get", "heroruns", "-n", "ao-sandboxes", "-o", "json")
		if err == nil {
			if unmarshalErr := json.Unmarshal([]byte(hrOut), &hrList); unmarshalErr == nil && len(hrList.Items) == 1 {
				finalHRPhase = hrList.Items[0].Status.Phase
				if finalHRPhase == string(finalRun.Phase) {
					break
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(hrList.Items) != 1 {
		t.Fatalf("expected exactly 1 HeroRun in ao-sandboxes, got %d", len(hrList.Items))
	}
	if gotRunID := hrList.Items[0].Spec.RunID; gotRunID != finalRun.ID {
		t.Errorf("expected HeroRun spec.runID %q, got %q", finalRun.ID, gotRunID)
	}
	if finalHRPhase != string(finalRun.Phase) {
		t.Errorf("expected HeroRun status.phase %q, got %q", string(finalRun.Phase), finalHRPhase)
	}

	t.Logf("PASS: session transcript for run %s shows agent reading cloned repository README", runID)
}

func TestRunPushesBranch(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e run pushes branch tests")
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

	aoBin := buildAOBinary(t, gitRoot)
	subH := h.WithT(t)
	bareRepoDir, repoURL, baseCommit := setupHelloRepoFixture(t, subH)

	scriptJSON := `[{"tool":"run_command","args":{"command":"cd /workspace/repo && echo 'appended line for scripted run' >> README.md && git add README.md && git commit -m 'docs: add a line to the README'"}},{"tool":"open_pull_request","args":{"title":"docs: add a line to the README","body":"This scripted run adds a line to the README."}}]`

	serverAddr, httpAddr, _, serveStderr := startAOServe(t, subH, aoBin, repoURL,
		"OPENAI_MODEL=fake-script",
		"AO_FAKE_SCRIPT="+scriptJSON,
	)

	t.Log("Waiting for run to reach Succeeded phase...")
	finalRun, finalEvents := pollRunPhase(t, aoBin, serverAddr, repoURL, "Succeeded", 90*time.Second, serveStderr)
	runID := finalRun.ID

	wantBranch := fmt.Sprintf("ao/hello/%s", runID)
	t.Logf("PASS: Run %s reached phase %s with branch %s", runID, finalRun.Phase, finalRun.Branch)

	// 1. Assert run record has Branch equal to ao/hello/<run-id>
	if finalRun.Branch != wantBranch {
		t.Errorf("run record branch = %q, want %q", finalRun.Branch, wantBranch)
	}

	// 2. Assert bare repository has that branch, its tip's parent is base commit, and its tree changes README.md
	branchRef := "refs/heads/" + wantBranch
	tipCommit := runGit(t, bareRepoDir, "rev-parse", branchRef)
	if tipCommit == "" || tipCommit == baseCommit {
		t.Errorf("expected branch %s to have a new tip commit distinct from base %s, got %s", wantBranch, baseCommit, tipCommit)
	}

	tipParent := runGit(t, bareRepoDir, "rev-parse", branchRef+"^")
	if tipParent != baseCommit {
		t.Errorf("expected branch %s tip parent to be base commit %s, got %s", wantBranch, baseCommit, tipParent)
	}

	diffTree := runGit(t, bareRepoDir, "diff-tree", "--no-commit-id", "--name-only", "-r", baseCommit, tipCommit)
	if !strings.Contains(diffTree, "README.md") {
		t.Errorf("expected branch %s tree diff against %s to contain README.md, got:\n%s", wantBranch, baseCommit, diffTree)
	}

	// 3. Assert events include branch_pushed and pull_request_not_opened
	var foundBranchPushed, foundPRNotOpened bool
	for _, ev := range finalEvents {
		if ev.Kind == "branch_pushed" {
			foundBranchPushed = true
		}
		if ev.Kind == "pull_request_not_opened" {
			foundPRNotOpened = true
		}
	}
	if !foundBranchPushed {
		t.Errorf("expected events to include branch_pushed, got events: %+v", finalEvents)
	}
	if !foundPRNotOpened {
		t.Errorf("expected events to include pull_request_not_opened, got events: %+v", finalEvents)
	}

	// Also verify via ao run get formatted output
	getFmtCmd := exec.Command(aoBin, "run", "get", runID, "--server", serverAddr)
	getFmtOut, err := getFmtCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ao run get failed: %v\nOutput: %s", err, string(getFmtOut))
	}
	fmtStr := string(getFmtOut)
	if !strings.Contains(fmtStr, "branch_pushed") || !strings.Contains(fmtStr, "pull_request_not_opened") {
		t.Errorf("expected formatted run output to mention events branch_pushed and pull_request_not_opened, got:\n%s", fmtStr)
	}

	// 4. Assert run page returns 200 and mentions the branch
	runPageURL := fmt.Sprintf("http://%s/runs/%s", httpAddr, runID)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(runPageURL)
	if err != nil {
		t.Fatalf("failed to fetch run page %s: %v", runPageURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("run page returned status %d, want 200 OK", resp.StatusCode)
	}
	pageBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read run page body: %v", err)
	}
	if !strings.Contains(string(pageBody), wantBranch) {
		t.Errorf("expected run page to mention branch %q, got body:\n%s", wantBranch, string(pageBody))
	}
	t.Logf("PASS: Run page %s returned 200 and contained branch %s", runPageURL, wantBranch)

	// 5. Verify sandbox was deleted in Kubernetes after session ended
	deleteDeadline := time.Now().Add(30 * time.Second)
	var sbDeleted bool
	for time.Now().Before(deleteDeadline) {
		_, getErr := subH.Kubectl("get", "-n", "ao-sandboxes", "sandbox", finalRun.SandboxName)
		if getErr != nil {
			sbDeleted = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !sbDeleted {
		t.Errorf("expected sandbox %s to be deleted after session ended, but it still exists", finalRun.SandboxName)
	}
}

func TestRunSandboxPortalIsolation(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e portal isolation test")
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

	aoBin := buildAOBinary(t, gitRoot)
	subH := h.WithT(t)

	_, _ = subH.Kubectl("delete", "heroruns", "--all", "-n", "ao-sandboxes")
	t.Cleanup(func() {
		_, _ = subH.Kubectl("delete", "heroruns", "--all", "-n", "ao-sandboxes")
	})

	aoClusterIP, err := subH.Kubectl("get", "service", "-n", "ao-system", "ao", "-o", "jsonpath={.spec.clusterIP}")
	if err != nil {
		t.Fatalf("failed to read ao service clusterIP: %v", err)
	}
	aoIP := strings.TrimSpace(aoClusterIP)

	k8sClusterIP, err := subH.Kubectl("get", "service", "-n", "default", "kubernetes", "-o", "jsonpath={.spec.clusterIP}")
	if err != nil {
		t.Fatalf("failed to read kubernetes service clusterIP: %v", err)
	}
	k8sIP := strings.TrimSpace(k8sClusterIP)

	_, repoURL, _ := setupHelloRepoFixture(t, subH)
	scriptJSON := `[{"tool":"run_command","args":{"command":"sleep 10"}}]`
	serverAddr, _, stateDir, serveStderr := startAOServe(t, subH, aoBin, repoURL,
		"OPENAI_MODEL=fake-script",
		"AO_FAKE_SCRIPT="+scriptJSON,
	)

	t.Log("Waiting for run to reach Working phase and verify portal isolation...")
	var assertedIsolation bool
	pollRunPhase(t, aoBin, serverAddr, repoURL, "Working", 90*time.Second, serveStderr, func(r *jsonRun) {
		if assertedIsolation || r.Phase != "Working" || r.SandboxName == "" {
			return
		}
		subH.WaitForCondition("ao-sandboxes", "sandbox/"+r.SandboxName, "condition=Ready", 60*time.Second)

		restConfig, err := session.GetRESTConfig()
		if err != nil {
			t.Fatalf("session.GetRESTConfig: %v", err)
		}
		sbClient, err := session.NewSandboxClient(restConfig)
		if err != nil {
			t.Fatalf("session.NewSandboxClient: %v", err)
		}
		signingKey, err := session.LoadOrGenerateSigningKey(filepath.Join(stateDir, "sandbox-signing-key"))
		if err != nil {
			t.Fatalf("loading signing key: %v", err)
		}
		runHarness := session.NewHarness(nil, sbClient, nil, session.RunOptions{
			Namespace:             "ao-sandboxes",
			SigningKey:            signingKey,
			ControlPlaneNamespace: "ao-system",
		})
		sb := runHarness.GetOrAttachSandbox(r.SandboxName)

		// 1. Positive control: request to listed test repository host succeeds
		resListed, errListed := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-sS", "-f", "--connect-timeout", "5", repoURL + "/info/refs?service=git-upload-pack"},
		})
		if errListed != nil {
			t.Fatalf("positive control exec failed: %v", errListed)
		}
		if resListed == nil || resListed.ExitCode != 0 {
			t.Fatalf("expected request to listed host on egress list to succeed, got exit code %d (stdout: %s, stderr: %s)", resListed.ExitCode, resListed.Stdout, resListed.Stderr)
		}
		t.Logf("PASS: request to listed host %s succeeded as expected (exit: %d)", repoURL, resListed.ExitCode)

		// 2. Negative check: connecting to control plane Service fails on web port 8080 (by name and address)
		resWeb, errWeb := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-s", "--connect-timeout", "3", "http://ao.ao-system.svc:8080/"},
		})
		if errWeb != nil {
			t.Fatalf("web port exec by name failed: %v", errWeb)
		}
		if resWeb == nil || resWeb.ExitCode == 0 {
			t.Errorf("expected connection to control plane web port 8080 by name to fail, but succeeded: %+v", resWeb)
		} else {
			t.Logf("PASS: connection to control plane web port 8080 failed as expected (exit: %d)", resWeb.ExitCode)
		}

		resWebIP, errWebIP := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-s", "--connect-timeout", "3", fmt.Sprintf("http://%s:8080/", aoIP)},
		})
		if errWebIP != nil {
			t.Fatalf("web port exec by address failed: %v", errWebIP)
		}
		if resWebIP == nil || resWebIP.ExitCode == 0 {
			t.Errorf("expected connection to control plane web port 8080 by address to fail, but succeeded: %+v", resWebIP)
		} else {
			t.Logf("PASS: connection to control plane web port 8080 by address %s failed as expected (exit: %d)", aoIP, resWebIP.ExitCode)
		}

		// 3. Negative check: connecting to control plane Service fails on --listen port 8081 (by name and address)
		resListen, errListen := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-s", "--connect-timeout", "3", "http://ao.ao-system.svc:8081/"},
		})
		if errListen != nil {
			t.Fatalf("listen port exec by name failed: %v", errListen)
		}
		if resListen == nil || resListen.ExitCode == 0 {
			t.Errorf("expected connection to control plane listen port 8081 by name to fail, but succeeded: %+v", resListen)
		} else {
			t.Logf("PASS: connection to control plane listen port 8081 failed as expected (exit: %d)", resListen.ExitCode)
		}

		resListenIP, errListenIP := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-s", "--connect-timeout", "3", fmt.Sprintf("http://%s:8081/", aoIP)},
		})
		if errListenIP != nil {
			t.Fatalf("listen port exec by address failed: %v", errListenIP)
		}
		if resListenIP == nil || resListenIP.ExitCode == 0 {
			t.Errorf("expected connection to control plane listen port 8081 by address to fail, but succeeded: %+v", resListenIP)
		} else {
			t.Logf("PASS: connection to control plane listen port 8081 by address %s failed as expected (exit: %d)", aoIP, resListenIP.ExitCode)
		}

		// 4. Negative check: request to unlisted host fails without --fail (by name and address)
		resUnlisted, errUnlisted := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-s", "-k", "--connect-timeout", "3", "https://kubernetes.default.svc/"},
		})
		if errUnlisted != nil {
			t.Fatalf("unlisted host exec by name failed: %v", errUnlisted)
		}
		if resUnlisted == nil || resUnlisted.ExitCode == 0 {
			t.Errorf("expected request to unlisted host by name to fail, but succeeded: %+v", resUnlisted)
		} else {
			t.Logf("PASS: request to unlisted host https://kubernetes.default.svc/ failed as expected (exit: %d)", resUnlisted.ExitCode)
		}

		resUnlistedIP, errUnlistedIP := sb.ExecCommand(t.Context(), tools.ExecCommandOptions{
			Command: []string{"curl", "-s", "-k", "--connect-timeout", "3", fmt.Sprintf("https://%s/", k8sIP)},
		})
		if errUnlistedIP != nil {
			t.Fatalf("unlisted host exec by address failed: %v", errUnlistedIP)
		}
		if resUnlistedIP == nil || resUnlistedIP.ExitCode == 0 {
			t.Errorf("expected request to unlisted host by address to fail, but succeeded: %+v", resUnlistedIP)
		} else {
			t.Logf("PASS: request to unlisted host https://%s/ failed as expected (exit: %d)", k8sIP, resUnlistedIP.ExitCode)
		}

		assertedIsolation = true
	})

	if !assertedIsolation {
		t.Errorf("expected isolation checks to run, but did not")
	}
}

func TestRunSandboxDeletedMidTurnWithPortal(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("RUN_E2E env var not set, skipping e2e test")
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

	aoBin := buildAOBinary(t, gitRoot)
	subH := h.WithT(t)

	_, _ = subH.Kubectl("delete", "heroruns", "--all", "-n", "ao-sandboxes")
	t.Cleanup(func() {
		_, _ = subH.Kubectl("delete", "heroruns", "--all", "-n", "ao-sandboxes")
	})

	_, repoURL, _ := setupHelloRepoFixture(t, subH)

	// Step 1: run sleep 10 so the command is in flight when sandbox is deleted.
	// Step 2: request test repository host via curl to prove portal policy is bound on replacement sandbox.
	// Step 3: commit changes and open a pull request to transition the run to Succeeded.
	scriptSteps := []map[string]any{
		{
			"tool": "run_command",
			"args": map[string]string{
				"command": "sleep 10",
			},
		},
		{
			"tool": "run_command",
			"args": map[string]string{
				"command": "cd /workspace/repo && echo 'replacement verified' >> README.md && git add README.md && git commit -m 'test: replacement verified' && curl -sS -f --connect-timeout 5 " + repoURL + "/info/refs?service=git-upload-pack",
			},
		},
		{
			"tool": "open_pull_request",
			"args": map[string]string{
				"title": "test: replacement verified",
				"body":  "Replacement sandbox verified with portal policy bound.",
			},
		},
	}
	scriptBytes, err := json.Marshal(scriptSteps)
	if err != nil {
		t.Fatalf("marshalling script: %v", err)
	}

	serverAddr, _, stateDir, serveStderr := startAOServe(t, subH, aoBin, repoURL,
		"OPENAI_MODEL=fake-script",
		"AO_FAKE_SCRIPT="+string(scriptBytes),
	)

	t.Log("Waiting for run to reach Working phase, recording initial sandbox UID, then deleting sandbox while sleep is in flight...")
	var initialUID string
	var replacementUID string
	finalRun, _ := pollRunPhase(t, aoBin, serverAddr, repoURL, "Succeeded", 180*time.Second, serveStderr, func(r *jsonRun) {
		if r.Phase != "Working" || r.SandboxName == "" {
			return
		}

		if initialUID == "" {
			subH.WaitForCondition("ao-sandboxes", "sandbox/"+r.SandboxName, "condition=Ready", 60*time.Second)

			uidOut, err := subH.Kubectl("get", "sandbox", r.SandboxName, "-n", "ao-sandboxes", "-o", "jsonpath={.metadata.uid}")
			if err != nil {
				t.Fatalf("failed to read sandbox UID before delete: %v", err)
			}
			initialUID = strings.TrimSpace(uidOut)
			if initialUID == "" {
				t.Fatalf("read empty initial sandbox UID for %s", r.SandboxName)
			}

			// Give the sleep tool call 2 seconds to enter flight inside the sandbox
			time.Sleep(2 * time.Second)

			t.Logf("Deleting sandbox %s (UID: %s) while tool call is in flight...", r.SandboxName, initialUID)
			out, delErr := subH.Kubectl("delete", "sandbox", r.SandboxName, "-n", "ao-sandboxes")
			if delErr != nil {
				t.Fatalf("failed to delete sandbox %s: %v, output: %s", r.SandboxName, delErr, out)
			}
			t.Logf("Deleted sandbox %s; waiting for on-demand replacement and run completion...", r.SandboxName)
			return
		}

		if replacementUID == "" {
			uidOut, err := subH.Kubectl("get", "sandbox", r.SandboxName, "-n", "ao-sandboxes", "-o", "jsonpath={.metadata.uid}")
			if err == nil {
				uid := strings.TrimSpace(uidOut)
				if uid != "" && uid != initialUID {
					replacementUID = uid
					t.Logf("Observed replacement sandbox %s with new UID: %s", r.SandboxName, replacementUID)
				}
			}
		}
	})

	if initialUID == "" {
		t.Fatal("expected initial sandbox UID to be recorded before delete")
	}
	if replacementUID == "" {
		t.Fatal("expected replacement sandbox with a new UID to be observed")
	}
	if replacementUID == initialUID {
		t.Fatalf("expected replacement sandbox to have a different UID than initial sandbox %s, got same: %s", initialUID, replacementUID)
	}

	if finalRun.Phase != "Succeeded" {
		t.Errorf("expected final run phase Succeeded, got %s (error: %s)", finalRun.Phase, finalRun.Error)
	}

	_ = stateDir
}
