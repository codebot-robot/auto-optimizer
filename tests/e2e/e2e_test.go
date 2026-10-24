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
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

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
          image: us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/sandboxd:latest-main
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /v1/health
              port: 8080
`, sandboxName, shutdownTime)

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
		t.Skip("Skipping service-portals image check: upstream gke-labs/service-portals publishes no container images yet (see #5)")
	})
}
