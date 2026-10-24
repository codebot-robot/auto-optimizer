<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Development Guide

This guide covers setting up a development environment, running tests, deploying dependencies to Kubernetes, and managing upstream versions for `auto-optimizer`.

## Prerequisites

- **Go**: Version 1.27.3 or later (or run commands with `GOTOOLCHAIN=auto`).
- **Docker**: A running Docker daemon accessible locally or via `DOCKER_HOST`.
- **Git**: For version control and repository metadata.

Pinned versions of `kind` and `kubectl` are installed automatically into `.build/bin/` by the development tasks and do not need to be installed on your system `PATH`.

## Core Development Tasks

The project uses two primary development scripts located in `dev/tasks/`:

### 1. `dev/tasks/deploy-to-kube`

Installs all cluster infrastructure and prerequisites needed by `auto-optimizer` into the **current kubeconfig context**.

```bash
./dev/tasks/deploy-to-kube
```

#### What It Installs

`deploy-to-kube` installs, in order, and waits for each to be ready:

1. **Namespaces**:
   - `ao-system`: Namespace for auto-optimizer control plane components.
   - `ao-sandboxes`: Namespace where agent sandboxes run.

2. **agent-sandbox**:
   - Upstream manifests (`sandbox.yaml` and `extensions.yaml`) from `kubernetes-sigs/agent-sandbox` pinned to release `v1.0.8`.
   - Applied via `kubectl apply --server-side`.
   - Waits for the `sandboxes.agents.x-k8s.io` CRD to be established and the `agent-sandbox-controller` deployment in `agent-sandbox-system` to roll out.

3. **service-portals**:
   - Ensures `cert-manager` (pinned to `v1.19.2`) is installed and `cert-manager-webhook` is ready, which is required for `service-portals` to generate CA secrets dynamically.
   - Cluster-wide manifests (`k8s/manifests.yaml`) from `gke-labs/service-portals` pinned to commit `686b34d8aa74546af1039119bbfa6e05ead15f53`.
   - Note: `gke-labs/service-portals` does not publish pre-built container images to a public registry. To deploy images to a custom registry, run `IMAGE_PREFIX=<registry> ap deploy //...` from that repository. Sidecar images (`init-service-portals`, `all-in-one-portal`) are referenced by sandboxes when portals are needed.

4. **in-cluster-storage (ObjectFS)**:
   - Deploys `k8s/wal.yaml` and `k8s/objectfs.yaml` from `gke-labs/in-cluster-storage` pinned to commit `923578df519c02cd0da1b7455d8a1e0278a103c4`.
   - On local `kind` clusters, deploys an in-cluster `fakes3` S3-compatible service in `kube-objectfs-system` and configures the `objectfs-config` ConfigMap `backend` URL to point to it.
   - Builds container images from source and loads them into kind if not already present.
   - Waits for the `wal-buffer` StatefulSet, `objectfs-controller` StatefulSet, and `objectfs-node-daemon` DaemonSet to roll out successfully.
   - Waits for the `objectfs-exclusive-local` StorageClass to be established.

#### Idempotence

`deploy-to-kube` is idempotent. Running it a second time against an already-configured cluster will make no changes, detect existing resources and rollouts, and exit `0` within seconds.

#### Pointing at a Real Cluster (e.g. GKE)

To point `deploy-to-kube` at a real cluster rather than `kind`:

1. Set your `kubectl` context to your target cluster:
   ```bash
   kubectl config use-context <my-cluster-context>
   ```

2. Specify an external object storage backend URL using the `OBJECTFS_BACKEND` environment variable. For example, for Google Cloud Storage:
   ```bash
   export OBJECTFS_BACKEND="gs://my-bucket/prefix"
   ```
   Or for Amazon S3:
   ```bash
   export OBJECTFS_BACKEND="s3://my-bucket/prefix"
   ```

3. Run the script:
   ```bash
   ./dev/tasks/deploy-to-kube
   ```

When `OBJECTFS_BACKEND` is set, `deploy-to-kube` skips deploying in-cluster `fakes3` and configures ObjectFS to use the specified cloud storage bucket.

#### Bumping Pinned Versions

All upstream dependencies are pinned in variables at the top of `dev/tasks/deploy-to-kube`:

```bash
AGENT_SANDBOX_VERSION="v1.0.8"
SERVICE_PORTALS_COMMIT="686b34d8aa74546af1039119bbfa6e05ead15f53"
CERT_MANAGER_VERSION="v1.19.2"
IN_CLUSTER_STORAGE_COMMIT="923578df519c02cd0da1b7455d8a1e0278a103c4"
```

To bump a version:
- **`AGENT_SANDBOX_VERSION`**: Check releases at [kubernetes-sigs/agent-sandbox/releases](https://github.com/kubernetes-sigs/agent-sandbox/releases) and update the version tag.
- **`SERVICE_PORTALS_COMMIT`**: Check latest commits at [gke-labs/service-portals/commits/main](https://github.com/gke-labs/service-portals/commits/main) and update the commit SHA.
- **`CERT_MANAGER_VERSION`**: Check releases at [cert-manager/cert-manager/releases](https://github.com/cert-manager/cert-manager/releases) and update the version tag.
- **`IN_CLUSTER_STORAGE_COMMIT`**: Check latest commits at [gke-labs/in-cluster-storage/commits/main](https://github.com/gke-labs/in-cluster-storage/commits/main) and update the commit SHA.

After updating versions, run `./dev/tasks/test-e2e` to verify compatibility.

---

### 2. `dev/tasks/test-e2e`

Runs end-to-end tests against a local `kind` cluster. `dev/tasks/test-e2e` is a thin wrapper that prepares the cluster and invokes Go-based E2E tests in `tests/e2e/`.

```bash
./dev/tasks/test-e2e
```

#### What It Does

1. **Tool Setup**: Ensures pinned `kind` (v0.31.0) and `kubectl` (v1.35.0) are installed into `.build/bin/` and placed at the beginning of `PATH`.
2. **Cluster Creation**: Creates a `kind` cluster named `auto-optimizer` (or reuses an existing one if already present).
3. **Deployment**: Executes `dev/tasks/deploy-to-kube` against the cluster.
4. **Go E2E Tests**: Executes `RUN_E2E=1 go test -v -count=1 -timeout 20m ./tests/e2e/...`:
   - Tests are implemented in `tests/e2e/e2e_test.go` using the `Harness` in `tests/e2e/harness.go`.
   - The test suite skips automatically when `RUN_E2E` is not set, ensuring `ap test` and `go test ./...` pass without a cluster.
   - The harness wraps `kubectl` against the kind cluster configured by `KIND_CLUSTER_NAME` (default `auto-optimizer`), providing helpers for manifest application, condition waiting, port-forwarding, log inspection, and automatic resource cleanup through `t.Cleanup`.
   - Subtests cover:
     - `SandboxHealth`: Creates a minimal `Sandbox` (`agents.x-k8s.io/v1beta1`) in `ao-sandboxes` running upstream `sandboxd`, waits for `Ready`, port-forwards to port 8080, queries `/v1/health` for `{"status":"ok"}`, and deletes the sandbox.
     - `ObjectFSPVCWrite`: Creates a `PersistentVolumeClaim` with `storageClassName: objectfs-exclusive-local` and `ReadWriteOncePod` access mode. Launches a test Pod writing to the mounted volume, verifies successful exit and output, and deletes test resources.
     - `ServicePortalsImages`: Skips with an informative message until upstream publishes images.
5. **Teardown**: Deletes the `auto-optimizer` kind cluster upon completion (or on error, unless `--keep` is passed).

#### Options and Flags

- `--keep` or `-k` (or environment variable `KEEP_CLUSTER=true`): Keeps the kind cluster alive after tests complete for debugging.
  ```bash
  ./dev/tasks/test-e2e --keep
  ```
- `--delete` or `-d`: Deletes any existing `auto-optimizer` cluster before running tests.
  ```bash
  ./dev/tasks/test-e2e --delete
  ```
- Any additional flags are forwarded directly to `go test`, allowing selection of specific subtests via `-run`:
  ```bash
  ./dev/tasks/test-e2e --keep -run TestSmoke/SandboxHealth
  ```

---

## Presubmits and CI

The repository uses `ap` from `gke-labs-infra` for code generation, formatting, and CI presubmits.

### Running with `ap`

```bash
# Regenerate presubmits, workflows, and format code
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest generate //...

# Run E2E tests
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest e2e

# Run linter
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest lint

# Run unit tests
GOTOOLCHAIN=auto go run github.com/gke-labs/gke-labs-infra/ap@latest test
```

### GitHub Actions Presubmits

Pull requests run the presubmits defined in `.github/workflows/ci-presubmits.yaml`, which are generated by `ap generate`:
- `ap-verify-generate`: Verifies that generated files are in sync with `ap generate`.
- `ap-lint`: Static analysis and formatting check.
- `ap-test`: Unit tests.
- `ap-e2e`: Runs `dev/tasks/test-e2e` against a kind cluster in CI.
