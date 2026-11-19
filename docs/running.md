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

# Running a First Goal Against a Real Repository

This runbook guides maintainers from a deployed `ao` instance to a first pull request on a real repository.

## Prerequisites

Prepare a Kubernetes cluster with agent-sandbox installed.
Deploy the `ao` control plane using `deploy-to-kube` against your target cluster context:

```bash
IMAGE_PREFIX=<your-registry-prefix> ./dev/tasks/deploy-to-kube --only ao
```

The script deploys the `ao` Deployment and Service in the `ao-system` namespace.
It also configures the `ao-sandboxes` namespace for sandbox execution.

Provide model credentials to the control plane.
Create the `ao-model-keys` Secret in the `ao-system` namespace.
For Gemini, provide `GEMINI_API_KEY`:

```bash
kubectl create secret generic ao-model-keys \
  -n ao-system \
  --from-literal=GEMINI_API_KEY="<api-key>"
```

For OpenAI or compatible endpoints, provide `OPENAI_API_KEY` and an optional `OPENAI_BASE_URL`:

```bash
kubectl create secret generic ao-model-keys \
  -n ao-system \
  --from-literal=OPENAI_API_KEY="<api-key>" \
  --from-literal=OPENAI_BASE_URL="<endpoint-url>"
```

Alternatively, on Google Kubernetes Engine you can configure Vertex AI with Workload Identity to avoid cluster Secrets.

Provide forge credentials so the control plane can push branches and open pull requests.
Create a fork of the target repository on GitHub.
Generate a GitHub personal access token with repository read and write permissions.
Store the token in the `ao-forge` Secret in the `ao-system` namespace:

```bash
kubectl create secret generic ao-forge \
  -n ao-system \
  --from-literal=GITHUB_TOKEN="<token>"
```

The account that owns this token pushes branches and authors the pull requests.

Configure the repository list and model in the `ao-config` ConfigMap:

```bash
kubectl -n ao-system patch configmap ao-config --type=merge -p '{
  "data": {
    "AO_REPOS": "<repo-url>",
    "OPENAI_MODEL": "<model-name>"
  }
}'
kubectl -n ao-system rollout restart deploy/ao
```

Set `AO_REPOS` to the URL of your fork.
Set `OPENAI_MODEL` to the model name for the agent.

## Web UI Authentication

Production deployments require authentication for the web UI.
Configure GitHub sign-in through OAuth:

1. Create a GitHub OAuth App in GitHub under **Settings > Developer settings > OAuth Apps > New OAuth App**.
   Set the **Authorization callback URL** to `<public URL>/auth/github/callback` (for example, `https://ao.example.com/auth/github/callback`).
   Note the Client ID and generate a Client Secret.

2. Create the `ao-web-auth` Secret in the `ao-system` namespace, generating a session key of at least 32 bytes:

```bash
kubectl create secret generic ao-web-auth \
  -n ao-system \
  --from-literal=AO_GITHUB_CLIENT_ID="<client-id>" \
  --from-literal=AO_GITHUB_CLIENT_SECRET="<client-secret>" \
  --from-literal=AO_SESSION_KEY="$(openssl rand -hex 32)"
```

3. Set `AO_PUBLIC_URL` and the allowed GitHub users in the `ao-config` ConfigMap:

```bash
kubectl -n ao-system patch configmap ao-config --type=merge -p '{
  "data": {
    "AO_PUBLIC_URL": "https://<public-url>",
    "AO_GITHUB_ALLOWED_USERS": "<github-login1>,<github-login2>"
  }
}'
kubectl -n ao-system rollout restart deploy/ao
```

4. Sign in by navigating to `<public URL>` (or `http://localhost:8080` if using `kubectl port-forward`).
   The browser completes the GitHub OAuth authorization flow and signs in users listed in `AO_GITHUB_ALLOWED_USERS`.

## Writing a First Goal

A goal defines a task for the coding agent.
Create a goal file at `.goals/<name>.md` in your repository fork.
The file name without the `.md` extension becomes the goal name.
Add YAML front matter at the top of the file to configure execution settings.
Specify a `verify` command that validates candidate changes.
The verify command must exit 0 for a candidate to succeed.
Specify `budget.maxDuration` to cap the execution time of each run.
Goals use the `on-change` schedule by default.
The scheduler triggers a run whenever the goal file changes or the base commit advances.
Write instructions for the agent in the Markdown body below the front matter.

```markdown
---
verify: make test
budget:
  maxDuration: 30m
---

# Fix flaky cache tests

Investigate and fix failing or flaky tests in the cache package.
Run the test suite to verify your changes.
```

Commit this file and push it to the default branch of your fork.

Ensure the sandbox container image contains all tools required by your verify command.
The default toolbox image in `SANDBOX_IMAGE` provides Debian with git, curl, bash, and process tools.
It does not provide language compilers or build runtimes.
Use `sandbox-go` for Go repositories, and build your own for other toolchains.
Update `SANDBOX_IMAGE` in the `ao-config` ConfigMap with your image reference:

```bash
kubectl -n ao-system patch configmap ao-config --type=merge -p '{
  "data": {
    "SANDBOX_IMAGE": "<custom-sandbox-image>"
  }
}'
kubectl -n ao-system rollout restart deploy/ao
```

## What Happens Next and How to Watch It

The scheduler polls watched repositories once every minute.
When the scheduler finds a new goal or an advanced base commit, it creates a run.
The orchestrator launches a sandbox pod in the `ao-sandboxes` namespace.
Track runs in Kubernetes with the `HeroRun` custom resource:

```bash
kubectl get hr -n ao-sandboxes -w
```

The output displays the goal name, current phase, branch name, and elapsed time.
A run moves through the `Pending`, `Preparing`, `Working`, `Selecting`, and `OpeningPR` phases.

Inspect run details and agent transcripts in the web UI.
Port-forward the web service to your local workstation:

```bash
kubectl -n ao-system port-forward svc/ao 8080:8080
```

Open `http://localhost:8080/` in your browser.
Sign in with an authorized GitHub account. The web UI requires authentication via GitHub OAuth; only users listed in `AO_GITHUB_ALLOWED_USERS` can sign in (local kind clusters run with `--insecure-no-auth`).
The run page displays the session transcript, tool executions, and state events.

You will see several event kinds in the run event log:

- `verify_failed`: the candidate failed the verification command or the command timed out.
- `no_changes`: the agent made no git commits against the base commit.
- `refused`: the agent modified files under `.goals/`.
- `deadline_exceeded`: the run reached its `budget.maxDuration` or the `--max-run-duration` default and the orchestrator ended it.
- `sandbox_deleted`: the Sandbox disappeared or failed while the session was running.
- `sandbox_replaced`: the Sandbox disappeared during execution and a replacement Sandbox was created on the existing workspace claim.
- `branch_pushed`: the control plane pushed the candidate branch to the forge.
- `pull_request_opened`: the control plane opened a pull request on the repository.

## The Result

When a candidate passes verification, the control plane pushes commits to a branch named `ao/<goal>/<run-id>` on your fork.
The control plane then opens a pull request against the default branch of the fork.
The pull request originates from the account that owns the `GITHUB_TOKEN`.
The pull request description contains the harness-written summary and run metadata.
The scheduler does not start another run for that goal while the pull request remains open.
Review and merge or close the pull request before the scheduler runs that goal again.
Once a run finishes, you can use the **Run again** button on the run page to start a new run of the goal.

## Limits to Know About Today

Keep current system limits in mind when running goals.

- Without a Service Portal Controller configured, sandboxes have the cluster's default network egress and hold no credentials. When `--portal-controller-addr` is configured, sandboxes are intercepted by service-portals in per-node NRI mode and bound to an egress allowlist before execution begins. Configure allowed egress with `--sandbox-egress-hosts`, which accepts bare host names (for HTTPS on port 443) or `http://name` / `http://name:port` for cleartext HTTP (such as an in-cluster git server). Each entry must be a valid DNS name pattern; IP addresses are not accepted.
- The harness records `budget.maxCostUSD` in run records, but it does not enforce monetary limits yet. Enforce bounds by setting `--max-turns` on the server and `budget.maxDuration` in each goal.
- Sandboxes default to an `emptyDir` volume for the workspace, meaning the workspace is lost with the Sandbox. Pass `--workspace-storage-class` (for example, `objectfs-exclusive-local` from gke-labs/in-cluster-storage) and `--workspace-size` (default `10Gi`) to provision a persistent workspace claim for each run so the workspace survives sandbox deletion.
- State lives on the ReadWriteOnce PersistentVolumeClaim named `ao-state`. The volume is bound to one zone.
- The deployment runs exactly one replica. Shared storage such as ObjectFS (epic #33) is what allows more than one replica.
