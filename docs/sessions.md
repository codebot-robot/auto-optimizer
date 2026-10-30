# Agent Sessions

`auto-optimizer` runs coding agents in Kubernetes [Agent Sandboxes](https://github.com/kubernetes-sigs/agent-sandbox).
The core session loop, toolsets, and session persistence are housed under `pkg/session`, `pkg/llm`, `pkg/tools`, and `pkg/toolsets`.

## Origin

The session implementation was forked from `examples/sandboxed-tools` in the [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) repository at commit `f83f2d87c1aeeb90407d8cfe9a0c1fad74044304`. Both repositories are licensed under Apache 2.0. Upstream license headers (`The Kubernetes Authors`) are preserved on copied files with their origin attributed in each package's doc comment.

## Changes from Upstream

1. **Package Layout**:
   - `pkg/agent` and `pkg/sessions` are merged into a unified `pkg/session` package.
   - `cmd/sandboxed-tools-cli` is implemented as the `ao session run` subcommand in `cmd/ao`.
   - Import paths are updated from `sigs.k8s.io/agent-sandbox/examples/sandboxed-tools/...` to `github.com/gke-labs/auto-optimizer/...`.
2. **State Directory**:
   - Session metadata, message history, and filesystem snapshots are stored in `~/.local/auto-optimizer` by default (configurable via the `--state-dir` flag), instead of `~/.local/sandboxed-tools`.
3. **Default Namespace**:
   - Sandboxes are created in the `ao-sandboxes` namespace by default (configurable via `--namespace` or the `SANDBOX_NAMESPACE` environment variable), matching auto-optimizer conventions.
4. **One-Shot Execution and Auto-Approval**:
   - Added `--prompt` for non-interactive single-turn execution, enabling scripting and automated testing.
   - Added `--yolo` flag to auto-approve tool calls without user confirmation prompts. Interactive prompt approval remains the default when `--yolo` is not passed.
5. **Kubeconfig Precedence**:
   - `GetRESTConfig` prefers local kubeconfig (or `KUBECONFIG`) if present and valid, falling back to in-cluster configuration. This allows running `ao` from dev containers or laptops against local kind clusters even when running within a container.
6. **Sandbox Lifecycle**:
   - Sandboxes persist after process exit and expire automatically via `shutdownPolicy: Delete` and inactivity timeout `shutdownTime`, allowing subsequent CLI commands to reuse active sandboxes without recreate latency.
7. **Sandbox Transport (`sandboxd`) and Asymmetric Authentication**:
   - The transport uses `sandboxd` gRPC `ProcessService` (port 9090) instead of SPDY `kubectl exec` (`pods/exec`).
   - Sandbox pods deploy using Topology B: an `install-sandboxd` init container copies the `sandboxd` binary into a shared volume, and the worker container runs it with `tini` as PID 1 and a readiness probe on `/v1/health` (port 8080).
   - Asymmetric authentication (`ed25519`):
     - **Signing key**: The control plane (both `ao serve` and the `ao` CLI) loads or generates a 64-byte raw ed25519 signing key at `<state-dir>/sandbox-signing-key` with `0600` permissions. The private key remains exclusively in control plane memory and on its local state disk; it is never transmitted over the network or stored in Kubernetes Secret resources.
     - **Public key and audience injection**: `CreateSandbox` passes only the base64-encoded public key via `--auth-public-key` and the target audience via `--auth-audience=<namespace>/<sandbox-name>` to `sandboxd` in the pod template args. No Kubernetes Secret is created, and the control plane's ServiceAccount has no RBAC permissions for Secrets in its namespace.
     - **Per-call signed tokens**: For every gRPC call to `sandboxd`, the client interceptor mints a fresh token signed by the control plane's private key in `v1.<base64url(payload)>.<base64url(signature)>` format. The payload carries the target `aud` (`<namespace>/<sandbox-name>`), issuance time `iat`, and expiration `exp` with a 5-minute token lifetime.
     - **Captured token scope**:
       - *What it can do*: Within its 5-minute lifetime, a captured token can only invoke gRPC operations on the specific sandbox instance matching its audience claim (`<namespace>/<sandbox-name>`).
       - *What it cannot do*: It cannot authenticate against any other sandbox (mismatched audience is rejected by sandboxd), cannot be used after the 5-minute validity window expires, cannot be used to forge new tokens (which requires the private signing key), and cannot access the Kubernetes API or any control plane Secrets.
   - Connections use direct Pod IP connectivity in-cluster, with automatic fallback to SPDY port-forwarding when executed outside the cluster (e.g. from developer laptops).
   - Upstream status: The asymmetric authentication support in `sandboxd` (`--auth-public-key`, `--auth-audience`, `--auth-max-token-ttl`) currently lives on the fork branch `justinsb/agent-sandbox:sandboxd-asymmetric-auth` (at commit `76156ce`) until it is accepted and released upstream in `kubernetes-sigs/agent-sandbox`.

## Relationship to Upstream

`pkg/session`, `pkg/llm`, `pkg/tools`, and `pkg/toolsets` were forked from `examples/sandboxed-tools` in [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) at commit `f83f2d87c1aeeb90407d8cfe9a0c1fad74044304`. They are maintained in this repository, and changes are not sent back to upstream.

## Running Locally Against Kind

### 1. Prepare Kind Cluster

Ensure the kind cluster is created and dependencies are deployed:

```bash
./dev/tasks/test-e2e
```

### 2. Build `ao`

Build the `ao` binary:

```bash
go build -o bin/ao ./cmd/ao
```

### 3. Run with the Fake LLM (No API Key Required)

You can run non-interactive one-shot commands using the built-in `fake-eliza` model:

```bash
OPENAI_MODEL=fake-eliza ./bin/ao session run --session my-session --prompt "run: uname -a" --yolo
```

### 4. Run Interactively with a Real LLM

Configure your API key and start an interactive session:

```bash
export GEMINI_API_KEY="your-api-key"
./bin/ao session run --session my-session
```

### 5. Using the `geminicli` Toolset

To use the gemini-cli tool surface (`read_file`, `write_file`, `replace`, `grep_search`, `glob`, `list_directory`, `run_shell_command`):

```bash
./bin/ao session run --session my-session --toolset geminicli
```

## Running the Session API Server (`ao serve`)

`ao serve` starts the gRPC session service, executing turns and managing sandboxes through a server interface rather than an in-process REPL:

```bash
./bin/ao serve --listen 127.0.0.1:8081
```

The listen address defaults to `127.0.0.1:8081`, configurable via `--listen` or the `AO_LISTEN` environment variable.

> **Security Note**: The server provides no authentication in this milestone and listens on `127.0.0.1:8081` by default. It must not be exposed to untrusted networks.

### Admin CLI Subcommands

With `ao serve` running, use the `ao session` client subcommands (all take `--server`, defaulting to `127.0.0.1:8081` or the `AO_SERVER` environment variable):

1. **List sessions**:
   ```bash
   ./bin/ao session list
   ```

2. **Send a message and stream events**:
   ```bash
   ./bin/ao session send demo "what is the capital of France?"
   ```
   If the session does not exist, `ao session send` creates it automatically. Tool progress and errors stream as they arrive, and the final assistant reply is printed last.

   *Note on System Prompt Visibility*: The first `SendMessage` on a new session initializes the system prompt and streams it to the client as a `MessageAppended` event. This is expected behavior for an admin CLI inspecting the session history, but when authentication and non-admin clients are introduced in later milestones, streaming of the internal system prompt may be restricted or filtered based on client authorization.

3. **Inspect session messages**:
   ```bash
   ./bin/ao session get demo
   ```
   Prints the session messages in order, one per line, with their role. Assistant tool calls are rendered as `call: name(args)` and tool outputs as `tool: <output>`. Pass `--json` to output structured JSON for scripts.

### Web UI

`ao serve` also serves an embedded server-rendered web UI on `--http` (`127.0.0.1:8080` by default, configurable via `--http` or the `AO_HTTP` environment variable):

```bash
./bin/ao serve --listen 127.0.0.1:8081 --http 127.0.0.1:8080
```

Open `http://127.0.0.1:8080/` in a browser to:
- View all active sessions and whether a turn is in flight.
- Create new sessions using the form on `/`.
- View transcripts with tool calls and expandable tool results at `/sessions/{name}`.
- Send messages directly to sessions from the browser form; pages auto-refresh while turns execute.

Cross-site form submissions are protected via a `SameSite=Strict` cookie.

### Real-Model Path with Gemini

To run against a real model using Google Gemini:

1. Start the server with `GEMINI_API_KEY` set (and without an `OPENAI_MODEL` override):
   ```bash
   export GEMINI_API_KEY="your-api-key"
   ./bin/ao serve
   ```

2. Send a prompt to the session:
   ```bash
   ./bin/ao session send demo "what is the capital of France?"
   ```
   The model prints an answer containing `Paris`. CI cannot run this path because it requires external credentials and network access.
