# auto-optimizer design

This document is the source of truth for the auto-optimizer architecture, the
boundaries between its components, and the reasons behind its design decisions.
Keep this document current when architectural boundaries change, and record
changes of direction in the Decisions table with a date.

## What it is

You put goals in a `.goals/` directory in your repository and push to GitHub.
auto-optimizer detects the changes, runs coding agents in Kubernetes sandboxes
against a clone of your repository, and opens pull requests that make progress
on those goals. Maintainers review the pull requests, give feedback, and merge.

The canonical goal is a performance benchmark. It specifies a command that
prints a number, a direction, and an improvement threshold. Goals can also cover
broader objectives such as code quality bars, feature lists, or migrations. For
a benchmark goal, the harness re-measures the candidate and decides whether to
open a pull request. For a softer goal, the agent submits the pull request as a
proposal and the maintainer judges the result.

## Terms

- A **goal** is a Markdown file under `.goals/` that describes work for an agent
  and carries optional machine-readable settings.
- A **run** is one attempt to make progress on one goal from a single base
  commit. A run ends in a pull request or a recorded reason why none opened.
- A **sandbox** is a `Sandbox` resource from `kubernetes-sigs/agent-sandbox`
  that isolates command and filesystem execution for a run.
- The **workspace** is the sandbox's `/workspace` volume. It holds the
  repository clone and the agent's working files.
- A **session** is one LLM conversation with tools. A run starts a top-level
  session and can spawn child sessions that share its sandbox and workspace.
- The **control plane** is the `ao serve` Deployment. It watches repositories,
  reconciles runs, drives sessions, and serves the web UI and session API.
- A **forge** abstracts repository and pull request operations across backends
  such as GitHub or a plain git remote.
- The **portal** is `gke-labs/service-portals` running in per-node NRI mode. It
  intercepts sandbox network traffic so sandboxes reach external services
  without holding credentials.
- A **workflow** is a resumable multi-step process that persists its state
  between steps so it survives crashes. See
  [Workflows architecture](architecture/workflows.md).

## Decisions

| Date | Topic | Decision |
|---|---|---|
| 2026-11-22 | HeroRun as workflow state | `HeroRun` in `kube-apiserver` is the authoritative source of truth for a run's workflow state. The web UI and CLI read `HeroRun` through the Kubernetes client. Transcripts, tool outputs, and event logs stay on the state volume to respect etcd size limits and avoid write churn. Disk exports exist only for human inspection. An aggregated API server can replace etcd storage later if scale requires it. See [Workflows architecture](architecture/workflows.md). |
| 2026-11-17 | Web UI authentication | Users sign in with GitHub through the OAuth authorization-code flow. The server keeps a signed `Secure` session cookie and stores no GitHub token. An allowlist of GitHub logins controls access. The gRPC session API has no authentication and must stay on a trusted network. |
| 2026-11-09 | Sandbox lifetime | Each run sandbox receives one expiration time at creation equal to the run deadline plus a grace period. Nothing extends that expiration afterwards, and the orchestrator deletes the sandbox when the run ends. |
| 2026-10-23 | Sandbox transport | The control plane drives sandboxes through agent-sandbox's `sandboxd` over gRPC and HTTP. We write no custom in-sandbox server and do not use `pods/exec`. Because we also maintain agent-sandbox, we fix gaps upstream instead of working around them. |
| 2026-10-23 | Agent loop | The LLM loop runs in the control plane, and every tool call executes in the sandbox. Keeping the loop in the control plane keeps model credentials out of the sandbox and avoids an in-sandbox worker entrypoint. A top-level session can spawn child sessions, and future inner sessions will support multiple agent personalities. |
| 2026-10-23 | Goal format | Each goal lives in `.goals/<name>.md`. The agent reads the Markdown body verbatim, while optional YAML front matter holds machine-readable settings for metrics, verification, budgets, and schedules. |
| 2026-10-23 | GitHub integration | Production uses a GitHub App, while local development must not require one. A forge abstraction supports a pure-git remote poller, a GitHub REST client backed by a personal access token or an App, and an in-memory fake for tests. |
| 2026-10-23 | Credentials and egress | Sandbox pods use `gke-labs/service-portals` in per-node NRI mode instead of a sidecar. The orchestrator binds an egress policy through the Service Portal Controller before commands run. The node agent enforces allowlists transparently, terminates TLS, injects per-host credentials, and logs usage so sandboxes hold no real tokens. |
| 2026-10-23 | Filesystem storage | ObjectFS from `gke-labs/in-cluster-storage` backs workspace volumes so a workspace survives sandbox recreation and a session can resume. Control-plane bulk state lives as files under one state directory, backed by an ObjectFS volume in production. |
| 2026-10-23 | Control plane | The single `ao` Go binary provides both the CLI and the `ao serve` Deployment. The web UI uses server-rendered HTML templates with no JavaScript framework. |
| 2026-10-23 | Metric trust | The agent proposes changes, and the harness decides whether to open a pull request. For metric goals, the orchestrator re-measures the candidate in a clean sandbox before opening a pull request. |
| 2026-10-15 | Billing and budgets | The platform holds LLM keys and injects them from the control plane or portal so they never enter a sandbox. The platform meters usage per run and per repository with hard budget caps. |
| 2026-10-15 | Deployment | Developers run on kind locally, and production runs on GKE with agent-sandbox installed. Sandboxes start on `runc` and can adopt gVisor through a `RuntimeClass`. |

## Architecture

```
            GitHub (github.com or any git remote)
              ^  webhooks or ref polling
              |  branch pushes and pull requests
              v
 +------------------------------------------------------------------+
 |  ao serve  (control plane Deployment, namespace ao-system)       |
 |                                                                  |
 |   syncer -> goal discovery -> run scheduler -> workflow runner   |
 |                                                      |           |
 |   state volume (bulk data)         session manager <-+           |
 |   web UI & gRPC session API        (LLM loop, tools)             |
 +-----------------|------------------------------------|-----------+
                   | HeroRun & Sandbox CRs              | sandboxd gRPC/HTTP
                   v                                    | (ed25519 signed tokens)
          kube-apiserver                                v
 +------------------------------------------------------------------+
 |  Sandbox (namespace ao-sandboxes)                                |
 |  per-node NRI egress interception via Service Portal Controller  |
 |                                                                  |
 |   init container injects sandboxd                                |
 |   worker container runs sandboxd over /workspace (ObjectFS PVC)  |
 +------------------------------------------------------------------+
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| `cmd/ao` | Provides the single binary for the `ao serve` control plane and CLI commands for sessions, repositories, goals, and runs. |
| `api/herorun`, `api/session` | Define the `HeroRun` Kubernetes custom resource API and the gRPC session service contract. |
| `pkg/forge` | Abstracts repository refs, shallow tree fetches, branch pushes, and pull requests across GitHub, plain git remotes, and an in-memory fake. |
| `pkg/syncer` | Polls registered repositories, discovers goals under `.goals/`, and writes repository and goal metadata. |
| `pkg/goals` | Parses and validates `.goals/*.md` files and hashes their content to detect changes. |
| `pkg/run` | Schedules runs, reconciles `HeroRun` custom resources, drives workflow phases, and enforces budgets and candidate verification. |
| `pkg/session` | Executes the control-plane agentic loop, dispatches tool calls to `sandboxd`, provisions sandboxes on demand, and persists transcripts. |
| `pkg/sessionapi` | Serves the gRPC session API, serializes active turns per session, and streams turn events to clients. |
| `pkg/llm` | Wraps OpenAI-compatible chat endpoints with tool calling and provides a scripted fake for tests. |
| `pkg/tools`, `pkg/toolsets` | Define the tool boundary, output-size limits, sandbox filesystem and command tools, and control-plane run coordination tools. |
| `pkg/portal` | Binds per-pod egress policies through the Service Portal Controller before commands run in a sandbox. |
| `pkg/state` | Manages the state directory layout, atomic JSON persistence, and append-only event logs. |
| `pkg/web` | Renders the HTML web UI and enforces GitHub OAuth sign-in. |

## Goals

Repositories declare work as Markdown files under `.goals/` on the default
branch. The filename determines the goal name, which appears in branch names,
pull request titles, and the web UI. Keeping goals in the repository versions
them alongside the code they govern.

Front matter distinguishes two kinds of goals:

- **Metric goals** define a benchmark command, direction, and improvement
  threshold. The harness measures the baseline and the candidate in clean
  sandboxes and opens a pull request only when verification succeeds and the
  candidate beats the threshold. The harness writes the before and after
  measurements into the pull request body.
- **Judged goals** omit the metric section. The agent works until it finishes
  or requests a pull request. The harness runs any configured verification
  command, confirms the diff is non-empty, and opens the pull request as a
  proposal for maintainers to evaluate.

The metric protocol avoids a plugin API so any repository can adopt it with a
shell script. The harness runs the metric command from the repository root and
parses the last non-empty stdout line as a bare number, a number with a unit,
or JSON. Any non-zero exit fails measurement. The run retains full command
output in logs so scripts can print diagnostic details freely.

The orchestrator freezes the goal definition at the base commit before a session
starts. If a candidate edits `.goals/` or the metric script, the harness rejects
the candidate so an agent cannot weaken its own target.

## Forge and repository watching

The forge layer isolates GitHub-specific behavior behind a narrow boundary so
local development and tests run without a GitHub App. Three backends implement
this boundary:

- **Plain git remote** lists refs and fetches shallow trees over HTTPS without
  API quotas or credentials. It does not open pull requests.
- **GitHub REST** reuses git remote operations for ref discovery and adds pull
  request and push support using a personal access token or a GitHub App
  installation token. It uses conditional requests and respects rate limits.
- **Fake** runs in memory for hermetic unit tests.

Repository watching relies on cheap ref polling with jitter to detect moved
branches without exhausting API quota, paired with periodic full reconciliation.
GitHub App webhooks feed the same consumer as an accelerator rather than
creating a second code path. When the default branch advances, the control plane
re-reads `.goals/` and schedules eligible runs. When an auto-optimizer pull
request closes or merges, the control plane records the outcome and releases
associated resources.

## Sessions and tools

A session runs its LLM conversation loop in the control plane and executes every
tool call inside the run's sandbox over `sandboxd`. Separating reasoning from
execution keeps model credentials out of the sandbox. It also lets the control
plane bound tool output returned to the model while saving full outputs on the
state volume.

The session manager provisions sandboxes on demand and attaches them to a
persistent workspace volume. If a sandbox disappears or fails while a session
works, the run does not fail. The next tool call creates a replacement sandbox
attached to the same workspace, binds its egress policy, and retries cleanly.

Tools split across the sandbox boundary:

- **Sandbox tools** inspect files, edit code, and run shell commands inside the
  worker container.
- **Control-plane tools** coordinate run-level actions such as verifying or
  measuring candidates, requesting a pull request, ending a session, and
  managing child sessions.

A top-level session can delegate work to child sessions up to a bounded tree
depth. Child sessions share the parent's sandbox, workspace, deadline, and turn
budget. They coordinate through workspace files and control-plane messages, and
they can pause without spending model turns while waiting for messages or child
completion. On a control-plane restart, the top-level session resumes while
in-flight child sessions remain stopped.

The control plane also exposes a gRPC session API for CLI and programmatic
clients. It runs one active turn per session at a time, creates sandboxes lazily
on the first tool call, and stays on a trusted network without authentication
until App-backed authorization lands.

In the future, inner sessions will support pluggable agent personalities. Native
personalities will reuse the control-plane loop with different models, prompts,
and toolsets. CLI personalities will run external agent CLIs inside the sandbox
behind the portal so auto-optimizer can compare agents under the same goal and
budget.

## Sandboxes

Sandboxes isolate untrusted repository code and agent commands in unprivileged
Kubernetes pods. Each pod runs as a non-root user with dropped Linux
capabilities, no mounted service account token, and optional gVisor isolation.

An init container injects `sandboxd` into a tool-rich worker container so
commands execute directly inside the worker's root filesystem without a custom
in-sandbox server or `pods/exec`. The control plane connects directly to
`sandboxd` in-cluster because `sandbox-router` does not proxy gRPC. Each
sandbox receives a fixed expiration time equal to the run deadline plus a grace
period and deletes itself on expiry as a safety net if the control plane dies.

When an operator configures persistent workspaces, the orchestrator provisions
an ObjectFS `ReadWriteOncePod` volume owned by the run's `HeroRun` resource.
The workspace survives sandbox recreation, and Kubernetes garbage collection
removes it when the `HeroRun` disappears.

Sandbox pods opt into `service-portals` per-node NRI interception rather than
running a sidecar. The node agent denies all egress by default until the
orchestrator binds an allowlist policy through the Service Portal Controller
after the sandbox becomes ready. The portal injects scoped credentials for git
pushes and external API calls outside the pod, so agent processes never see
tokens or need custom base URLs.

## Runs and workflows

A `controller-runtime` reconciler executes each run as a resumable workflow. We
chose `controller-runtime` for its workqueue de-duplication, backoff, typed
watches, and bounded worker concurrency.

A run progresses through `Pending`, `Preparing`, `Working`, `Selecting`, and
`OpeningPR` before reaching a terminal outcome of `Succeeded`, `NoImprovement`,
`VerifyFailed`, or `Failed`. During selection, the control plane verifies the
candidate, rejects empty diffs or edits under `.goals/`, pushes the
deterministic branch `auto-optimizer/<goal>/<run-id>` with a lease, and deletes
the sandbox. Because this branch name is deterministic, retrying pull request
creation reuses any existing open pull request instead of creating duplicates.

`HeroRun` custom resources in `kube-apiserver` hold the authoritative workflow
state for every run. Representing runs as Kubernetes resources gives
auto-optimizer standard owner references for cascading cleanup of sandboxes and
workspace volumes, native cluster inspection through `kubectl`, and optimistic
concurrency.

See [Workflows architecture](architecture/workflows.md) for the workflow runner
model, crash-recovery idempotency, long-running operation tracking, retention
rules, and storage scaling options.

## State and storage boundaries

auto-optimizer splits state across two stores based on access pattern and size:

- **`kube-apiserver`** stores `HeroRun` custom resources as the single source of
  truth for run workflow state. The controller, CLI, and web UI read and watch
  `HeroRun` directly through the Kubernetes client.
- **The state volume** uses ObjectFS in production and a local directory in
  development. It stores bulk data and history, including repository and goal
  metadata, session transcripts, unabridged tool outputs, append-only event
  logs, optional human-readable run exports, and rebuildable caches.

Keeping transcripts, tool outputs, and streaming event logs on the state volume
prevents runs from hitting etcd's 1.5 MB object limit and avoids etcd write
churn during active turns.

Every file on the state volume follows a single-writer rule so concurrent
components do not race on partial updates. JSON records use atomic writes, and
logs append one JSON object per line. The control plane can rebuild caches from
scratch if an operator deletes them.

## Security model

- **No credentials in sandboxes.** Sandboxes hold no GitHub tokens, model keys,
  or cluster credentials. Outbound requests pass through the portal, which
  enforces per-pod host allowlists and injects credentials scoped to one
  repository and the platform's LLM projects.
- **Network isolation.** Sandboxes cannot reach the control plane, other
  sandboxes, cluster metadata servers, or unlisted external hosts. Per-node NRI
  interception enforces deny-by-default egress. We omit per-sandbox ingress
  `NetworkPolicy` resources because they do not scale with sandbox churn.
- **Asymmetric sandbox authentication.** The control plane signs a short-lived
  per-call token for every `sandboxd` request using an ed25519 private key kept
  only in control-plane memory and on its persistent state volume. Each sandbox
  receives only the public key and an audience naming that specific sandbox, so
  the control plane creates no Kubernetes Secrets and holds no Secret RBAC. A
  captured token works only for that single sandbox during its short validity
  window. It cannot reach other sandboxes, mint new tokens, or access the
  Kubernetes API. We plan to add TLS encryption between the control plane and
  `sandboxd`.
- **Prompt injection containment.** Malicious repository code can waste a run's
  budget or propose a bad pull request, but it cannot exfiltrate tokens, alter
  `.goals/` to cheat verification, or touch other repositories. The
  orchestrator enforces budgets and verification gates outside the model prompt.
- **Control-plane surfaces.** The web UI authenticates users through GitHub
  OAuth with signed `Secure` cookies and a login allowlist, storing no GitHub
  tokens. The gRPC session API has no authentication and must stay on a trusted
  network. Webhook handlers hold no credentials and only enqueue events.

## Metering and budgets

The control plane records token usage from model API responses for native
sessions. For inner CLI agents inside sandboxes, the portal parses model traffic
and logs usage by pod identity so the control plane can attribute cost to the
run. Each run tracks its own cost, and repositories roll costs up monthly. The
scheduler and session manager enforce hard caps rather than trusting prompts.

## Web UI

The web UI uses server-rendered HTML templates with no JavaScript framework and
refreshes live views with standard HTML meta refresh tags. Server rendering keeps
the binary self-contained and avoids a frontend build pipeline. The UI reads run
state from `HeroRun` resources through the Kubernetes client and reads
transcripts and event logs from the state volume.

## Upstream dependencies

auto-optimizer builds on three sibling projects and fixes gaps upstream instead
of carrying local workarounds:

- **`kubernetes-sigs/agent-sandbox`** supplies the `Sandbox` CRD, controller,
  and `sandboxd` runtime daemon.
- **`gke-labs/service-portals`** provides per-node NRI egress enforcement,
  credential injection, and usage logging.
- **`gke-labs/in-cluster-storage`** provides ObjectFS volumes for persistent
  workspaces and control-plane bulk state.

## Further reading

- [Workflows architecture](architecture/workflows.md) for the `HeroRun` workflow
  model, crash recovery, and retention.
- [Running a first goal](running.md) for deploying and operating auto-optimizer
  against a real repository.
- [Development guide](development.md) for local kind setup, upstream version
  pins, and end-to-end tests.
- [Agent sessions](sessions.md) for running and inspecting sessions locally.
