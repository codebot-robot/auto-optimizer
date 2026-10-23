# auto-optimizer design

Status: v1 design, 2026-10-23. This document is the source of truth for
architecture. It replaces the design of a 2026-10-15 spike that is not part of
this repository; the ideas worth keeping from it are summarised in
[Ideas from the spike](#ideas-from-the-spike). Keep this document current when
the code changes; record changes of direction in the Decisions table with a
date.

## What it is

You put goals in a `.goals/` folder in your repository and push to GitHub.
auto-optimizer notices, runs coding agents in Kubernetes sandboxes against a
clone of your repository, and sends pull requests that make progress on the
goals. You review, give feedback, and merge.

The canonical goal is a performance benchmark: a command that prints a number,
a direction, and a threshold. Goals can also be less concrete: a code quality
bar, a feature list, a migration. For concrete goals the harness decides
whether a PR is opened by re-measuring; for soft goals the PR is the proposal
and the maintainer is the judge.

## Decisions

| Date | Topic | Decision |
|---|---|---|
| 2026-10-23 | Sandbox transport | The control plane drives sandboxes through agent-sandbox's `sandboxd` (gRPC `ProcessService` on 9090, REST filesystem on 8080). No custom in-sandbox server, no `pods/exec`. We also maintain agent-sandbox; gaps found here are fixed upstream, not worked around. |
| 2026-10-23 | Agent loop | A Go orchestrator watches GitHub and starts **sessions**. A session is the agentic loop from agent-sandbox's `examples/sandboxed-tools`, forked into this repo: the LLM loop runs in the control plane, every tool call executes in the session's sandbox. Phase 1 is one "all-in-one" session per run that does the work and opens the PR. Later, an outer session gets tools to start inner sessions running other agent personalities (gemini-cli, Claude Code, codex, opencode, pi, or native toolsets). |
| 2026-10-23 | Goal format | `.goals/<name>.md`: Markdown for the agent, with optional YAML front matter for machine-readable fields (metric, verify, budget, schedule). More goal types later. |
| 2026-10-23 | GitHub | GitHub App in production. Development must not require an App: a **forge** abstraction (adapted from gke-labs/eval-runner) with a `gitremote` implementation that polls `git ls-remote`, a `github` implementation that works with a PAT or an App, and a `fake` for tests. |
| 2026-10-23 | Credentials and egress | gke-labs/service-portals runs as a sidecar in every sandbox pod: no real tokens inside the sandbox, iptables egress allowlist, TLS interception with a pod-local CA, credential injection per host, usage logging for LLM traffic. |
| 2026-10-23 | Filesystem storage | gke-labs/in-cluster-storage (ObjectFS) provides the session workspace volume (clone, worktrees, agent state) so a workspace survives sandbox recreation and can be resumed, replacing the tar snapshot/restore of the upstream example. Control-plane state is files under one state directory, which in production is an ObjectFS volume. |
| 2026-10-23 | Control plane | One Go binary, `ao`. `ao serve` is a normal Deployment on the cluster that has agent-sandbox installed. Server-rendered `html/template` UI, no JavaScript framework. |
| 2026-10-23 | Metric trust | The agent never decides a PR is good. For metric goals the orchestrator re-measures the candidate in a clean sandbox before opening a PR. |
| 2026-10-15 | Billing | Platform-held LLM keys (injected by the portal, never in the sandbox), usage metered per run and per repo, hard budget caps. Stripe later. |
| 2026-10-15 | Deployment | kind locally; the GKE cluster that already runs agent-sandbox for real use. runc first, gVisor via RuntimeClass later. |

Compared with the spike: the agent loop moves from inside the sandbox to the
control plane, so there is no in-sandbox worker entrypoint and no CLI tool
belt for a supervisor agent; blob-store SDKs are replaced by a plain state
directory on a cluster filesystem; a PAT-only first cut is replaced by the
forge abstraction so both PAT and App work from day one.

## Architecture

```
            GitHub (github.com or any git remote)
              ^  webhooks (App) or git ls-remote polling (forge)
              |  PRs, comments, checks (forge Reporter)
              v
 +------------------------------------------------------------------+
 |  ao serve  (Deployment, namespace ao-system)                      |
 |                                                                  |
 |   forge syncer --> goal discovery --> run scheduler              |
 |                                           |                      |
 |   state dir (ObjectFS volume)             v                      |
 |   repos/<host>/<owner>/<name>/...   session manager              |
 |                                     (LLM loop, tool registry)    |
 |   web UI (html/template)                  |                      |
 +-------------------------------------------|----------------------+
                                             | sandboxd gRPC :9090 + REST :8080
                                             | (bearer token, NetworkPolicy)
                                             v
 +------------------------------------------------------------------+
 |  Sandbox (agents.x-k8s.io/v1beta1), namespace ao-sandboxes        |
 |                                                                  |
 |   init: init-service-portals  (pod CA, iptables egress rules)    |
 |   init: install-sandboxd      (copies sandboxd into /ao-bin)     |
 |   container worker: tool-rich image, runs sandboxd as PID 1      |
 |       /workspace  <- ObjectFS PVC (exclusive-local, RWOP)        |
 |       repo clone, git worktrees, agent CLIs (inner sessions)     |
 |   container portal: all-in-one-portal (UID 1337)                 |
 |       api.github.com, github.com (git) -> installation token     |
 |       api.anthropic.com, generativelanguage.googleapis.com, ...  |
 |       -> platform LLM keys, usage logged per request             |
 +------------------------------------------------------------------+
```

### Components

| Package | Origin | Purpose |
|---|---|---|
| `cmd/ao` | new | Single binary: `serve` (control plane + UI), `run` (one run from a laptop against kind), `goal validate`. |
| `pkg/forge` | adapted from eval-runner `pkg/forge` | `Forge` interface (repository, branches, change requests, commits), `Reporter` (comments, statuses), plus a `ChangeRequestCreator` we add for opening PRs. Implementations: `github` (PAT or App installation token), `gitremote` (pure-Go `ls-remote`, no credentials), `fake`. `syncer` polls and emits events; `store` is a rebuildable local cache. |
| `pkg/goals` | new | Finds `.goals/*.md` at the default-branch head, parses front matter, validates, computes a content hash so unchanged goals are not re-run needlessly. |
| `pkg/session` | forked from agent-sandbox `examples/sandboxed-tools/pkg/{agent,sessions}` | The agentic loop: messages, tool dispatch, approval hooks, transcript persistence, sandbox lifetime extension. Changed to talk to sandboxd and to use a workspace volume instead of tar snapshots. |
| `pkg/llm` | forked from `sandboxed-tools/pkg/llm` | Minimal OpenAI-compatible chat client with tool calling, plus `fake` for tests. Gemini and Anthropic both expose OpenAI-compatible endpoints. |
| `pkg/tools`, `pkg/toolsets` | forked from `sandboxed-tools/pkg/{tools,toolsets}` | Tool interface, registry, output limiting, the `basic` and `geminicli` toolsets (the `geminicli-toolbox` helper binary ships in our worker image). New `ao` toolset adds `measure`, `open_pull_request`, `finish`, and later `start_session`. |
| `pkg/acp` | forked from `examples/agentclientprotocol/pkg/acp` | ACP client used to drive inner CLI agents that speak ACP (gemini-cli `--acp`) over a sandboxd `Start` stream, giving us streamed tool calls and permission gating without parsing terminal output. |
| `pkg/agents` | new (idea from the spike) | Declarative definitions of CLI agent personalities: command template, prompt delivery, output parser for summary and token usage. |
| `pkg/sandbox` | new | Builds the per-run `Sandbox` object (or `SandboxClaim` once warm pools are used), waits for readiness, holds the sandboxd client (bearer token, Pod IP or headless Service), extends `shutdownTime`, deletes. Wraps `sigs.k8s.io/agent-sandbox/clients/go/sandbox` where it fits. |
| `pkg/portal` | new | Renders per-run `PortalRule`s (which hosts, which token) into the Secret the sidecar reads; parses the portal's usage logs for metering. |
| `pkg/measure` | new (idea from the spike) | Metric protocol (last stdout line is a number or `{"value":N,"unit":"..."}`), median of N runs, verify gate, improvement arithmetic. Now executed through sandboxd `Execute`. |
| `pkg/state` | new | File layout under the state directory, atomic JSON writes, append-only JSONL. |
| `pkg/web` | new | Repo list, repo page with goals and run history, run page with session transcript, tool calls, measurements, PR link. Inline SVG chart of the metric over time. |

Build and deploy follow the gke-labs conventions (`dev/tasks/*`, `.ap/`, `ap deploy`), like eval-runner and service-portals.

## Goals

A goal is a Markdown file under `.goals/` on the default branch. The file name
is the goal name (`.goals/sort-benchmark.md` is goal `sort-benchmark`), used
in branch names, PR titles, and the UI. The body is shown to the agent
verbatim. Front matter is optional and holds everything the harness needs to
measure and budget:

```markdown
---
metric:
  command: ./hack/bench.sh      # run from the repo root; last stdout line is the number
  direction: lower               # lower | higher
  unit: ns/op
  minImprovement: 1%             # absolute (5) or relative (1%); default: any improvement
  runs: 3                        # repeat, take the median; default 1
  timeout: 10m
verify: go test ./...            # must exit 0 for a candidate to count
budget:
  maxDuration: 2h
  maxCostUSD: 20
schedule: daily                  # daily | weekly | on-change (default) | manual
agents:                          # optional allowlist of inner-session personalities
  - gemini
  - claude
---
# Make the sort benchmark faster

Do not change the public API of `pkg/sort`. The hot path is `quick.go`.
Measure with `./hack/bench.sh`.
```

Goal kinds, decided by which front matter is present:

- **Metric goal** (`metric` present): the harness re-measures baseline and
  candidate in clean sandboxes; a PR is opened only if `verify` passes and the
  candidate clears `minImprovement`. PR body carries before/after numbers.
- **Judged goal** (no `metric`): the session works until it calls `finish`;
  the harness checks `verify` (if given) and that the diff is non-empty, then
  opens the PR as a proposal. Feature lists and code quality live here until
  we add rubric/checklist goal types.

Metric protocol: the metric command runs from the repository root and its
**last non-empty stdout line** is the measurement, either a bare number
(`1234.5`), a number with a unit (`1234.5 ns/op`), or JSON
(`{"value": 1234.5, "unit": "ns/op"}`). A non-zero exit code is a failed
measurement, never a number. Everything else the command prints is kept in the
run logs, so scripts can be chatty. `minImprovement` is absolute (`5`) or
relative (`1%`) and is applied in the goal's direction. A goal never changes
what is measured mid-run: the orchestrator freezes the goal file at the base commit, and an
attempt that edits `.goals/` or the metric script is rejected.

## Watching GitHub: the forge

The forge layer isolates everything GitHub-specific and makes development
without a GitHub App possible.

- `gitremote`: lists refs with go-git's ref advertisement. Free, unmetered,
  works for any HTTPS git remote, including a local git daemon in tests. It
  cannot open PRs.
- `github`: REST client authenticated with an App installation token or a PAT.
  Implements `Reporter` and `ChangeRequestCreator`. Reads stay conditional
  (ETag) and respect a rate-limit floor.
- `fake`: in-memory, for unit tests.

Polling follows eval-runner's three tiers: `ls-remote` every 60s with jitter
for change detection, API fill-in calls only for refs that moved, a full
conditional reconcile hourly. Webhooks (App `push`, `pull_request`,
`issue_comment`) are an accelerator on top of the same consumer, not a
separate code path, so the webhook handler stays credential-free and
idempotent.

Events we consume:

| Event | Action |
|---|---|
| Default branch advanced | Re-read `.goals/`; create or update goal records; schedule runs for goals whose `schedule` is `on-change` and whose content hash or base commit changed. |
| Our PR closed or merged | Mark the run; for merged PRs record the new best; release the workspace volume after the retention window. |
| Comment on our PR (later) | Resume the run's session with the review comments as the next user turn, push to the same branch. |
| `/ao run <goal>` comment by a collaborator (later) | Manual trigger. |

Registration: in production, installing the App on a repository registers it.
In development, `ao serve --repo https://github.com/you/repo` or the UI form
registers a repository against the `gitremote` or PAT-backed `github` forge.

## Sessions

A session is one LLM conversation with tools, bound to one sandbox and one
workspace volume. The loop is upstream's `Harness.RunTurn`: call the model,
execute tool calls in the sandbox, append results, repeat until the model
answers without tool calls. We keep the `TurnEvents` hook so the same loop
serves the UI (transcript streaming to the state dir) and tests.

What changes from the upstream example:

- **Execution** goes through sandboxd: `Execute` for short tool calls,
  `Start` streaming for long ones (benchmarks, test suites, inner agents),
  `SendSignal` on tool timeout. Output is capped by the existing limited
  writer; full output is written to the run's state dir.
- **Filesystem** operations use sandboxd REST where it helps (read, write,
  list) and the `geminicli-toolbox` binary for glob, grep, replace.
- **Persistence**: `/workspace` is an ObjectFS volume. There is no snapshot
  step; if the sandbox is recreated (expiry, node loss, resume after review
  comments) the new pod mounts the same volume.
- **Lifetime**: every tool call extends `spec.shutdownTime` by the inactivity
  window (5 minutes) up to the run deadline; `shutdownPolicy: Delete` cleans
  up if the control plane dies.
- **Model access**: the control plane calls the model with platform keys for
  native sessions. Inner CLI agents inside the sandbox reach the model APIs
  through the portal sidecar.

### The `ao` toolset (phase 1, all-in-one)

System prompt: the goal body, repository facts (default branch, base commit,
languages detected), the metric and verify commands, the budget, and the rules
(never edit `.goals/` or the metric script, keep changes focused, end with a
summary written for maintainers).

| Tool | Runs where | Notes |
|---|---|---|
| `run_command`, `read_file`, `write_file`, `replace`, `glob`, `grep_search`, `list_directory` | sandbox | From the `geminicli` toolset. |
| `measure` | control plane, executes in the sandbox | Runs `verify` and `metric` on the current tree, records a `Measurement` in the run, returns a compact result with the baseline next to it. |
| `open_pull_request(title, body)` | control plane | Commits any uncommitted changes, pushes `ao/<goal>/<run-id>` through the portal, then hands over to the orchestrator's **select and verify** step. The tool result tells the model whether a PR was opened and why not if not. |
| `finish(summary)` | control plane | Ends the session without a PR. |
| `start_session(personality, task, branch)` (phase 2) | control plane | Starts an inner session; returns its summary and branch. |

### Inner sessions and personalities (phase 2)

The outer session is a planner with a small context budget. It delegates to
inner sessions, each with its own sandbox and workspace volume, cloned from
the repository at a branch the outer session names. Inner sessions hand work
back as a pushed branch plus a short summary; the outer session measures and
decides what to submit. Two kinds of personality:

- **Native**: the same Go loop with a different toolset, system prompt, and
  model (for example a "Gemini reviewer" or a "Claude implementer").
- **CLI**: an agent CLI from `pkg/agents` (gemini-cli, Claude Code, codex,
  opencode, pi) started with sandboxd `Start` in the inner sandbox, prompt on
  stdin, transcript streamed back. Where the CLI speaks ACP we drive it with
  `pkg/acp` over the same stream, so tool calls are visible and can be gated.

This is the comparison surface we care about: same goal, same budget,
different personalities, measured by the same harness.

## Sandboxes

One `SandboxTemplate` (`ao-worker`) per worker image; runs create a `Sandbox`
from it (a `SandboxClaim` against a `SandboxWarmPool` once startup latency
matters). The pod:

- `automountServiceAccountToken: false`, non-root uid 1000, all capabilities
  dropped, `allowPrivilegeEscalation: false`, resource limits from the goal's
  budget tier, optional `runtimeClassName: gvisor`.
- **worker** container: our tool-rich image (git, Go, Node, Python, build
  essentials, the agent CLIs, `geminicli-toolbox`). sandboxd is injected by an
  init container (topology B from the sandboxd user guide) so commands run in
  the tool-rich rootfs. The image ships `tini` so sandboxd is not PID 1 for
  long-lived sandboxes. `/workspace` is the sandboxd root and an ObjectFS
  volume declared in `volumeClaimTemplates` (`objectfs-exclusive-local`,
  `ReadWriteOncePod`, periodic checkpoint). sandboxd runs with
  `--auth-token-file` pointing at a per-run Secret; the control plane holds the
  token, the pod spec never contains it.
- **portal** sidecar: `all-in-one-portal` as uid 1337, rules from a per-run
  Secret. `init-service-portals` installs the iptables rules: only the proxy
  may leave the pod, DNS only to cluster resolvers, metadata server blocked.
  Rules match the real hostnames (`api.github.com`, `github.com`,
  `api.anthropic.com`, `generativelanguage.googleapis.com`, package registries
  as the repo needs), so agent CLIs need no base-URL changes, only the pod CA
  in `SSL_CERT_FILE`, `NODE_EXTRA_CA_CERTS`, and git's `http.sslCAInfo`.
- **NetworkPolicy**: ingress to 8080 and 9090 only from `ao serve` pods;
  egress handled by the portal's iptables plus a deny-by-default policy. The
  template's managed policy must be replaced with ours because the default
  only admits `sandbox-router`.

Connectivity from the control plane is in-cluster (Pod IP, or headless Service
with `spec.service: true`); `sandbox-router` is not used because it cannot
carry gRPC today. Readiness is the Sandbox `Ready` condition, which reflects
sandboxd's `/v1/health` probe.

Git access: the repository is cloned over HTTPS through the portal, which
injects the installation token (or PAT). Pushes are limited by the token's
scope (`contents: write` on the one repository); the portal can additionally
restrict methods and paths. The portal's `gitproxy-server` is an option for
caching clones of large repositories across runs.

## Run lifecycle

```
Pending -> Preparing -> Working -> Selecting -> OpeningPR -> Succeeded
                                     |              |
                                     |              +-> NoImprovement
                                     +-> Failed / Cancelled / Expired
```

1. **Pending**: a goal needs a run (schedule due, goal changed, manual). The
   scheduler enforces one active run per goal and the repo's budget cap.
2. **Preparing**: create the workspace PVC, the sandboxd token Secret, the
   portal rules Secret (installation token minted now, valid one hour, the
   portal re-reads rules on `SIGHUP` when the control plane rotates it), and
   the Sandbox. Wait for `Ready`. Clone the repository at the base commit,
   freeze the goal file, measure the **baseline** (metric goals).
3. **Working**: run the session turn by turn until it calls
   `open_pull_request` or `finish`, the model stops, or a limit is hit
   (`maxDuration`, `maxCostUSD`, max turns, tool timeout). Every tool call,
   its truncated result, and its full output go to the state dir as they
   happen, so the UI shows progress live.
4. **Selecting**: for metric goals, start a clean sandbox, clone the pushed
   branch, run `verify` and `metric` (median of N). Compare with the baseline
   measured the same way. For judged goals, run `verify` if present.
5. **OpeningPR**: open or update the PR via the forge with a body the harness
   writes: goal, base commit, baseline and candidate numbers, improvement,
   cost, link to the run page, then the session's own summary.
6. **Terminal**: delete the Sandbox (the portal and token Secrets with it via
   owner references). Keep the workspace volume for the retention window so
   the session can be resumed with review feedback; delete it when the PR is
   closed or merged or the window passes.

Idempotency: a run has one deterministic branch name; re-running `OpeningPR`
updates the existing PR. The scheduler skips a goal that already has an open
auto-optimizer PR unless the goal's `schedule` says otherwise.

## State

Everything is a file under the state directory (`--state-dir`, an ObjectFS
volume in production, a local directory in development and tests). JSON
objects are written atomically; logs are append-only JSONL. The forge cache
(bbolt) lives there too and may be deleted at any time.

```
repos/<host>/<owner>/<name>/repo.json
repos/<host>/<owner>/<name>/goals/<goal>/goal.json           parsed goal, hash, best so far
repos/<host>/<owner>/<name>/runs/<run-id>/run.json           phase, timings, baseline, candidate, PR, cost
repos/<host>/<owner>/<name>/runs/<run-id>/events.jsonl       phase changes, measurements, errors
repos/<host>/<owner>/<name>/runs/<run-id>/sessions/<session-id>/metadata.json
repos/<host>/<owner>/<name>/runs/<run-id>/sessions/<session-id>/messages.jsonl
repos/<host>/<owner>/<name>/runs/<run-id>/sessions/<session-id>/tools/<n>.json   full tool output
repos/<host>/<owner>/<name>/usage/<YYYY-MM>.json              metering roll-up
forge/forge.db                                                 rebuildable cache
```

Run IDs sort by time (`20261023T120000Z-ab12`). Exactly one writer per file:
the session manager owns its run directory, the syncer owns `repo.json` and
`goal.json`.

## Security model

- The sandbox holds no credentials: not for GitHub, not for models, not for
  the cluster. Its outbound identity is whatever the portal injects, scoped to
  one repository and the platform's LLM projects.
- The sandbox cannot reach the control plane, other sandboxes, the metadata
  server, or arbitrary hosts. Egress is an allowlist in iptables, owned by a
  UID the workload cannot become, plus NetworkPolicy.
- The control plane reaches sandboxd only with a per-run bearer token over
  the pod network restricted by NetworkPolicy; mTLS as in eval-runner is the
  follow-up if we deploy where NetworkPolicy is not enforced.
- Prompt-injected code in the repository can waste budget and produce a bad
  PR; it cannot exfiltrate tokens or touch other repositories. Budgets are
  enforced by the orchestrator, not by the prompt.
- The webhook endpoint is credential-free; it only enqueues events.

## Metering and budgets

Native sessions report token usage from API responses. Inner CLI agents are
metered by the portal: the Anthropic and Gemini portals parse request and
response bodies and log usage with the pod identity; the control plane
attributes it to the run. Each run records cost; each repository rolls up per
month; the scheduler refuses to start runs over the repository's cap and the
session is stopped when a run's `maxCostUSD` is reached.

## Web UI

Server-rendered pages, no JavaScript framework, auto-refresh with a meta
refresh tag on live pages:

| Path | Content |
|---|---|
| `/` | Repositories, goals, last run, best metric. |
| `/repos/{host}/{owner}/{name}` | Goals with schedule and best, run history, SVG trend per metric goal, Run now. |
| `/repos/.../runs/{id}` | Phase, budget used, baseline and candidate, PR link, session list. |
| `/repos/.../runs/{id}/sessions/{sid}` | Transcript with tool calls, truncated results, links to full outputs. |
| `/healthz` | Liveness. |

Auth: none while the UI is cluster-internal; GitHub OAuth with repository
ownership checks before anything is exposed publicly.

## Development without a GitHub App

- `ao serve --forge gitremote --repo <url>` watches any public repository or a
  local git daemon and runs sessions; PRs are skipped or pushed to a local
  bare remote.
- `GITHUB_TOKEN=... ao serve --repo <url>` uses the `github` forge with a PAT
  and opens real PRs from your account. Use a fork, since merging your own
  bot's PR is a self-merge.
- `ao run --repo . --goal sort-benchmark` runs once from a laptop against the
  current kubeconfig (kind with agent-sandbox, sandboxd image, and ObjectFS
  installed by `dev/tasks/kind-up`).
- Unit tests use the `fake` forge, the fake LLM (`fake-eliza` style scripted
  tool calls), and a fake sandboxd client. One e2e test runs on kind in CI.

## Ideas from the spike

A one-week spike (2026-10-15) built a working vertical slice with a different
shape: a worker process inside the sandbox ran a supervisor coding agent that
delegated to subagents through a CLI tool belt. We are not following it
closely, but these ideas held up and inform this design. Treat them as
guidance, not as a spec.

- **The harness decides, the agent proposes.** The agent's own numbers are
  never trusted. The final tree is re-measured by Go code and the PR body is
  generated from that measurement. Any agent-reported value is at most
  context for the reviewer.
- **A tiny metric protocol beats a plugin API.** Any command that prints a
  number on its last line is a metric; a non-zero exit is a failure, not a
  value; repeat and take the median. Repositories can adopt it with a shell
  script. (Described in full under Goals.)
- **Small outputs for the model, full outputs on disk.** Tool calls returned
  about 40 lines and wrote everything else to the run directory. The model's
  context stayed small and the UI still had the full story. Our limited
  writer plus per-tool output files keep this.
- **Context discipline for the planner.** The supervisor prompt said: spend a
  few turns on layout, never run the benchmark yourself, give subagents
  complete self-contained tasks, prefer several small independent attempts in
  separate worktrees over one long one, ask each subagent for a five-line
  summary, stop before the deadline. This carries straight into the outer
  session's system prompt in phase 2.
- **Agents as data, not code.** Each CLI agent was a definition: command
  template, whether the prompt goes on stdin, and an output mode that extracts
  the final message and token usage from the CLI's JSON stream. Adding an
  agent was a YAML entry. Headless flags found to work in October 2026:
  `claude -p --output-format stream-json --dangerously-skip-permissions`,
  `gemini --approval-mode yolo --skip-trust --output-format json`,
  `codex exec --dangerously-bypass-approvals-and-sandbox --json`,
  `opencode run --auto --format json`, `pi --mode json`. Model names differ
  (`provider/model` for opencode and pi, bare ids elsewhere). The auto-approve
  flags are acceptable only because the sandbox is the permission boundary.
- **Freeze the goal at the base commit.** The goal file was copied out of the
  tree before the agent started, so an agent could not change what was being
  measured. Edits to the goal or the metric script in an attempt are rejected.
- **Exactly one writer per state object.** State was JSON files keyed by a
  time-sortable run ID (`20261015T120000Z-ab12`), one writer per object, no
  partial updates. Concurrency problems never appeared. Same rule here.
- **Let the sandbox controller be the safety net.** Each run's `Sandbox` had
  `shutdownPolicy: Delete` and a `shutdownTime` of deadline plus grace, so a
  dead control plane could not leak pods. Everything of value was persisted
  outside the pod before exit.
- **Sandbox image recipe.** Debian slim, Go, Node, Python, build essentials,
  the agent CLIs installed with npm, a non-root uid 1000 user with git
  configured, `GIT_TERMINAL_PROMPT=0`. Our worker image starts from this and
  adds sandboxd injection, `tini`, and `geminicli-toolbox`.
- **Dogfooding through a checklist.** The spike's repository goal was the
  percentage of acceptance checks passing in a spec file, each check a fast
  command such as `go test -run TestX`, with the rule that weakening a check
  is cheating. That returns as a checklist goal type and this repository's
  own `.goals/`.
- **A fake agent and a local bare remote** made the whole lifecycle testable
  without network or credentials. Our fake LLM and fake forge serve the same
  purpose.

Dropped on purpose: the in-sandbox worker entrypoint, the supervisor CLI tool
belt, blob-store SDKs for state, and PAT-first sequencing.

## Expected upstream work

Things this design leans on that upstream does not fully provide yet. Each is
an issue in the respective project, not a workaround here.

- agent-sandbox: `sandbox-router` cannot proxy gRPC, so sandboxd needs direct
  Pod IP or Service connectivity and a custom NetworkPolicy; the managed
  policy only admits the router. Per-sandbox `env` or Secret volumes force a
  cold start from a warm pool, which conflicts with per-run portal rules and
  sandboxd tokens. sandboxd as PID 1 does not reap orphans. `Start` needs a
  way to bound output or we rely on the client.
- service-portals: rules are read from a mounted Secret at start and on
  `SIGHUP`; a config API would allow warm pools and token rotation without a
  Secret per pod. Git smart-HTTP auth header form for GitHub installation
  tokens must be verified in the `all-in-one-portal` path. Usage logs need a
  structured format the control plane can attribute to a run.
- in-cluster-storage: `volumeClaimTemplates` on `Sandbox` with the
  `objectfs-exclusive-local` StorageClass and `ReadWriteOncePod`; hand-off
  between a deleted and a recreated sandbox pod relies on the controller
  hold lease; checkpoint interval versus session tool-call cadence.

## Roadmap

Each milestone is a handful of issues assigned to the coding bot.

1. **Skeleton**: `cmd/ao`, `pkg/state`, `pkg/goals` (front matter parsing and
   validation), `pkg/forge` adapted from eval-runner with `gitremote`,
   `github` (PAT), `fake`, and the syncer; `ao serve` lists repos and goals in
   the UI. No sandboxes yet.
2. **Sessions on kind**: fork `sandboxed-tools` into `pkg/session`, `pkg/llm`,
   `pkg/tools`, `pkg/toolsets`; `pkg/sandbox` with sandboxd (bearer token,
   Pod IP, NetworkPolicy); worker image with injected sandboxd; `ao run`
   executes a session against a fixture repository with the fake LLM.
3. **All-in-one PR**: the `ao` toolset with `measure`, `open_pull_request`,
   `finish`; baseline and select steps in clean sandboxes; PRs through the
   `github` forge; run and session pages in the UI. First real PR on a test
   repository.
4. **Portal and ObjectFS**: portal sidecar with per-run rules and token
   minting; workspace on ObjectFS; resume a session after review comments.
5. **GitHub App**: installation flow, webhook accelerator, installation
   tokens, OAuth for the UI; deploy to GKE.
6. **Inner sessions**: `start_session`, native and CLI personalities, ACP for
   gemini-cli, comparison page across personalities.
7. **Operations**: metering from portal logs, budgets, warm pools, gVisor,
   checklist and rubric goal types, this repository's own `.goals/`.

## Development process for this repository

Design changes land here first. Work is filed as issues and assigned to the
coding bot, which sends PRs; maintainers review, request changes, and merge.
Maintainers do not send code PRs themselves so that merges are never
self-merges. An issue should name the milestone, the packages touched, the
tests expected, and the acceptance check a reviewer will run.
