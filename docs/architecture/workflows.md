# Workflows Architecture

A workflow in auto-optimizer uses two paired Kubernetes controllers over two
custom resources, `HeroRun` and `Session`.
The `HeroRun` controller drives each run from creation to a terminal outcome,
and the `Session` controller drives each LLM conversation with tools.

The goal is to adopt the Kubernetes controller model cleanly across both runs
and sessions.
All durable workflow state and session state live on `HeroRun` and `Session`.
Neither controller keeps shadow state in process memory.
Speaking strictly through the Kubernetes API decouples both controllers from
storage.
Auto-optimizer starts with `kube-apiserver` as the backing store, and can later
move both resources to a backend such as Redis behind a Kubernetes aggregated
API server without changing either controller.

## Core rules

Seven invariants govern workflow design.

1. **`HeroRun` and `Session` are the single source of truth for control-plane
   state.**
   The controllers, the web UI, and the CLI all read and update run and session
   state through the Kubernetes API.
   An optional `run.json` disk export exists only for human inspection.
   The system never reads it.
2. **Controllers keep no shadow state in memory.**
   Workflow progress, session state, and session outcomes live on `HeroRun` and
   `Session` rather than in controller memory.
   Losing all process memory never loses workflow progress or completed session
   outcomes.
3. **Bulk payloads stay on the state volume.**
   Conversation transcripts, unabridged tool outputs, and the user-facing event
   log remain files on the persistent state volume rather than fields on
   `HeroRun` or `Session`.
4. **Crash recovery comes from controller idempotency.**
   Every reconcile pass must be safe to repeat from the last persisted `HeroRun`
   or `Session` state and durable state-volume files after a crash at any point.
   The system does not rely on graceful shutdown handlers or an event-replay log
   for correctness.
5. **Controllers never poll in-process or block unrelated work.**
   The `HeroRun` controller returns promptly and relies on Kubernetes watches
   over owned resources rather than waiting on slow work.
   The `Session` controller isolates model turns and tool calls in its own
   worker pool and returns promptly whenever a session waits on a sandbox or
   child sessions.
6. **Persisted state never holds credentials.**
   Authentication tokens and signing keys stay out of `HeroRun`, `Session`, and
   state files.
   The control plane holds secrets in memory or injects them through the portal.
7. **Prefer standard Kubernetes controller libraries.**
   Build on the Go standard library and standard Kubernetes controller machinery
   instead of adopting an external workflow engine or custom background worker
   framework.

## Paired `HeroRun` and `Session` controllers

In a Kubernetes controller, each reconcile pass reads the current resource from
the API, compares desired state with observed state, performs one bounded unit
of work, and writes updated status back through the API.
Auto-optimizer applies this pattern as a paired-controller design analogous to
Kubernetes `CronJob` and `Job`.

Just as `CronJob` manages higher-level lifecycle orchestration and delegates
execution to owned `Job` resources, `HeroRun` manages the run workflow and
delegates agent reasoning to owned `Session` resources.

- **`HeroRun` orchestrates the run.**
  The `HeroRun` controller provisions the workspace claim and initial sandbox,
  prepares the repository clone, creates the run's root `Session`, verifies and
  exports the candidate, opens a pull request, and releases resources at the end
  of the run.
  It never runs an LLM loop itself.
- **`Session` executes the agent conversation.**
  The `Session` controller drives the LLM loop for a single session, dispatches
  tool calls to the sandbox, persists transcripts and tool outputs on the state
  volume, coordinates delegated child sessions, and reports progress and final
  outcomes on `Session` status.

Splitting runs and sessions into paired custom resources provides four
architectural benefits.

1. **Clear boundaries.**
   Run orchestration stays separate from model prompting, tool execution, and
   conversation state.
2. **Standard Kubernetes coordination.**
   `HeroRun` owns its root `Session` through a Kubernetes controller owner
   reference and watches `Session` resources through `Owns`.
   Whenever the `Session` controller updates `Session` status, the watch wakes
   the owning `HeroRun` automatically without custom wake channels or in-memory
   worker registries.
3. **Isolated concurrency.**
   Each controller has its own workqueue and worker pool.
   Slow model responses and sandbox commands run inside the `Session` controller
   pool and never stall `HeroRun` reconciliation for workspace setup, candidate
   verification, or other runs.
4. **First-class observability and cleanup.**
   Every root and child session is a Kubernetes resource that operators can
   inspect with `kubectl` and that the CLI and web UI can watch directly.
   Kubernetes garbage collection deletes those sessions automatically when their
   owner disappears.

### Spec, status, and optimistic concurrency

Both `HeroRun` and `Session` separate immutable desired inputs from observed
execution progress.

- **`HeroRun` spec and status.**
  Spec holds immutable desired inputs set when the scheduler creates the run,
  such as the target repository, the frozen goal definition, the base commit,
  and the run limits.
  Status holds observed workflow state, including the current phase, Kubernetes
  conditions, child resource references, progress and cost metrics, and the run
  outcome.
- **`Session` spec and status.**
  Spec holds immutable inputs set when the owner creates the session, such as
  the prompt or role configuration, budget limits, sandbox and workspace
  context, and parent relationship.
  Status holds observed session state, including the current phase, Kubernetes
  conditions, turn and token metrics, requested run actions such as opening a
  pull request, and the terminal outcome.
- **`SessionMessage` spec and status.**
  Spec holds the immutable payload of a message addressed to a `Session`,
  including the target session name, optional run identifier, optional sender
  identity, and message text.
  Status records delivery state through `DeliveredAt` so the `Session`
  controller never mutates spec.

Standard Kubernetes optimistic concurrency protects status updates on these
resources.
Every write carries the resource version that the controller read.
If another actor updated the resource concurrently, the API rejects the write
with a conflict error.
The controller discards its stale copy and requeues the resource to reconcile
from the latest resource version.

### Disk exports for human inspection

The control plane can optionally write a snapshot of `HeroRun` to `run.json` on
the state volume after a status update.
It can also write timestamped copies to preserve a point-in-time timeline on
disk, because etcd regularly compacts older resource versions.

These exports exist strictly so an operator can inspect a run directory with
standard command-line tools.
Because an export is optional and written after the API update, it can be stale
or missing.
The controllers, the web UI, and the CLI never read `run.json`.
They always read `HeroRun` and `Session` through the Kubernetes client so every
component sees one authoritative state.

## Storage separation

Auto-optimizer divides state strictly between control-plane resources in the
Kubernetes API and bulk payloads on the persistent state volume.

- **`HeroRun` and `Session` in the Kubernetes API.**
  Hold workflow and session phases, Kubernetes conditions, ownership and
  resource references, progress counters, token and cost metrics, benchmark
  results, and pull request or session outcomes.
- **Files on the state volume.**
  Hold the `messages.jsonl` conversation transcripts for root and child
  sessions, unabridged tool execution outputs, the `events.jsonl` append-only
  event log, and optional human-readable exports.

Two operational constraints require this split.

1. **Object size limits.**
   etcd caps individual objects at 1.5 megabytes.
   Conversation transcripts, prompts, compiler diagnostics, and tool outputs
   routinely exceed that limit in a single run and cannot live inside `HeroRun`
   or `Session`.
2. **Write amplification and churn.**
   Agent turns produce continuous streaming events and verbose tool output.
   Writing every intermediate message or tool output through etcd would trigger
   excessive Raft consensus rounds, increase compaction pressure, and degrade
   cluster responsiveness.
   The state volume handles high-throughput, append-only file writes without
   burdening the API server.

### Event log and status conditions

The `events.jsonl` event log is an append-only audit trail for operators and the
web UI.
The control plane appends events when phases change, measurements arrive, errors
occur, or tools execute.
Neither controller reads `events.jsonl` to decide what to do next.

Kubernetes conditions on `HeroRun` and `Session` status report high-level
readiness and milestones so operators and cluster tooling can observe progress
natively with `kubectl`.

## Run lifecycle

A `HeroRun` moves through five active phases before reaching a terminal phase.

- **`Pending`.** The `HeroRun` controller ensures the workspace claim and
  initial sandbox exist with controller owner references pointing to `HeroRun`,
  records those references in status, and advances to `Preparing`.
- **`Preparing`.** The `HeroRun` controller waits for the sandbox to become
  ready without blocking.
  Once ready, the controller binds the portal egress policy, ensures the
  repository clone is at the base commit, configures git identity, and advances
  to `Working`.
- **`Working`.** The `HeroRun` controller ensures the run's root `Session`
  exists with a controller owner reference pointing to `HeroRun`, records that
  reference in status, and returns while the `Session` controller runs the
  conversation.
  When the root `Session` requests a pull request, `HeroRun` advances to
  `Selecting`.
  When the root `Session` finishes without requesting a pull request, `HeroRun`
  enters `NoImprovement`.
  When the root `Session` fails, `HeroRun` enters `Failed`.
- **`Selecting`.** The `HeroRun` controller verifies the candidate, exports the
  candidate commits, pushes the branch to the forge, and advances to
  `OpeningPR`.
- **`OpeningPR`.** The `HeroRun` controller checks for an existing pull request
  on the branch or opens a new one, records the pull request link, and enters
  `Succeeded`.
- **Terminal phases.** When a run reaches `Succeeded`, `Failed`, or
  `NoImprovement`, the `HeroRun` controller stops reconciling the run and
  releases the active sandbox.

Each reconcile pass returns one of four outcomes to the controller runtime.

- **Done.** The resource has reached a terminal phase and needs no further
  reconciliation.
- **Requeue immediately.** One bounded unit of work finished and the next step
  can proceed right away.
- **Wait for external changes.** The controller is waiting on an owned resource
  such as a sandbox or a `Session`.
  Kubernetes `Owns` watches enqueue the owner when child state changes, while a
  periodic requeue duration provides a safety backstop.
- **Error.** A transient infrastructure failure occurred.
  The controller runtime logs the error and retries with backoff.

## The `Session` controller model

The `Session` controller drives every agent conversation through standard
Kubernetes reconciliation instead of relying on ad-hoc background workers inside
the `HeroRun` controller.

### Watch-driven handoff between `HeroRun` and `Session`

During `Working`, `HeroRun` does not hold a goroutine open or poll for turn
completion.
Instead, `HeroRun` creates a root `Session` owned by that `HeroRun` and returns.
Because the `HeroRun` controller registers an `Owns` watch on `Session`, any
status update written by the `Session` controller triggers a watch event that
maps through the owner reference and enqueues the owning `HeroRun`.

On that wake-up, `HeroRun` reads the updated `Session` status.
If the `Session` is still active, `HeroRun` stays in `Working` and returns
promptly.
If the `Session` has requested a pull request or reached a terminal state,
`HeroRun` transitions to `Selecting`, `NoImprovement`, or `Failed`.
All coordination travels through `Session` status and Kubernetes watches rather
than controller memory.

### Reconciling the LLM loop

The `Session` controller runs its own bounded worker pool so active model calls
and sandbox tool executions never block `HeroRun` reconciliation.
On each reconcile pass for an active `Session`, the controller loads the
session's `messages.jsonl` transcript from the state volume, drives bounded LLM
and tool-call work against the sandbox, appends new messages to the transcript,
and writes updated progress or outcome to `Session` status.

If the sandbox disappears during a turn, the `Session` controller obtains a
replacement sandbox attached to the same persistent workspace volume and
continues cleanly.
If the replacement sandbox is still starting, the controller returns promptly
and wakes when the sandbox becomes ready rather than polling in-process.

### Child sessions as `Session` resources

A session can delegate sub-tasks to child sessions using the exact same
controller model.
When a session delegates work, the `Session` controller creates child `Session`
custom resources with controller owner references pointing to the parent
`Session` rather than spawning unmanaged in-process goroutines.

- **Uniform execution.** Child sessions run through the same `Session`
  controller loop, status reporting, and crash-recovery path as root sessions.
- **Shared run environment.** Child sessions share the run's sandbox, workspace
  volume, deadline, and budget boundaries while writing to their own
  `messages.jsonl` transcript files on the state volume.
- **Non-blocking delegation.** The `Session` controller registers an `Owns`
  watch on `Session` as well.
  When a parent session pauses to wait for child sessions, its reconcile pass
  returns without spending model turns or occupying a worker slot.
  When a child `Session` updates its status or reaches a terminal outcome, the
  watch event wakes the parent `Session` through its owner reference so the
  parent can observe the result and resume.
- **Hierarchical ownership.** Owner references form a clean tree from `HeroRun`
  to the root `Session` to any child `Session` resources.
  Deleting a parent `Session` or the `HeroRun` cascades cleanup through the
  entire session tree automatically.

### Delivering messages with `SessionMessage`

Peer sessions calling `send_message` and users in the web UI or CLI send
messages to a `Session` by creating immutable `SessionMessage` custom resources.
Each `SessionMessage` carries labels identifying its target session and optional
run, along with a controller owner reference pointing to the target `Session`.

Because the `Session` controller registers an `Owns` watch on `SessionMessage`,
creating a message wakes the target `Session` automatically without
file-backed pending queues or out-of-band wakeup channels.
The controller lists undelivered `SessionMessage` resources for the session,
appends them to `messages.jsonl` in creation order, and writes `DeliveredAt` in
status only after the transcript write succeeds.
Messages therefore survive write failures and restarts without modifying
`SessionMessage` spec.

### Recovering in-flight sessions after restarts

If the control plane restarts while a session is mid-turn, the `Session`
resource remains non-terminal in the Kubernetes API.
Completed messages remain durable in `messages.jsonl` on the state volume.
When the `Session` controller starts up again, it reconciles every non-terminal
`Session` from the API.

The controller inspects the durable `messages.jsonl` transcript on the state
volume to determine where the session left off.
If the transcript already contains a completed turn or tool result, the
controller reflects that progress in `Session` status and continues.
If the crash interrupted an exchange before completion, the controller resumes
from the last persisted message in the transcript.
Both root sessions and child sessions recover through this same path without any
in-memory worker tracking or operation handoff state on `HeroRun`.

## Crash recovery and idempotency

The control plane can receive `SIGKILL` or lose network access at any instant.
Rather than catching signals and flushing state during shutdown, every reconcile
step in both controllers is idempotent with respect to the last saved resource
status and durable state-volume files.
Repeating a step after an external side effect has already succeeded produces
the same end state without duplicating work or corrupting the run.

### Example: crash between pushing a branch and saving status

Consider pushing candidate commits during `Selecting`.

1. `HeroRun` records phase `Selecting`, the run's deterministic branch name, and
   the candidate commit SHA.
2. The `HeroRun` controller exports the candidate commits and pushes them to the
   forge.
   The remote repository now points the branch at the candidate commit.
3. Before the controller writes phase `OpeningPR` to `HeroRun`, the control
   plane crashes.
   The Kubernetes API still records phase `Selecting`.
4. After restart, the controller reads `HeroRun`, sees phase `Selecting`, and
   repeats the push step for the same branch and commit.
5. The forge reports that the remote branch already points to that commit, so
   the push succeeds with no duplicate side effects.
6. The controller updates `HeroRun` status to `OpeningPR` and continues to the
   next step.

The same principle applies across all phases and both controllers.
Creating a workspace claim, sandbox, or owned `Session` reuses an existing
resource owned by the same parent rather than creating duplicates.
Binding portal egress policy is idempotent.
Opening a pull request reuses an existing open pull request on the run's
deterministic branch.

## Retention and garbage collection

`HeroRun` owns the lifecycle of a run's cluster resources and state files.

- **Retention period.**
  While an opened pull request remains open, the control plane retains the
  `HeroRun` resource so the run and its sessions can resume if review comments
  arrive.
  Once the pull request merges or closes, or when a run ends without opening a
  pull request, the control plane retains `HeroRun` for a configured retention
  window, such as 30 days, for operator inspection.
- **Deletion trigger.**
  A scheduled retention loop in the control plane deletes expired `HeroRun`
  resources.
  Operators can also delete a `HeroRun` manually at any time with `kubectl`.
- **Cascading Kubernetes cleanup.**
  The root `Session`, each child sandbox, and the workspace claim carry
  controller owner references to `HeroRun`.
  Each child `Session` carries a controller owner reference to its parent
  `Session`, and each `SessionMessage` carries a controller owner reference to
  its target `Session`.
  Deleting `HeroRun` causes standard Kubernetes garbage collection to delete the
  entire `Session` tree, all owned `SessionMessage` resources, any remaining
  child sandbox, and the workspace claim automatically.
- **State volume cleanup.**
  The control plane watches for `HeroRun` deletion and deletes the run's
  directory on the state volume, removing root and child session transcripts,
  tool outputs, event logs, and exported snapshots.

## Hermetic testing

Workflow unit tests run completely in memory with no network access, no external
binaries, and no real `kube-apiserver` or etcd.

An in-memory fake Kubernetes client stands in for `kube-apiserver`.
It tracks `HeroRun`, `Session`, and child resources in memory, enforces status
subresource updates, and checks resource versions for optimistic concurrency
conflicts.
Tests combine this fake client with in-memory fakes for the forge, the sandbox
runtime, and the model client.
This setup exercises full run phase transitions, `HeroRun` and `Session`
watch-driven handoffs, child session delegation, conflict retries, and
crash-recovery scenarios quickly and deterministically.

## Storage evolution

Starting with `kube-apiserver` backed by etcd provides native watches,
optimistic concurrency, RBAC, `kubectl` inspection, and owner-reference garbage
collection for both `HeroRun` and `Session`.
It also avoids synchronization loops between separate stores.

At higher scale, three operational thresholds could justify moving `HeroRun` and
`Session` storage off etcd.

1. **Write rate.**
   Hundreds of concurrent runs and active sessions updating status after each
   step could generate heavy etcd write traffic, raising Raft latency and
   compaction load.
2. **Object size.**
   Even with transcripts and tool outputs on the state volume, richer goal
   definitions, session summaries, or condition histories could grow toward
   etcd's 1.5 megabyte cap.
3. **Historical volume.**
   Retaining tens of thousands of completed runs and their sessions across many
   repositories could strain cluster etcd storage and backup sizes.

When any of those thresholds matters, auto-optimizer can serve `HeroRun` and
`Session` through a Kubernetes aggregated API server backed by a store suited to
that workload, such as Redis or files on the state volume.

Because both controllers interact exclusively through the Kubernetes API and
hold all control-plane state on `HeroRun` and `Session`, this storage change
stays behind the API boundary.

- The `HeroRun` and `Session` API group, schemas, and REST semantics remain
  unchanged.
- `kubectl` workflows, RBAC policies, and Kubernetes API clients keep working
  without modification.
- Neither controller requires architectural changes.
