# Workflows Architecture

A workflow in auto-optimizer is a Kubernetes controller.
A `HeroRun` custom resource represents each run, and the controller drives that
resource from creation to a terminal outcome through standard reconciliation.

The goal is to adopt the Kubernetes controller model cleanly.
All workflow state and operation state live on `HeroRun`.
The controller keeps no shadow state in memory.
Speaking strictly through the Kubernetes API decouples the controller from
storage.
Auto-optimizer starts with `kube-apiserver` as the backing store, and can later
move storage to a backend such as Redis behind a Kubernetes aggregated API
server without changing the controller.

## Core rules

Seven invariants govern workflow design.

1. **`HeroRun` is the single source of truth for workflow and operation state.**
   The controller, the web UI, and the CLI all read and update run state through
   the Kubernetes API.
   An optional `run.json` disk export exists only for human inspection.
   The system never reads it.
2. **The controller keeps no shadow state in memory.**
   In-flight operation state and completed operation outcomes live on `HeroRun`,
   not in controller memory.
   Process memory holds only ephemeral handles to live background work so the
   controller can detect running goroutines or cancel them.
3. **Bulk payloads stay on the state volume.**
   Conversation transcripts, unabridged tool outputs, and the user-facing event
   log remain files on the persistent state volume rather than fields on
   `HeroRun`.
4. **Crash recovery comes from controller idempotency.**
   Every reconcile pass must be safe to repeat from the last persisted `HeroRun`
   state after a crash at any point.
   The system does not rely on graceful shutdown handlers or an event-replay log
   for correctness.
5. **Reconciliation returns quickly.**
   A reconcile pass never blocks on slow work such as sandbox startup or model
   turns, and it never polls in-process.
   Long-running operations record their state and outcome on `HeroRun` and wake
   the controller when they finish.
6. **Persisted state never holds credentials.**
   Authentication tokens and signing keys stay out of `HeroRun` and out of state
   files.
   The control plane holds secrets in memory or injects them through the portal.
7. **Prefer standard Kubernetes controller libraries.**
   Build on the Go standard library and standard Kubernetes controller machinery
   instead of adopting an external workflow engine.

## Controller model and `HeroRun` state

In a Kubernetes controller, each reconcile pass reads the current resource from
the API, compares desired state with observed state, performs one bounded unit
of work, and writes updated status back through the API.
Auto-optimizer follows this model for every run.

`HeroRun` separates immutable desired inputs from observed execution progress.

- **Spec** holds immutable desired inputs set when the scheduler creates the
  run, such as the target repository, the frozen goal definition, the base
  commit, and the run limits.
- **Status** holds all observed workflow and operation state, including the
  current phase, active and completed operation state, Kubernetes conditions,
  child resource references, progress and cost metrics, and the run outcome.

Standard Kubernetes optimistic concurrency protects status updates.
Every write carries the resource version that the controller read.
If another actor updated `HeroRun` concurrently, the API rejects the write with
a conflict error.
The controller discards its stale copy and requeues the run to reconcile from
the latest resource version.

### Disk exports for human inspection

The control plane can optionally write a snapshot of `HeroRun` to `run.json` on
the state volume after a status update.
It can also write timestamped copies to preserve a point-in-time timeline on
disk, because etcd regularly compacts older resource versions.

These exports exist strictly so an operator can inspect a run directory with
standard command-line tools.
Because an export is optional and written after the API update, it can be stale
or missing.
The controller, the web UI, and the CLI never read `run.json`.
They always read `HeroRun` through the Kubernetes client so every component sees
one authoritative state.

## Storage separation

Auto-optimizer divides run data strictly between control-plane state in the
Kubernetes API and bulk payloads on the persistent state volume.

- **`HeroRun` in the Kubernetes API.** Holds workflow phase, conditions,
  operation state and outcomes, child resource references, progress counters,
  costs, benchmark metrics, and pull request metadata.
- **Files on the state volume.** Holds the `messages.jsonl` conversation
  transcripts, unabridged tool execution outputs, the `events.jsonl` append-only
  event log, and optional human-readable `HeroRun` exports.

Two operational constraints require this split.

1. **Object size limits.**
   etcd caps individual objects at 1.5 megabytes.
   Conversation transcripts, prompts, compiler diagnostics, and tool outputs
   routinely exceed that limit in a single run and cannot live inside `HeroRun`.
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
The controller never reads `events.jsonl` to decide what to do next.

Kubernetes conditions on `HeroRun` status report high-level readiness and
milestones so operators and cluster tooling can observe progress natively with
`kubectl`.

## Run lifecycle

A run moves through five active phases before reaching a terminal phase.

- **`Pending`.** The controller ensures the workspace claim and initial sandbox
  exist with controller owner references pointing to `HeroRun`, records those
  references in status, and advances to `Preparing`.
- **`Preparing`.** The controller waits for the sandbox to become ready without
  blocking.
  Once ready, the controller binds the portal egress policy, ensures the
  repository clone is at the base commit, configures git identity, and advances
  to `Working`.
- **`Working`.** The controller drives the agent session across one or more
  asynchronous turns, obtaining or replacing the sandbox on demand when needed.
  When the session requests a pull request, the run advances to `Selecting`.
  When the session ends without requesting a pull request, the run enters
  `NoImprovement`.
- **`Selecting`.** The controller verifies the candidate, exports the candidate
  commits, pushes the branch to the forge, and advances to `OpeningPR`.
- **`OpeningPR`.** The controller checks for an existing pull request on the
  branch or opens a new one, records the pull request link, and enters
  `Succeeded`.
- **Terminal phases.** When a run reaches `Succeeded`, `Failed`, or
  `NoImprovement`, the controller stops reconciling the run and releases the
  active sandbox.

Each reconcile pass returns one of four outcomes to the controller runtime.

- **Done.** The run has reached a terminal phase and needs no further
  reconciliation.
- **Requeue immediately.** One bounded unit of work finished and the next step
  can proceed right away.
- **Wait for external changes.** The run is waiting on an external resource or
  an asynchronous operation.
  Kubernetes watches or internal wake signals enqueue the run when conditions
  change, while a requeue duration provides a safety backstop.
- **Error.** A transient infrastructure failure occurred.
  The controller runtime logs the error and retries with backoff.

## Crash recovery and idempotency

The control plane can receive `SIGKILL` or lose network access at any instant.
Rather than catching signals and flushing state during shutdown, every reconcile
step is idempotent with respect to the last saved `HeroRun` status.
Repeating a step after an external side effect has already succeeded produces
the same end state without duplicating work or corrupting the run.

### Example: crash between pushing a branch and saving status

Consider pushing candidate commits during `Selecting`.

1. `HeroRun` records phase `Selecting`, the run's deterministic branch name, and
   the candidate commit SHA.
2. The controller exports the candidate commits and pushes them to the forge.
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

The same principle applies across all phases.
Creating a workspace claim or sandbox reuses an existing resource owned by the
same `HeroRun`.
Binding portal egress policy is idempotent.
Opening a pull request reuses an existing open pull request on the run's
deterministic branch.

## Long-running work

Reconcile passes must return promptly so a bounded pool of controller workers
can serve many runs without stalling.
The controller handles two kinds of long-running work without blocking or
polling in-process.

- **External Kubernetes resources.**
  When waiting for a sandbox pod to become ready, the controller relies on
  Kubernetes watch events from owned resources to enqueue the run as soon as the
  status changes, backed by a periodic requeue safety net.
- **Asynchronous control-plane operations.**
  When an agent session turn executes against a model and sandbox, the turn runs
  in a background worker outside the reconcile pass.

Tracking asynchronous operations follows five rules so no state is hidden in
controller memory.

1. **Persist before launch.**
   Before launching a background worker, the controller records the active
   operation on `HeroRun` status so in-flight work is durable across restarts.
2. **Record outcomes on `HeroRun`, never in memory.**
   When the background worker finishes, it writes the completed operation state
   and outcome directly to `HeroRun` status and wakes the reconciler.
   The controller does not cache completed operation outcomes in memory.
3. **Consume and clear during reconciliation.**
   On the next reconcile pass, the controller reads the operation state from
   `HeroRun`.
   If the operation is still running in this process, the controller returns
   promptly with a requeue backstop.
   If the operation has completed, the controller applies the outcome to the
   run, clears the operation from `HeroRun`, and advances to the next step.
4. **Recover from restarts via durable transcripts.**
   If the control plane restarts while a turn is in flight, the background
   worker dies while `HeroRun` still records an active operation.
   When reconciliation resumes and finds no live local worker for that
   operation, the controller inspects the durable `messages.jsonl` session
   transcript on the state volume.
   If the transcript already contains the completed turn, the controller
   recovers the outcome, updates `HeroRun`, clears the operation, and advances.
   If the crash interrupted the turn mid-flight, the controller clears the stale
   operation and starts a fresh turn from the last completed turn in the
   transcript.
5. **Keep only ephemeral execution handles in memory.**
   The controller may hold in-process handles for currently running goroutines
   so it knows a worker is alive locally or can cancel it when a run ends.
   Losing all controller memory never loses workflow progress or completed
   operation outcomes.

## Retention and garbage collection

`HeroRun` owns the lifecycle of a run's cluster resources and state files.

- **Retention period.**
  While an opened pull request remains open, the control plane retains the
  `HeroRun` resource so the session can resume if review comments arrive.
  Once the pull request merges or closes, or when a run ends without opening a
  pull request, the control plane retains `HeroRun` for a configured retention
  window, such as 30 days, for operator inspection.
- **Deletion trigger.**
  A scheduled retention loop in the control plane deletes expired `HeroRun`
  resources.
  Operators can also delete a `HeroRun` manually at any time with `kubectl`.
- **Cascading Kubernetes cleanup.**
  Each child sandbox and workspace claim carries a controller owner reference to
  its `HeroRun`.
  Deleting `HeroRun` causes standard Kubernetes garbage collection to delete any
  remaining child sandbox and the workspace claim automatically.
- **State volume cleanup.**
  The control plane watches for `HeroRun` deletion and deletes the run's
  directory on the state volume, removing transcripts, tool outputs, event logs,
  and exported snapshots.

## Hermetic testing

Workflow unit tests run completely in memory with no network access, no external
binaries, and no real `kube-apiserver` or etcd.

An in-memory fake Kubernetes client stands in for `kube-apiserver`.
It tracks `HeroRun` and child resources in memory, enforces status subresource
updates, and checks resource versions for optimistic concurrency conflicts.
Tests combine this fake client with in-memory fakes for the forge, the sandbox
runtime, and the model client.
This setup exercises full phase transitions, conflict retries, asynchronous turn
completion, and crash-recovery scenarios quickly and deterministically.

## Storage evolution

Starting with `kube-apiserver` backed by etcd provides native watches,
optimistic concurrency, RBAC, `kubectl` inspection, and owner-reference garbage
collection.
It also avoids synchronization loops between separate stores.

At higher scale, three operational thresholds could justify moving `HeroRun`
storage off etcd.

1. **Write rate.**
   Hundreds of concurrent runs updating status after each step could generate
   heavy etcd write traffic, raising Raft latency and compaction load.
2. **Object size.**
   Even with transcripts and tool outputs on the state volume, richer goal
   definitions, operation records, or condition histories could grow toward
   etcd's 1.5 megabyte cap.
3. **Historical volume.**
   Retaining tens of thousands of completed runs across many repositories could
   strain cluster etcd storage and backup sizes.

When any of those thresholds matters, auto-optimizer can serve `HeroRun` through
a Kubernetes aggregated API server backed by a store suited to that workload,
such as Redis or files on the state volume.

Because the controller interacts exclusively through the Kubernetes API and
holds all workflow and operation state on `HeroRun`, this storage change stays
behind the API boundary.

- The `HeroRun` API group, schema, and REST semantics remain unchanged.
- `kubectl` workflows, RBAC policies, and Kubernetes API clients keep working
  without modification.
- The controller itself requires no architectural changes.
