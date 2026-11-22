# Workflows Architecture

A workflow in auto-optimizer is a Kubernetes controller. Its state lives in the
Kubernetes API as a `HeroRun` custom resource, and the `kube-apiserver` is the
authoritative backing store to start with.

In a standard Kubernetes controller, the control loop watches for changes,
reads a custom resource from the API server, reconciles desired and observed
state, and writes the updated status back to the API server. In auto-optimizer,
the workflow runner executes this exact reconciliation pattern: `Run`
reconciles the run's `HeroRun` resource fetched from the `kube-apiserver`,
performs one unit of execution, and the caller writes the updated resource
status back to the `kube-apiserver`.

This controller model guides the design throughout:
- **`Run` is `Reconcile`:** Calling `Run` on the workflow runner corresponds to
  a controller's `Reconcile`. Each invocation performs one discrete unit of work
  to drive the run toward its desired state, updating `HeroRun` in memory and
  returning execution directives.
- **The saved state is the `HeroRun` Custom Resource:** The workflow state
  object is the `HeroRun` custom resource in the `kube-apiserver`. The
  reconciler loads the resource from the API server, modifies its spec or status
  during execution, and saves it back via standard Kubernetes status updates.
- **`Result` is `reconcile.Result`:** The workflow runner's result directly
  mirrors controller-runtime's `reconcile.Result`. It instructs the caller how
  and when to invoke `Run` again (whether the workflow is terminal, needs an
  immediate requeue, or should wait for an external condition).
- **Crash recovery is reconciliation idempotency:** Like any well-behaved
  Kubernetes controller, `Run` is idempotent: calling it again from the same
  persisted `HeroRun` state in the API server is safe, even when a previous
  invocation crashed immediately after an external side effect (such as pushing
  a git commit). There is no event-replay log; the persisted `HeroRun` resource
  is the sole authoritative record.
- **Bulk payloads stay on the state volume:** Conversation transcripts
  (`messages.jsonl`), tool call outputs, and the user-facing event log
  (`events.jsonl`) remain on the persistent state volume as files. They do not
  live in the `HeroRun` custom resource because etcd caps object sizes at 1.5 MB
  and streaming turn-by-turn logs would cause severe write churn.

## Design rules

The design follows seven core rules:
1. **The `HeroRun` custom resource in `kube-apiserver` is the workflow state.**
   The workflow runner reads and updates `HeroRun` directly in the Kubernetes API.
2. **Bulk payload state is files.** Transcripts, large tool outputs, and the
   event log live on the state volume as files. No external database is used.
3. **Fix resumption after a crash at any point.** Do not add graceful-shutdown
   handling.
4. **Standard library and controller-runtime first.** Do not adopt an external
   workflow library.
5. **The saved state never holds a credential.** All authentication tokens stay
   in memory or are injected by the portal sidecar.
6. **Saving updates `kube-apiserver` after each call to `Run`.** An export
   option writes a disk snapshot (`run.json`) strictly for human inspection.
   The web UI and CLI read `HeroRun` through the client and must not read the
   export.
7. **Run should return quickly.** The design does not enforce a time limit. For
   a long-running operation, `Run` records the operation's ID in the state and
   returns. A later call to `Run` looks the operation up by that ID. `Run` does
   not poll the operation in-process.

## Workflow state as the HeroRun custom resource

The design establishes `HeroRun` as the single authoritative state object for a
run.

Previously, the repository maintained two distinct types: `HeroRun` in the API
server for cluster visibility and sandbox ownership, and `state.Run` in
`run.json` on disk for execution bookkeeping. Maintaining dual representations
creates schema duplication, translation overhead, and field drift.

Under this architecture, they become one type: `HeroRun`.
- **`HeroRunSpec`** defines desired and immutable run parameters set when the run
  is scheduled: target repository URL, goal name, frozen goal definition, base
  commit SHA, deadline timestamp, and budget constraints.
- **`HeroRunStatus`** defines observed execution progress: current phase, phase
  transition timestamps, conditions, active operation identifier, child sandbox
  reference, workspace claim name, turn counts, costs, benchmark metrics,
  candidate commit SHA, branch name, pull request metadata, summary, and error
  details.
- Obsolete publisher fields, such as `ObservedAt` (which was recorded by the
  background summary publisher that this design eliminates), are removed.
- The legacy `state.Run` struct is phased out.

### Role of run.json and existing runs on disk

With `HeroRun` in the `kube-apiserver` serving as the source of truth:
1. **`run.json` is strictly an export for a person to read:**
   `run.json` on disk is not a database and is not read by the controller, the
   CLI, or the web UI. Because the export is written after the save and is
   optional, it can be stale or missing. If the web UI or CLI read `run.json`,
   they would risk displaying a state that the controller does not have.
   Therefore, **the web UI and CLI read `HeroRun` exclusively through the
   Kubernetes client**. The disk export exists solely for a person inspecting a
   run directory with command-line tools (`cat`, `jq`, text editors).
2. **Handling existing runs on disk:**
   On orchestrator startup or during `Resume`, the orchestrator inspects the
   state directory for legacy runs. If a run exists in `run.json` on disk but
   has no corresponding `HeroRun` custom resource in the `kube-apiserver`, the
   orchestrator imports it by creating the `HeroRun` object in `kube-apiserver`
   with spec and status populated from the file. Once imported, `HeroRun` in the
   `kube-apiserver` is the sole source of truth.

## Storage separation: what stays on the state volume and why

The architecture maintains a strict division between control-plane state in
`kube-apiserver` and bulk data on the state volume (ObjectFS):

- **In `kube-apiserver` (etcd):**
  The `HeroRun` custom resource holds all workflow execution state: spec inputs,
  current phase, phase transition timestamps, conditions, child sandbox
  references, active operation ID, turn counts, costs, and summary metrics.
- **On the state volume (ObjectFS files):**
  The run directory (`repos/<host>/<owner>/<name>/runs/<run-id>/`) holds:
  - `sessions/<session-id>/messages.jsonl`: Full LLM conversation history and
    transcripts.
  - `sessions/<session-id>/tools/<n>.json`: Unabridged tool execution payloads
    (compiler outputs, diffs, large file contents).
  - `events.jsonl`: Append-only, chronological event stream for UI rendering and
    detailed audit logs.
  - `run.json`: Optional exported snapshot of the `HeroRun` object for human
    inspection.

This separation is necessary for two critical reasons:
1. **etcd 1.5 megabyte object size limit:** Agent conversation transcripts,
   model context prompts, compiler diagnostics, and tool outputs easily reach
   several megabytes in a single run. These payloads cannot fit in an etcd
   resource.
2. **etcd write churn and write amplification:** LLM turn interactions generate
   continuous streaming events and large chunked outputs. Persisting every
   intermediate token or verbose tool response to etcd would trigger excessive
   Raft consensus rounds, increase compaction frequency, and degrade cluster
   responsiveness. The state volume is designed for unmetered, high-throughput,
   append-only file I/O.

## Workflow runner interface

`RunWorkflow` is the concrete runner that drives a run through its
reconciliation loop. Each invocation of `Run(ctx)` executes one unit of work and
returns a `Result`:
- **"Done":** When the run enters a terminal phase (`Succeeded`, `Failed`, or
  `NoImprovement`), `Run` reports done. The caller saves the final status to
  `kube-apiserver` and halts execution.
- **"Call me again":** When a unit of work finishes and more work is ready
  immediately, `Run` requests an immediate requeue. The caller saves the
  updated status to `kube-apiserver` and invokes `Run` again.
- **"Call me again when something external changes":** When execution is
  waiting on an external condition (such as a Sandbox pod becoming ready),
  `Run` returns a requeue duration. The caller saves status and yields the worker
  thread. An informer watching Sandbox resources enqueues the run immediately
  when conditions change, while the requeue duration provides a safety backstop.
- **Error:** When an infrastructure error occurs, `Run` returns an error. The
  caller saves status if modified, logs the error, and retries with backoff.

### Saving state and optimistic concurrency

With `kube-apiserver` as the backing store, saving state updates the `HeroRun`
custom resource via standard Kubernetes status updates.
- **Optimistic locking:** Kubernetes provides optimistic concurrency control via
  `metadata.resourceVersion`. If another actor modifies the `HeroRun` object
  concurrently, the status update returns a conflict error (`409 Conflict`). In
  accordance with standard controller-runtime behavior, the reconciler aborts
  the current turn and requeues the request to fetch the latest resource
  version from the API server cache and re-run reconciliation.
- **History preservation option:** In etcd, older resource versions are
  regularly compacted and purged. To preserve full point-in-time state history
  for human inspection and debugging, the history option writes a timestamped
  export copy to disk before or after each save (for example,
  `repos/.../runs/<run-id>/history/run.<timestamp>.json`). This gives operators
  an immutable timeline of state transitions on disk without burdening etcd.

### Phase reconciliation lifecycle

The phases of a run map directly onto `Run`:
- **Phase `Pending`:** `Run` ensures the workspace persistent volume claim
  exists, recording its name in status, advances phase to `Preparing`, and
  requeues immediately.
- **Phase `Preparing`:** `prepareSandbox` binds the portal egress policy, ensures
  the repository clone is present at the base commit or reuses the workspace
  volume, and sets git identity. If an upfront sandbox exists, `Run` waits for
  readiness (returning a requeue duration when unready) and executes
  `prepareSandbox`, advancing to `Working`.
- **Phase `Working`:** `Run` drives the agent session. The session obtains and
  prepares its Sandbox on demand. For long-running turns, `Run` records the turn
  operation ID in `ActiveOperation` and returns promptly. When a turn completes,
  `Run` updates turn counts and cost, clearing `ActiveOperation`. If the session
  requests a pull request, `Run` records pull request details and advances to
  `Selecting`. If the session finishes without a pull request, `Run` advances to
  `NoImprovement`.
- **Phase `Selecting`:** `Run` executes candidate verification tests, exports
  candidate commits into a packfile, pushes the branch to the remote forge, and
  advances to `OpeningPR`. Replacement sandboxes are prepared on demand if
  needed.
- **Phase `OpeningPR`:** `Run` queries the forge for an existing pull request on
  the branch or opens a new pull request, recording the pull request URL, and
  advances to `Succeeded`.

Several fields and in-memory maps on main are eliminated by this abstraction:
- `ReadyDeadline` is removed. Readiness timeouts are evaluated directly against
  the run deadline or caller context rather than tracked in persistent state.
- `PortalBoundAt` is removed. Because `prepareSandbox` binds portal egress policy
  idempotently whenever a sandbox is created, recording a binding timestamp in
  run state is unnecessary.
- The `turnOutcomes` map on `Orchestrator` is removed. Turn outcomes are
  recorded directly into status upon turn completion.
- The `activeRuns` and `workerCancels` maps on `Orchestrator` are reduced to an
  in-memory execution cache. The persisted `ActiveOperation` in `HeroRun.Status`
  is the authoritative record of work in flight, and an orchestrator restart
  rebuilds in-memory execution handles directly from that operation ID.

## State persistence and crash resilience

Crash recovery follows standard Kubernetes controller idempotency.

### Worked example: crash between side effect and save

Consider pushing candidate commits to a remote branch in phase `Selecting`:

1. **State before call:**
   `HeroRun` in `kube-apiserver` contains `Status.Phase: "Selecting"`, branch
   `ao/sort-benchmark/run-1`, and candidate commit `d3b0738a9`.
2. **Caller calls `Run`:**
   The reconciler receives `HeroRun` and invokes `Run(ctx)`.
3. **Side effect occurs:**
   The workflow executes `forge.PushPackfile`. The push succeeds over HTTPS to
   GitHub, updating `refs/heads/ao/sort-benchmark/run-1` on the remote
   repository to commit `d3b0738a9`.
4. **State mutated in memory:**
   The workflow updates its internal state, setting `Status.Phase: "OpeningPR"`,
   and prepares to return an immediate requeue.
5. **The crash:**
   Before the caller can call status update to save the new status to
   `kube-apiserver`, the pod receives `SIGKILL`. In `kube-apiserver`, `HeroRun`
   still records `Status.Phase: "Selecting"`.
6. **Resumption:**
   The control plane restarts. The reconciler's informer watches `HeroRun`
   resources and receives the object from `kube-apiserver`, which still
   reflects `Status.Phase: "Selecting"`.
7. **Next call to `Run`:**
   The reconciler invokes `Run(ctx)` again from the loaded state.
8. **Why `Run` is safe:**
   `Run` enters the `Selecting` handler and calls `forge.PushPackfile` with the
   same branch and commit tip. The remote forge advertises that the remote ref
   is already at `d3b0738a9`. Git recognises that the push is up to date and
   returns success immediately without error or duplicate side effects.
9. **State saved:**
   `Run` sets `Status.Phase: "OpeningPR"` and returns an immediate requeue. The
   caller successfully updates `HeroRun.Status` in `kube-apiserver`.
10. Execution advances to `OpeningPR`.

## Handling long-running work

`Run` returns quickly without blocking on long operations or polling in-process:

1. **Operation ID tracking:**
   For a long-running operation, such as an agent session turn in phase
   `Working`, `Run` does not block for the duration of the turn. Instead, `Run`
   initiates the turn, records the operation ID in `Status.ActiveOperation` (for
   example, `session-1:turn-3`), and returns immediately with a requeue duration
   as a backstop.
2. **Where the operation runs:**
   The session turn executes in a background worker goroutine managed by the
   session harness in the control plane. It calls the model and executes tool
   calls against the sandbox asynchronously.
3. **Subsequent calls to `Run`:**
   When the reconciler calls `Run` again (woken by `Orchestrator.Notify` when
   the turn worker signals completion, or by the requeue backstop), `Run` looks
   up the operation by its ID:
   - If the turn is still running in the background, `Run` returns a requeue
     duration without polling in-process.
   - If the turn has finished, `Run` collects the outcome from the worker,
     updates turn count and cost, clears `ActiveOperation`, and returns an
     immediate requeue to continue to the next step.
4. **Resumption when the control plane restarts:**
   If the control plane crashes while a turn is running, the background
   goroutine terminates, but `ActiveOperation` remains in `HeroRun.Status` in
   the `kube-apiserver`. When the system restarts, the next call to `Run`
   inspects `Status.ActiveOperation` and consults the session transcript in
   `messages.jsonl` on the state volume:
   - If `messages.jsonl` contains the completed turn (the assistant message and
     tool results were persisted to disk before the crash), `Run` reconstructs
     the outcome, updates status, clears `ActiveOperation`, and advances.
   - If `messages.jsonl` shows the turn was interrupted mid-flight, `Run`
     detects that the worker is dead. Because completed turns are preserved on
     disk in `messages.jsonl`, `Run` clears the stale operation ID and launches
     a fresh turn worker to resume from the last completed turn.
5. **In-memory state as an ephemeral cache:**
   Any in-memory worker handles or cancellation functions are strictly an
   execution cache. After a restart, the control plane reconstructs its worker
   tracking directly from the persisted `Status.ActiveOperation` ID.

## Event log and status reporting

- **Event log (`events.jsonl`):**
  The event log on the state volume is an append-only audit trail for user
  visibility and web UI rendering. The caller compares `HeroRun.Status` before
  and after each call to `Run`:
  - When `Phase` changes, the caller detects the transition and appends a phase
    change event to `events.jsonl`.
  - When benchmark measurements appear, the caller appends a measurement event.
  - When an error is recorded, the caller appends an error event.
  During `Working`, tool-call and message events are appended directly by the
  background turn worker as they execute. The workflow never reads from
  `events.jsonl` to determine what to execute next.
- **Conditions on HeroRun:**
  Standard Kubernetes conditions (`metav1.Condition`) are recorded directly on
  `HeroRun.Status.Conditions` (such as `Ready`, `SandboxReady`, `Selected`,
  `PROpened`), enabling external cluster tooling and `kubectl wait` to track
  progress natively.

## Retention and garbage collection

Because `HeroRun` objects hold the authoritative copy of a run's state and own
its child resources, their lifecycle and cleanup follow strict rules:

- **How long a finished `HeroRun` is kept:**
  When a run reaches a terminal phase (`Succeeded`, `Failed`, or
  `NoImprovement`), its `HeroRun` resource is retained:
  - If a pull request was opened, `HeroRun` is retained for as long as the pull
    request remains open, so that the session can resume if review comments
    arrive.
  - When the pull request is merged or closed, or if the run completed without
    opening a pull request (or failed), the `HeroRun` is retained for a
    retention window (such as 30 days) to allow developers and operators to
    inspect outcomes.
- **Who deletes it:**
  A scheduled retention loop in the control plane (`ao serve`) deletes
  expired `HeroRun` resources after their retention window passes. An operator
  or user can also delete a `HeroRun` manually via `kubectl delete herorun <name>`.
- **What deletion does to child resources and files:**
  - **Child Kubernetes resources:** `HeroRun` is the owner of its `Sandbox`
    and its workspace `PersistentVolumeClaim` (both carry owner references
    naming the `HeroRun` with `controller: true`). When the `HeroRun` is
    deleted, standard Kubernetes cascading garbage collection automatically
    deletes the child Sandbox (if not already deleted at terminal phase) and
    deletes the workspace claim.
  - **Files on the state volume:** The control plane watches for `HeroRun`
    deletion. When a `HeroRun` is deleted, the control plane deletes the run's
    directory on the state volume (`repos/<host>/<owner>/<name>/runs/<run-id>/`),
    removing `messages.jsonl`, `tools/`, `events.jsonl`, and any exported
    snapshot files.

## Testing

The project's unit tests run hermetically with fakes and no network access:

- **Fake client for kube-apiserver:**
  In unit tests of `RunWorkflow.Run` and the orchestrator, controller-runtime's
  in-memory fake client (`sigs.k8s.io/controller-runtime/pkg/client/fake`,
  configured with `HeroRun` in the scheme and status subresource) stands in for
  the `kube-apiserver`.
- **Hermetic execution:**
  The fake client implements `client.Client` entirely in memory, tracking
  objects, updating status subresources, and validating resource versions
  without launching a `kube-apiserver` binary, without etcd, and without opening
  network ports.
- **Combined test doubles:**
  Tests combine the fake client with the existing fake forge (`forge.NewFake()`),
  fake agents clientset (`fakeagents.NewSimpleClientset()`), and fake LLM client
  (`llm.FakeClient`), verifying complete workflow reconciliation without
  external dependencies.

## Future evolution: when to add a second store or an aggregated API server

Starting with `kube-apiserver` as the backing store for `HeroRun` provides
immediate architectural benefits: standard controller patterns, native
controller-runtime client caching, built-in RBAC, and zero synchronization loops
between disparate stores.

However, as auto-optimizer scales, specific operational thresholds would motivate
introducing a secondary store or an aggregated API server:

1. **High write rate and etcd churn:**
   If hundreds of concurrent runs execute simultaneous agent turns, saving
   status updates after every step could generate significant write traffic to
   etcd, increasing Raft consensus latency and etcd compaction load.
2. **Object size pressure:**
   While large transcripts and tool outputs are already kept on the state
   volume, if goal specifications, candidate commit metadata, or detailed
   condition histories expand and threaten etcd's 1.5 MB object cap, offloading
   run state from etcd becomes necessary.
3. **High run volume and cluster retention:**
   If repositories accumulate tens of thousands of historical runs, storing all
   completed run records in etcd would strain cluster storage and backup
   operations.

### Aggregated API Server

If these thresholds are reached, auto-optimizer can introduce a Kubernetes
aggregated API server using `k8s.io/apiserver` registered under the
`auto-optimizer.labs.gke.io` API group.

With an aggregated API server:
- The resource schema, API group, and URL path remain the same.
- `kubectl` commands (`get`, `describe`, `watch`) and controller-runtime
  clients continue to interact with `HeroRun` through standard Kubernetes REST
  requests.
- The reconciler, workflow runner, and RBAC rules remain the same.
- The storage implementation behind the REST handler changes from etcd to
  ObjectFS files or a specialized store.

## Migration roadmap

The orchestrator moves to the resumable workflow abstraction through an ordered
sequence of small PRs. The store changes in one step up front, so state is never
split across two stores, and each subsequent PR moves one phase of the live
reconciler onto `Run`:

1. **PR 1: Switch the store for all phases to `HeroRun` in `kube-apiserver` (about 220 lines)**
   Aligns the `HeroRun` custom resource type in `api/herorun/v1alpha1` with the
   complete workflow state schema (adding execution fields to `HeroRunStatus`
   including `ActiveOperation`, and inputs to `HeroRunSpec`), regenerates
   deepcopy methods and CRD manifests, and updates the existing reconciler in
   `pkg/run/orchestrator.go` to read and write `HeroRun` in `kube-apiserver`
   across all phases as the single source of truth. Imports any legacy runs
   found on disk on startup into `kube-apiserver`. Keeps `run.json` only as an
   optional disk export for human inspection. Tested in `pkg/run` with
   `controller-runtime/pkg/client/fake`. Changes the store in one step,
   preventing dual-store divergence.
2. **PR 2: Workflow runner skeleton and live adoption of Phase Pending (about 190 lines)**
   Introduces `RunWorkflow` and `Result`. Implements workspace claim creation in
   `RunWorkflow.Run` for `PhasePending`. The live reconciler delegates
   `Pending` reconciliation directly to `RunWorkflow.Run` and updates
   `HeroRun.Status` via status updates. Main uses `Run` for all pending runs.
3. **PR 3: Live adoption of Phase Preparing (about 200 lines)**
   Implements sandbox readiness check with requeue durations and `prepareSandbox`
   invocation in `RunWorkflow.Run` for `PhasePreparing`. The live reconciler
   delegates `Preparing` reconciliation to `RunWorkflow.Run`. Main uses `Run`
   for all preparing runs.
4. **PR 4: Live adoption of Phase Working (about 210 lines)**
   Implements turn worker launching, `ActiveOperation` tracking, quick return
   without in-process polling, and turn completion collection in
   `RunWorkflow.Run` for `PhaseWorking`. The live reconciler delegates `Working`
   reconciliation to `RunWorkflow.Run`. Main runs session turns through `Run`.
5. **PR 5: Live adoption of Phase Selecting (about 200 lines)**
   Implements candidate verification, commit export, and branch pushing in
   `RunWorkflow.Run` for `PhaseSelecting`, using on-demand sandbox provisioning.
   The live reconciler delegates `Selecting` reconciliation to `RunWorkflow.Run`.
   Main runs selection through `Run`.
6. **PR 6: Live adoption of Phase OpeningPR and terminal transitions (about 190 lines)**
   Implements pull request discovery or creation in `RunWorkflow.Run` for
   `PhaseOpeningPR`, alongside terminal phase transitions with synchronous
   `HeroRun` status updates. The live reconciler delegates `OpeningPR` to
   `RunWorkflow.Run`.
7. **PR 7: Live reconciler unification (about 170 lines)**
   Simplifies the reconciler's top-level `Reconcile` method into a single
   invocation of `RunWorkflow.Run`, followed by `HeroRun` status update and
   event logging, removing the individual phase delegation methods.
8. **PR 8: Obsolete field and in-memory map cleanup (about 150 lines)**
   Removes obsolete fields (`ReadyDeadline`, `PortalBoundAt`) and in-memory
   maps (`turnOutcomes`, `activeRuns`, `workerCancels`) from `Orchestrator` now
   that work in flight is tracked purely by `ActiveOperation` on
   `HeroRun.Status`.
