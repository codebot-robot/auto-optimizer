# AGENTS.md

This file gives context and instructions to LLM coding agents working on
`auto-optimizer`.

## Project vision

`auto-optimizer` reads goals from a `.goals/` folder in a repository, runs
coding agents in Kubernetes sandboxes against a clone of that repository, and
sends pull requests that make progress on the goals. The canonical goal is a
performance benchmark. Goals can also be less concrete, such as a code quality
bar or a feature list.

Read [docs/design.md](docs/design.md) before you change anything. It is the
source of truth for the architecture, the decisions behind it, and the
roadmap. The repository is in an early phase, so the design describes work
that does not exist yet; build towards it, and update it in the same PR when
you have a reason to depart from it.

## Key principles for agents

- **The harness decides, the agent proposes.** Go code re-measures a
  candidate before a pull request is opened. Never move that decision into a
  prompt or trust a number an agent reports.
- **Sandboxes hold no credentials.** GitHub tokens and model keys are injected
  by the service-portals sidecar or used by the control plane. Never pass a
  secret into a sandbox pod spec, an environment variable the agent can read,
  or a log line.
- **Use upstream projects, and fix them upstream.** The sandbox API is
  kubernetes-sigs/agent-sandbox with its `sandboxd` runtime daemon. Egress and
  credentials come from gke-labs/service-portals. Workspace storage comes from
  gke-labs/in-cluster-storage. When one of them lacks something we need, say
  so in the issue or PR so it becomes an upstream issue. Do not add a
  workaround silently.
- **Go, with the standard library first.** Add a dependency only when it
  removes a lot of code. Use `any` instead of `interface{}`.
- **Server-rendered UI.** Pages are `html/template`. No JavaScript framework.
- **State is files.** Records live under one state directory as JSON and
  JSONL, with exactly one writer per file. Anything else is a rebuildable
  cache.
- **Structured logging.** Use `log := scoped.Log(ctx)`, pass the context
  down, and log with structured fields such as
  `log.Info("sandbox ready", "sandbox", name)` rather than formatted strings.
- **Testability.** Unit tests use the fake forge, the fake LLM, and a fake
  sandboxd client, with no network. End-to-end tests run on kind.

## Writing

Code comments, docstrings, commit messages, PR descriptions and docs should
read clearly to someone new to the code. Follow Google's
[Technical Writing One](https://developers.google.com/tech-writing/one) and,
for details, the
[Google developer documentation style guide](https://developers.google.com/style).
For doc comments, see [Go doc comments](https://go.dev/doc/comment).

The rules that matter most here:

- **Use active voice.** Say who does what: "the scheduler starts a run when
  the goal changes", not "a run is started when the goal is changed".
- **Write full sentences, one idea each.** Don't chain clauses with colons,
  semicolons and parenthetical asides.
- **Define a term before you use it,** and use the project's existing words:
  goal, run, session, sandbox, forge, portal, workspace. Don't coin shorthand.
- **Say what the code does and why,** not how it got that way.
- **Check all the prose in your change** before you send it. Start from the
  diff, and check every file that holds prose: docs such as `docs/design.md`
  and `README.md`, code comments, docstrings, proto and script comments, error
  and log messages, flag help, the commit message and the PR description.

## Development workflow

Maintainers file issues and assign them to a coding agent, which sends the PR.
Maintainers review and merge. Design changes land in `docs/design.md` in the
same PR as the code; GitHub issues hold the backlog, the design lives in the
repository.

- Solve only the specific issue. One idea per PR.
- Keep the PR small enough to review in one sitting. If an issue needs more,
  say so in the issue and split it.
- Do not check in binary files or large data files. Confirm this before you
  send a PR.
- Code that is forked from an upstream example (for example the session loop
  from agent-sandbox's `examples/sandboxed-tools`) keeps the upstream license
  header and names its origin in the package comment.
- Before pushing, read each commit message against its diff and remove any sentence that describes work not in that diff.

### Commit messages and PR descriptions

**The code and its commit history must stand on their own.** PR descriptions do not become part of the repository; commit messages do. Anything a future reader needs to understand a change belongs in the commit messages, and the PR description should simply repeat them.

- **Subject line:** `area: imperative summary`, about 72 characters at most, where `area` is the package or component touched (for example `session`, `sessionapi`, `forge`, `sandbox`, `goals`, `web`, `api/session`, `dev`, `docs`).
- **Body: explain why, not just what.** Include whatever the diff alone doesn't make obvious:
  - **Bug fixes:** the symptom, the root cause, why the change fixes it, and the test that reproduces it.
  - **Design changes:** the approach taken, and the alternatives considered and why they were rejected.
  - **Performance changes:** the numbers, how they were measured, and in what configuration.
  - **Review answers:** if a reviewer had to ask, the answer probably belongs in the message.
- **One logical change per commit**, each building and passing tests where practical (for example: proto changes, then implementation, then tests or CI).
- **Keep messages true to the final code.** When review changes the approach, make sure the commit messages describe what the code does now, not the history of attempts. Either amend the original commit, or add `fixup!` commits and leave them in place: do not squash them. The fixups are useful history, because code that needed many rounds of tweaking is also the code most likely to still hold bugs, and the record shows where that was. A `fixup!` commit whose diff speaks for itself needs no body; one that changes behaviour or reverses a decision gets a body saying what changed and why. Whichever route you take, never leave a message describing code that was later removed.
- **Reference issues** in the commit body: `Fixes #N` for the commit that completes an issue, `Part of #N` otherwise.
- **PR description = the commit messages**, in order, for multi-commit PRs. If a reviewer would need information that isn't in the commits, add it to the commits rather than only to the PR description.

### Commands

The project uses the gke-labs `ap` tool. Run it with `go run`:

- `go run github.com/gke-labs/gke-labs-infra/ap@latest generate //...`:
  regenerate code and format.
- `go run github.com/gke-labs/gke-labs-infra/ap@latest test`: run unit tests.
- `go run github.com/gke-labs/gke-labs-infra/ap@latest e2e`: run end-to-end
  tests on kind.
- `go run github.com/gke-labs/gke-labs-infra/ap@latest lint`: static
  analysis.

Until the `.ap/` configuration exists in this repository, run `gofmt -l .`,
`go vet ./...`, and `go test ./...` instead, and add the `.ap/` configuration
as part of the first code milestone.

**Reminder**: run at least `ap generate //...` and `ap lint` before sending a
PR, and preferably `ap e2e` as well. The CI presubmit `ap-verify-generate`
fails if generated code is stale.
