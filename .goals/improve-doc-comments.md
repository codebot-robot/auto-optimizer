---
verify: 'test -z "$(gofmt -l .)" && go vet ./...'
budget:
  maxDuration: 15m
---
# Improve Go doc comments

Inspect Go packages under `pkg/` and `cmd/` for missing, inaccurate, or stale
Go doc comments on package declarations and exported types, functions, and
methods. Pick one package to improve and keep the diff to at most 100 lines so
the pull request stays small and easy to review.

Follow Go doc comment conventions and the writing rules in `AGENTS.md`. Use
active voice, write full sentences, and explain what the code does and why. Do
not change code behavior. Do not edit generated files such as
`zz_generated.*.go` or `*.pb.go`.

If you update doc comments, commit your changes and call `open_pull_request`.
If every package and exported symbol under `pkg/` and `cmd/` already has
accurate doc comments, call `finish` with a brief summary.
