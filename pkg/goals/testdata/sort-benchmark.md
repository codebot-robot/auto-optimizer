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
