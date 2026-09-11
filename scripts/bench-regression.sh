#!/usr/bin/env bash
# Run a short 3-scenario load-test matrix against a live server and gate it
# against benchmarks/baseline via cmd/benchcheck.
#
# Usage: scripts/bench-regression.sh <server-url> [admin-token]
#   BENCH_DURATION  per-scenario run length (default 20s, matching the committed
#                   baseline). 20s is a smoke duration — reseed longer on real
#                   hardware for production numbers.
set -euo pipefail

URL="${1:?usage: bench-regression.sh <server-url> [admin-token]}"
TOKEN="${2:-}"

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

for s in baseline 5pub-5sub 20-participants; do
  echo ">>> running scenario: $s"
  args=(--scenario "$s" --server-url "$URL" --duration "${BENCH_DURATION:-20s}" --output-dir "$OUT")
  if [ -n "$TOKEN" ]; then
    args+=(--admin-token "$TOKEN")
  fi
  go run ./cmd/loadtest "${args[@]}"
done

go run ./cmd/benchcheck -baseline benchmarks/baseline -current "$OUT" -tolerance 0.25
