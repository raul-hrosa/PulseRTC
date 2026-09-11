#!/usr/bin/env bash
# Full load-test matrix runner for PulseRTC.
#
# Usage:   scripts/bench.sh <server-url> [admin-token]
# Example: scripts/bench.sh http://localhost:8090
#          scripts/bench.sh http://localhost:8090 "$ADMIN_TOKEN"
#
# Pass an http(s):// URL — the loadtest upgrades it to ws:// for the signaling
# socket and uses it as-is to poll the server's /metrics.json (a ws:// URL would
# break that poll, leaving cpu/heap stats empty).
#
# Runs every scenario preset in internal/loadtest/scenario.go against the given
# server, writing one <scenario>-<ts>.json result per scenario into
# benchmarks/run-<ts>/.
#
# Env overrides:
#   BENCH_DURATION   e.g. "20s" — overrides each preset's duration (quick runs).
set -euo pipefail

URL="${1:?server url, e.g. http://localhost:8090}"
TOKEN="${2:-}"

OUT="benchmarks/run-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"

SCENARIOS=(baseline 1pub-5sub 5pub-5sub 1pub-10sub 10pub-10sub 20-participants ramp-up join-leave)

for s in "${SCENARIOS[@]}"; do
  echo "=== $s ==="
  args=(--scenario "$s" --server-url "$URL" --output-dir "$OUT")
  if [[ -n "$TOKEN" ]]; then
    args+=(--admin-token "$TOKEN")
  fi
  if [[ -n "${BENCH_DURATION:-}" ]]; then
    args+=(--duration "$BENCH_DURATION")
  fi
  go run ./cmd/loadtest "${args[@]}"
done

echo
echo "wrote $OUT"
