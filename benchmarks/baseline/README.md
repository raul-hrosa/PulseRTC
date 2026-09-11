# Committed load-test baseline

These JSON files are the version-controlled reference numbers for the load-test
matrix. `cmd/benchcheck` (Task E2) compares a fresh run against them and fails
CI (Task E3) when a scenario regresses past its threshold.

Files here: `baseline.json`, `5pub-5sub.json`, `20-participants.json` — the
`.summary` block of `internal/loadtest`'s `Result` for each scenario.

## How these were generated

A short run (`BENCH_DURATION=20s`) of `scripts/bench.sh` against a server started
inside a single `golang:1.24` container with:

```text
PULSERTC_AUTH_ENABLED=false PULSERTC_METRICS_PUBLIC=true SFU_NAT_1TO1_IP=127.0.0.1 PORT=8090
```

The regression gate (`scripts/bench-regression.sh` / `.github/workflows/bench.yml`)
runs the **same 20s** per scenario, so a fresh run is compared against this
baseline at a matching duration. 20s is a *smoke* duration — reseed longer on
real hardware for production numbers.

so `/metrics.json` is served without a bearer and the SFU advertises a
loopback ICE candidate.

Caveat: in the CI/container environment ICE/DTLS between the synthetic clients and
the SFU does not fully establish, so RTP counters and bitrate/packet-loss stay at
zero. The **connection-time, media-start-time, server CPU% and heap** figures are
real and are what the regression gate keys on.

## Regenerate against a healthy server

`cmd/benchcheck` keys each result by the `scenario` field inside the JSON, not
by filename, so the timestamped names that `loadtest`/`bench.sh` emit
(`baseline-20260903T185936Z.json`) match the committed `baseline.json` fine.
Renaming is optional — done here only to keep the directory tidy.

```
scripts/bench.sh http://localhost:8090            # or the `bench` CI job (E3)
for s in baseline 5pub-5sub 20-participants; do
  cp "$(ls -t benchmarks/run-*/$s-*.json | head -1)" "benchmarks/baseline/$s.json"
done
```

Then re-run the full matrix (all 8 scenarios) when you want a complete refresh;
only the three scenarios above are tracked as the gate today.
