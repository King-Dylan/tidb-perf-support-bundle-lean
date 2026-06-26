#!/usr/bin/env bash
set -euo pipefail

# Run this on the EC2 instance before spending credits on a full benchmark.
# It validates the Python entrypoints and rebuilds the Go load generator so the
# final run cannot accidentally use an old binary that lacks per-event metrics.

cd "$(dirname "$0")"

echo "== Python syntax check =="
python3 -m py_compile \
  build_reuse_events_from_stats.py \
  generate_go_workload.py \
  customer_event_report.py \
  run_go_loadgen_fleet.py

echo "== Go toolchain check =="
if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: go is not installed or not on PATH. Install/use Go on EC2 before running the final benchmark." >&2
  exit 1
fi

echo "== Rebuild go-loadgen =="
(
  cd go-loadgen
  gofmt -w main.go
  go build -o go-loadgen-linux-amd64 .
)

echo "== Required final-run fields present =="
grep -q "bundles_by_300ms" go-loadgen/main.go
grep -q "bundles_by_350ms" go-loadgen/main.go
grep -q "bundles_by_500ms" go-loadgen/main.go
grep -q "sql_score60_ms" go-loadgen/main.go
grep -q "event-offset" go-loadgen/main.go
grep -q "event-stride" go-loadgen/main.go
grep -q "allow-event-reuse" generate_go_workload.py
grep -q "require-hot-fields" generate_go_workload.py

echo "OK: bundle preflight passed. Next step: build a tiny source pool/workload and generate a sample report before the full run."
