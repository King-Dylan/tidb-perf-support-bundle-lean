# Customer Event-Level Report Patch

This copy adds the reporting fields needed for the next Intuit benchmark validation run. It is intended to be validated on EC2 before a full-cost run.

## SE addendum (report parity + SQL-only)

On top of the changes below, the report and Go output were extended to match the
v10 customer report exactly and to address the review feedback:

- `code/customer_event_report.py`
  - Latency is benchmark-harness event wall-clock (timer before fan-out, stop at 60th / 65th result), NOT SQL-only.
  - Adds an **SQL-only (DB-side) event latency** table side by side with wall-clock, when the Go output provides it (graceful: hidden if absent, so the report still runs on an un-rebuilt binary).
  - SLA and Event Latency are now broken out **All / Normal / Hot-key** (matching the v10 fault-tolerance view).
  - Test Shape is populated from the run's `run_shape` (target EPS, duration, connections, processes, prepared statements, max execution time).
  - Title and EPS/duration derive from the run; no hardcoded "1000 EPS".
  - Errors clearly if `event_results` are missing (reminds to run with `--omit-event-results=false`).
  - Reads run-shape keys from the result top level (target_event_eps, duration_seconds, connections, prepare_all, max_execution_time_ms), with a nested `run_shape` fallback, so Test Shape populates on real go-loadgen output.
  - Guards against worker-pool's unset `bundles_by_*` (all zero): the Event-Level SLA and Average Bundles tables footnote instead of rendering a fake all-zero "catastrophic" result. Fanout-mode runs are unaffected.
- `code/go-loadgen/main.go`
  - Per event, also records **`sql_score60_ms` / `sql_full65_ms`** (the 60th / 65th order statistic of the per-bundle SQL execution times), so the report can show DB-side vs wall-clock side by side. `-1` when fewer than 60 / 65 bundles succeed, matching the wall-clock semantics.
  - NOTE: requires `gofmt -w main.go && go build` on EC2 (the struct-literal field alignment is intentionally left for gofmt).

## What Changed

- `code/generate_go_workload.py`
  - Keeps the 8-field event binding set in each generated workload event.
  - Adds workload-level stats for generated rows, unique source events, unique full binding sets, and max reuse.
  - Adds bundle routing metadata for runtime, pre-agg, and serving bundles.
  - Carries forward the hot-key profile, including hot values and row counts, when present in the source event sample.
  - Fails fast by default if the requested workload would cycle/reuse the source event pool.
  - Requires hot-key events for all 8 key fields by default when `--hot-event-pct` is non-zero.
  - Adds `--allow-event-reuse` only for smoke tests where cycling is intentional.
  - Can compute workload size from `--target-event-eps` and `--duration`, so a 5-minute 1000 EPS run automatically generates 300,000 events and a 10-minute run generates 600,000.

- `code/go-loadgen/main.go`
  - Adds `--event-offset` and `--event-stride` so multiple load-generator processes can walk through a shared workload without all starting at row 0.
  - Adds per-event counts for bundles returned by 300ms, 350ms, and 500ms.
  - Keeps raw `event_results` when run with `--omit-event-results=false`.
  - Adds `over_300` to bundle/runtime summaries.
  - The new per-event cutoff counters are implemented for `event-fanout` and `conn-fanout`, which are the relevant high-concurrency modes. Avoid `worker-pool` for the final customer run unless it is separately patched and validated.

- `code/run_go_loadgen_fleet.py`
  - Passes `--event-offset`, `--event-stride`, and `--omit-event-results=false` to each remote Go process.
  - Fetches each remote result JSON back into the local `results/` folder.
  - Computes `events_total` from `--target-event-eps` and `--duration` in steady mode, and fails if a manually supplied `--events-total` disagrees.
  - Logs per-worker event sizing and warns if the fleet event count is not evenly divisible by worker count.

- `code/validate_event_report_bundle.sh`
  - EC2 preflight script for the final handoff.
  - Compiles the Python entrypoints.
  - Runs `gofmt` and rebuilds `go-loadgen/go-loadgen-linux-amd64`.
  - Greps for the required final-run fields so an old binary/script cannot silently be used.

- `FINAL_RUN_CHECKLIST.md`
  - One-page runbook for the final customer-facing run.
  - Calls out the exact no-reuse workload generation path, required validation report sections, and the “do not delete EC2 until report is reviewed” rule.

- `code/customer_event_report.py`
  - Merges one or more Go result JSON files into a customer-readable event-level report.
  - Reports 60/65 and 65/65 completion by 300ms, 350ms, and 500ms.
  - Reports score-ready and full-event p50/p95/p99/max.
  - Reports event latency histogram.
  - Reports binding reuse, unique full 8-field binding sets, per-field distinct counts, max repeats, event mix, and tail/miss drivers.
  - Reports runtime vs pre-agg vs serving bundle counts by group.
  - Reports hot-key values used and row counts when the source sample includes the hot-key profile.

- `code/build_reuse_events_from_stats.py`
  - Adds `--target-workload-events` and `--hot-event-pct` so the source pool can be sized for the final run, for example 300,000 events for a 5-minute 1000 EPS run.
  - Can also compute source-pool size directly from `--target-event-eps` and `--duration`.
  - Computes the required normal-event count and per-field hot-event count from the target.
  - Fails by default if any hot field returns fewer events than requested.
  - Adds source-pool stats to the reuse-events JSON.

## Required Validation Before Full Run

Run these on EC2 where Go is installed:

```bash
cd /home/ec2-user/intuit-demo/code
chmod +x validate_event_report_bundle.sh
./validate_event_report_bundle.sh
```

Then run a tiny validation workload and generate the report:

```bash
cd /home/ec2-user/intuit-demo/code
python3 build_reuse_events_from_stats.py \
  --target-event-eps 1000 \
  --duration 5m \
  --hot-event-pct 0.05 \
  --output results/reuse_events_final_300k.json
python3 generate_go_workload.py \
  --reuse-events-json results/reuse_events_final_300k.json \
  --output results/go_workload_final_300k.json \
  --target-event-eps 1000 \
  --duration 5m \
  --hot-event-pct 0.05 \
  --runtime-window-params \
  ...serving/preagg flags...
python3 run_go_loadgen_fleet.py ...small validation args...
python3 customer_event_report.py \
  --workload results/<workload>.json \
  --results results/<run_prefix>_*.json \
  --output-md results/<run_prefix>_customer_event_report.md \
  --output-json results/<run_prefix>_customer_event_report.json
```

The validation report should be checked before any full 1000 EPS run. Specifically confirm:

- Latency scope says benchmark-harness event wall-clock, not SQL-only.
- Event results are present and non-empty.
- 60/65 and 65/65 rows include 300ms, 350ms, and 500ms.
- Unique source event and unique binding-set counts are populated.
- All 8 key fields have distinct-count and max-repeat values.
- Event mix includes all desired hot-key categories, not just merchant.
- Runtime vs pre-agg vs serving counts match the intended physical design.
- Hot-key values and row counts are populated.
- `Source event sample reused` and `Workload rows cycled during run` are `no` for the customer-facing final run.
- Tail/miss drivers include `over_300`, `over_350`, and `over_500`.
