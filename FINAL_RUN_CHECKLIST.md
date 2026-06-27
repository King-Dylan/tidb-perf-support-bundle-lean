# Final Intuit Event-Level Run Checklist

Use this checklist before any customer-facing 1000 EPS run. The goal is to prevent the two previous failure modes: replaying a tiny event set and saving only DB-side SQL metrics.

## 1. Rebuild and validate the runner on EC2

```bash
cd /home/ec2-user/intuit-demo/code
chmod +x validate_event_report_bundle.sh
./validate_event_report_bundle.sh
```

This must pass before running the benchmark. It rebuilds `go-loadgen/go-loadgen-linux-amd64`; otherwise the run can miss the per-event fields.

## 2. Build the source event pool from real data

For a 5-minute 1000 EPS run:

```bash
python3 build_reuse_events_from_stats.py \
  --target-event-eps 1000 \
  --duration 5m \
  --hot-event-pct 0.07 \
  --no-hot-field card_holder_number_sha512 \
  --no-hot-field check_bank_account_number_sha512 \
  --output results/reuse_events_1000eps_5m.json
```

For a 10-minute 1000 EPS run, change `--duration 10m`.

**Why `--no-hot-field` on two fields.** The two SHA512 hash fields (`card_holder_number_sha512`, `check_bank_account_number_sha512`) have no real hot key, their most-frequent value appears only ~5 and ~589 times in the data (they are high-cardinality identifiers). Confirmed live against the cluster. So we inject hot keys only on the 6 fields that genuinely have them (merchant, routing, exact_id, smart_id, input_ip, true_ip); the 2 hash fields stay unique, exactly like production. The script still fails loudly if it cannot build enough normal events, or enough hot events for those 6 fields. Do not pass `--allow-event-reuse` or `--allow-partial-hot-fields` for the customer run.

**Why `--hot-event-pct 0.07` (was 0.05).** Measured from cluster stats on 2026-06-26 (`SHOW STATS_TOPN` / `SHOW STATS_META`, hot = a field value with >10,000 occurrences, the harness's own normal/hot threshold). Per-field hot share of real traffic: routing 4.29%, input_ip 1.12%, true_ip 1.09%, smart_id 0.81%, merchant 0.10%, exact_id 0.09%; the two SHA512 fields are 0.00% (confirms `--no-hot-field`). Combined, ~5-7% of events are hot; 0.07 is the rounded-up (conservative) rate. This is a stats-based estimate from the two base tables (pmt_txn_fact 83.5M rows, deviceprofile_fact 365.8M rows); the exact event-level number depends on the join, and the tables reflect the all-time average, so confirm with Intuit whether their peak is spikier.

**Known gap — hot-field mix is still even, real traffic is routing-dominated.** The generator currently spreads hot events evenly across the 6 real hot fields, but the measurement says hot events are ~57% routing-number, ~15% input_ip, ~14% true_ip, ~11% smart_id, ~1% merchant, ~1% exact_id. If routing lookups are the slow tail, the even split under-tests them. Weighting the hot-field selection to those shares is a follow-up (not yet wired into the generator default).

## 3. Generate the Go workload without event reuse

```bash
python3 generate_go_workload.py \
  --reuse-events-json results/reuse_events_1000eps_5m.json \
  --output results/go_workload_1000eps_5m.json \
  --target-event-eps 1000 \
  --duration 5m \
  --hot-event-pct 0.07 \
  --runtime-window-params \
  --preagg-mode serving \
  --serving-layout wide \
  --serving-bundle group_a_bundle_017 --serving-bundle group_a_bundle_018 \
  --serving-bundle group_a_bundle_019 --serving-bundle group_a_bundle_020 \
  --serving-bundle group_b_bundle_017 --serving-bundle group_b_bundle_018 \
  --serving-bundle group_b_bundle_019 --serving-bundle group_b_bundle_020 \
  --serving-bundle group_c_bundle_022 --serving-bundle group_c_bundle_023 \
  --serving-bundle group_c_bundle_024 --serving-bundle group_c_bundle_025
```

**IMPORTANT — pre-agg mode decides both realism and the 180d path.** This config gives the prior final-run shape: the **53 short-window bundles (1d/7d/30d/90d) run live against the base tables**, and the **12 180d bundles are served from the wide serving table** (`risk_feature_serving_wide`). Confirmed live: that keeps the 180d bundles at ~100-240ms.

`--serving-layout wide` is required to actually hit `risk_feature_serving_wide` (the single-row flat read). Without it the serving bundles default into the KV pivot table `risk_feature_serving`, which has a different latency profile and is NOT the documented/optimized path. After generating, confirm the workload JSON records `"serving_layout": "wide"` and `"serving_table": "risk_feature_serving_wide"`.

Do not pass `--no-require-hot-fields` for the customer run. The generator now requires hot-key coverage on the 6 real hot fields (merchant, routing, exact_id, smart_id, input_ip, true_ip) by default and auto-relaxes the 2 SHA512 fields, so it will fail loud if the pool is missing hot coverage. `--no-require-hot-fields` is a smoke-test escape hatch only.

Defaults now match the blessed shape with no flags (v4.5.2): a bare `--preagg-mode serving` (no `--serving-bundle`) resolves to exactly the 12 180d serving bundles (53 runtime + 12 serving), `--serving-layout` defaults to `wide`, the fleet's `--execution-mode` defaults to `conn-fanout`, and `--hot-event-pct` defaults to `0.07` (the measured event-hot rate, see the note above). The explicit flags shown above are kept for clarity but are no longer required. To route ALL 65 to the serving table for a smoke test, pass `--allow-all-serving`; any serving set other than the 12 fails loud.

Do NOT use `--preagg-mode hybrid` either: that routes the 12 180d bundles to the **daily rollup tables** (`group_*_180d_daily_distinct`, which are huge, billions of rows), and in live testing those became the slow tail (~1.6-2.4s each). The wide serving table is the optimized path and what the prior run used.

After generating, confirm the output is **53 runtime + 0 preagg + 12 serving** (not 65 serving, not 12 preagg).

Do not pass `--allow-event-reuse` for the customer-facing run. That flag is only for tiny smoke tests.

Expected final-run checks:

- `generated_workload_rows` should equal EPS x duration, for example 300,000 for 5 minutes or 600,000 for 10 minutes.
- `unique_full_binding_sets` should be close to the generated workload rows.
- Hot-key events cover the 6 fields that have real hot keys (merchant, routing, exact_id, smart_id, input_ip, true_ip). The 2 SHA512 hash fields are intentionally excluded (no hot key).

## 4. Run a tiny validation first

Before full scale, run a short validation with a small duration and confirm the report format. This is not for performance claims.

Required report sections:

- Test Shape
- Binding Reuse / Test Realism
- Event Mix
- Runtime vs Pre-Agg / Serving Bundle Counts
- Hot-Key Values Used
- Event-Level SLA
- Event Latency (wall-clock)
- Event Latency (SQL-only, DB-side)
- Return-Time Histogram
- Average Bundles Returned
- Tail / Miss Drivers

Required correctness checks:

- Latency scope says benchmark-harness event wall-clock.
- `Source event sample reused` is `no`.
- `Workload rows cycled during run` is `no`.
- 60/65 and 65/65 include 300ms, 350ms, and 500ms.
- All 8 key fields have distinct-count and max-repeat values.
- Tail drivers include `>300ms`, `>350ms`, and `>500ms`.

## 5. Full run

Run from the bundle `code/` dir. Fill in your real hosts and key. `--remote-dir`, `--workload`, and the EPS/duration must match what you deployed and generated in step 3.

```bash
python3 run_go_loadgen_fleet.py \
  --hosts "ec2-user@host1,ec2-user@host2,ec2-user@host3,ec2-user@host4,ec2-user@host5,ec2-user@host6,ec2-user@host7,ec2-user@host8" \
  --ssh-key ~/intuit-bench.pem \
  --remote-dir /home/ec2-user/intuit-demo/code \
  --workload results/go_workload_1000eps_5m.json \
  --execution-mode conn-fanout \
  --prepare-all \
  --target-event-eps 1000 \
  --duration 5m \
  --query-timeout 0s \
  --max-execution-time-ms 500 \
  --output-prefix go_fleet_1000eps_5m
```

Notes:
- `--max-execution-time-ms 500` caps each bundle at Intuit's 500ms deadline (TiDB `max_execution_time`, server-side). A bundle that exceeds it is aborted and counts as a miss, and its connection is freed immediately so a slow tail can't starve other events. Precision is ~100ms. For an uncapped diagnostic run (to see true tail latencies and tune the slow bundles first), set `--max-execution-time-ms 0`.
- `--hosts` and `--ssh-key` are required (the script exits if missing). `--workload` must be passed explicitly; if omitted it silently falls back to a default filename that step 3 does not produce.
- `--remote-dir` must point at wherever the bundle was deployed on the hosts (this matches the `cd` path in step 1). The script's built-in default is a different path.
- The fleet divides `--target-event-eps` across the worker processes, so the cluster sees ~1000 events/sec total, not per host.
- Use `conn-fanout` for the customer-facing run (as above). `conn-fanout` applies real backpressure across the connection pool; `event-fanout` has no backpressure and folds connection-wait into query time, which inflates the wall-clock latency shown to the customer when the pool is contended. Avoid `worker-pool`: the per-event SLA / cutoff metrics are only recorded in the fanout modes (the report footnotes those tables otherwise).

## 6. Generate the customer report immediately after the run

```bash
python3 customer_event_report.py \
  --workload results/go_workload_1000eps_5m.json \
  --results results/<run_prefix>_[0-9]*.json \
  --fleet-summary results/<run_prefix>_summary.json \
  --output-md results/<run_prefix>_customer_event_report.md \
  --output-json results/<run_prefix>_customer_event_report.json
```

Notes:
- The fleet runner writes one per-host result `results/<run_prefix>_<index>.json` per app process **and** a fleet summary `results/<run_prefix>_summary.json`. The summary carries the true fleet-wide run shape (total target EPS, total connections, app-process count, duration). Pass it with `--fleet-summary` so the report's headline target EPS and total connections are the FLEET totals, not a single process's per-process slice.
- Use the `--results results/<run_prefix>_[0-9]*.json` glob (numeric suffix) so the per-host glob does NOT also match `<run_prefix>_summary.json`. The report also excludes any `*_summary.json` it sees, but the numeric glob keeps the inputs unambiguous and avoids counting the summary as an extra app process.
- If `--fleet-summary` is omitted, the report falls back to deriving the fleet shape from the per-host result files (summing the per-process target EPS and connections across processes, and taking the MAX elapsed since the processes run concurrently). Prefer passing `--fleet-summary` for the customer report so the headline numbers are authoritative.

Do not delete/recycle EC2 instances until the Markdown report and JSON report have been reviewed.
