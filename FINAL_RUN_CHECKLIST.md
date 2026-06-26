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

Choose the final run length first, then use the same duration for the source
pool, Go workload, and fleet run. Do not build a 300K pool for a longer run.
If the run is 10 minutes at 1000 EPS, build 600K source events; if it is 20
minutes, build 1.2M source events. Otherwise the later part of the run cycles
the same workload rows and becomes cache-friendly again.

Example for a 10-minute 1000 EPS run:

```bash
TARGET_EVENT_EPS=1000
RUN_DURATION=10m
RUN_LABEL=1000eps_10m

python3 build_reuse_events_from_stats.py \
  --target-event-eps "${TARGET_EVENT_EPS}" \
  --duration "${RUN_DURATION}" \
  --hot-event-pct 0.05 \
  --no-hot-field card_holder_number_sha512 \
  --no-hot-field check_bank_account_number_sha512 \
  --output "results/reuse_events_${RUN_LABEL}.json"
```

Sizing examples:

- 5 minutes at 1000 EPS: 300,000 events
- 10 minutes at 1000 EPS: 600,000 events
- 20 minutes at 1000 EPS: 1,200,000 events

**Why `--no-hot-field` on two fields.** The two SHA512 hash fields (`card_holder_number_sha512`, `check_bank_account_number_sha512`) have no real hot key, their most-frequent value appears only ~5 and ~589 times in the data (they are high-cardinality identifiers). Confirmed live against the cluster. So we inject hot keys only on the 6 fields that genuinely have them (merchant, routing, exact_id, smart_id, input_ip, true_ip); the 2 hash fields stay unique, exactly like production. The script still fails loudly if it cannot build enough normal events, or enough hot events for those 6 fields. Do not pass `--allow-event-reuse` or `--allow-partial-hot-fields` for the customer run.

## 3. Generate the Go workload without event reuse

```bash
python3 generate_go_workload.py \
  --reuse-events-json "results/reuse_events_${RUN_LABEL}.json" \
  --output "results/go_workload_${RUN_LABEL}.json" \
  --target-event-eps "${TARGET_EVENT_EPS}" \
  --duration "${RUN_DURATION}" \
  --hot-event-pct 0.05 \
  --runtime-window-params \
  --no-require-hot-fields \
  --preagg-mode serving \
  --serving-bundle group_a_bundle_017 --serving-bundle group_a_bundle_018 \
  --serving-bundle group_a_bundle_019 --serving-bundle group_a_bundle_020 \
  --serving-bundle group_b_bundle_017 --serving-bundle group_b_bundle_018 \
  --serving-bundle group_b_bundle_019 --serving-bundle group_b_bundle_020 \
  --serving-bundle group_c_bundle_022 --serving-bundle group_c_bundle_023 \
  --serving-bundle group_c_bundle_024 --serving-bundle group_c_bundle_025
```

**IMPORTANT — pre-agg mode decides both realism and the 180d path.** This config gives the prior final-run shape: the **53 short-window bundles (1d/7d/30d/90d) run live against the base tables**, and the **12 180d bundles are served from the wide serving table** (`risk_feature_serving_wide`). Confirmed live: that keeps the 180d bundles at ~100-240ms.

Do NOT use plain `--preagg-mode serving` (no `--serving-bundle`): that routes ALL 65 to the serving table, so nothing runs live and the result is unrealistically fast.

Do NOT use `--preagg-mode hybrid` either: that routes the 12 180d bundles to the **daily rollup tables** (`group_*_180d_daily_distinct`, which are huge, billions of rows), and in live testing those became the slow tail (~1.6-2.4s each). The wide serving table is the optimized path and what the prior run used.

After generating, confirm the output is **53 runtime + 0 preagg + 12 serving** (not 65 serving, not 12 preagg).

Do not pass `--allow-event-reuse` for the customer-facing run. That flag is only for tiny smoke tests.

Expected final-run checks:

- `generated_workload_rows` should equal EPS x full run duration, for example 300,000 for 5 minutes, 600,000 for 10 minutes, or 1,200,000 for 20 minutes.
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

For 10 minutes at 1000 EPS:

```bash
RUN_DURATION=10m
RUN_EVENTS=600000
RUN_LABEL=1000eps_10m
```

For 20 minutes at 1000 EPS, use `RUN_DURATION=20m`, `RUN_EVENTS=1200000`,
and `RUN_LABEL=1000eps_20m`.

```bash
python3 run_go_loadgen_fleet.py \
  --hosts "ec2-user@host1,ec2-user@host2,ec2-user@host3,ec2-user@host4,ec2-user@host5,ec2-user@host6,ec2-user@host7,ec2-user@host8" \
  --ssh-key ~/intuit-bench.pem \
  --remote-dir /home/ec2-user/intuit-demo/code \
  --workload "results/go_workload_${RUN_LABEL}.json" \
  --execution-mode conn-fanout \
  --prepare-all \
  --events-total "${RUN_EVENTS}" \
  --target-event-eps 1000 \
  --duration "${RUN_DURATION}" \
  --query-timeout 0s \
  --max-execution-time-ms 0 \
  --output-prefix "go_fleet_${RUN_LABEL}"
```

Notes:
- `--hosts` and `--ssh-key` are required (the script exits if missing). `--workload` must be passed explicitly; if omitted it silently falls back to a default filename that step 3 does not produce.
- `--remote-dir` must point at wherever the bundle was deployed on the hosts (this matches the `cd` path in step 1). The script's built-in default is a different path.
- The fleet divides `--target-event-eps` across the worker processes, so the cluster sees ~1000 events/sec total, not per host.
- Use `conn-fanout` or `event-fanout`. Avoid `worker-pool` for the customer-facing run, the per-event SLA / cutoff metrics are only recorded in the fanout modes (the report footnotes those tables otherwise).

## 6. Generate the customer report immediately after the run

```bash
python3 customer_event_report.py \
  --workload "results/go_workload_${RUN_LABEL}.json" \
  --results results/<run_prefix>_*.json \
  --output-md results/<run_prefix>_customer_event_report.md \
  --output-json results/<run_prefix>_customer_event_report.json
```

Do not delete/recycle EC2 instances until the Markdown report and JSON report have been reviewed.
