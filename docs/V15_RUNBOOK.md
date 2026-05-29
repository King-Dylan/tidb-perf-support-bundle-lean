# Intuit Demo v15 Runbook

v15 is the full-scale Premium/BYOC validation of the winning v12 design.

## Design

- Scale: `INTUIT_DEMO_SCALE=full`
- Physical layout: monthly partitioning, no clustering.
- Base tables: `pmt_txn_fact`, `deviceprofile_fact`.
- 180d pre-agg: six production-style consolidated tables:
  - `group_a_180d_daily_rollup`
  - `group_a_180d_daily_distinct`
  - `group_b_180d_daily_rollup`
  - `group_b_180d_daily_distinct`
  - `group_c_180d_daily_rollup`
  - `group_c_180d_daily_distinct`
- Runtime path: all 1d/7d/30d/90d bundles plus non-selected windows.
- Pre-agg path: all 180d bundles only.
- Event execution model: the 65 bundle SQLs for one event are independent,
  share the same event bindings/reference time, and fan out in parallel. The
  app waits only at the final fan-in to score the event.
- Optimized query path:
  - runtime SQL removes redundant `GROUP BY` when equality predicates make the group key constant
  - 180d distinct and mixed rollup+distinct SQL shares one raw-boundary CTE per bundle
  - read sessions force TiKV/TiDB and leave CTE inline forcing disabled

All base and pre-agg tables use `SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3` by default.
The schema also includes the verified covering indexes from the slow-query
optimization pass. On an existing cluster, run:

```bash
python3 apply_optimized_indexes.py --execute
```

## Full Build

Run this against the Premium/BYOC database credentials:

```bash
./run_v15_full_monthly_premium_build.sh
```

Default build settings:

```bash
INTUIT_PARTITION_GRAIN=monthly
INTUIT_DEMO_SCALE=full
INTUIT_RANDOM_SEED=20260512
INTUIT_LOAD_WRITER_THREADS=96
INTUIT_LOAD_BATCH_SIZE=10000
INTUIT_LOAD_MAX_PENDING_BATCHES=512
INTUIT_SEGMENTED_LOAD=1
PMT_SEGMENTS=16
DEVICE_SEGMENTS=32
INTUIT_SEGMENT_WRITER_THREADS=4
INTUIT_SEGMENT_BATCH_SIZE=10000
INTUIT_SEGMENT_MAX_PENDING_BATCHES=32
PREAGG_DAILY_WORKERS=8
START_DAY=2025-11-19
END_DAY=2026-05-18
```

Override any setting inline if needed:

```bash
INTUIT_LOAD_WRITER_THREADS=128 PREAGG_DAILY_WORKERS=12 ./run_v15_full_monthly_premium_build.sh
```

## Benchmark

After the full build, delete/restore the cluster if you want a cleaner cluster/cache state, then run the baseline:

```bash
./run_v15_prod180_benchmark.sh 3 300
```

The v15 benchmark wrapper defaults to unique-event mode. It automatically sizes
the event sample for the requested rate/duration, so a 1000 eps / 300 second run
requests about 300k unique events instead of replaying the same ~900 bindings.
It also defaults to `PREAGG_LAYOUT=prod180`, `TIDB_ISOLATION_READ_ENGINES=tikv,tidb`,
and `INTUIT_FORCE_INLINE_CTE=0`.

Ramp one run at a time:

```bash
./run_v15_prod180_benchmark.sh 10 300
./run_v15_prod180_benchmark.sh 25 300
./run_v15_prod180_benchmark.sh 50 300
./run_v15_prod180_benchmark.sh 100 300
```

Only continue to stress levels if 100 eps looks healthy:

```bash
./run_v15_prod180_benchmark.sh 200 300
./run_v15_prod180_benchmark.sh 500 300
./run_v15_prod180_benchmark.sh 1000 300
```

## Notes

- `100 eps = 6,500 bundle executions/sec`.
- `1000 eps = 65,000 bundle executions/sec`.
- Capacity sanity check: bundle slots needed at a deadline are approximately
  `events/sec * 65 * deadline_seconds`; for 1000 eps this is 22,750 slots at
  350ms or 32,500 slots at 500ms if every bundle occupies a slot until the
  deadline. Faster average bundle time reduces the needed slots.
- Runs at `>=100 eps` default to summary-only output to avoid multi-GB result
  JSON files. They still print Henry/Andrew coverage and the bundle drop-off
  histogram.
- For detailed per-bundle Markdown/CSV output at lower rates, keep
  `SUMMARY_ONLY=0`.
- The harness now uses bounded event and bundle worker pools instead of creating
  one event thread and one 65-thread bundle pool per event.
- The harness applies event backpressure (`MAX_PENDING_EVENTS`) so an overloaded
  run reports a lower achieved EPS instead of queueing until the client OOMs.
- If the client EC2 becomes the bottleneck, run multiple load generators or increase client resources before judging TiDB.
- Keep `READ_MAX_EXECUTION_TIME_MS=500` unless intentionally changing the customer timeout model.
