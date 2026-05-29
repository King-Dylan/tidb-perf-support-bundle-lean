# Intuit TiDB Performance Support Bundle

Lean internal bundle for Hua/Jinlong/TSE review. This contains only the files needed to recreate the final demo build, validate the data/pre-agg tables, and run the benchmark using whole-number events/sec.

This bundle intentionally excludes older experiment helpers and the separate `run_qps_ladder.py` harness. The benchmark runner is the same event/sec harness used for the demo.

## Contents

- `code/`: runnable scripts with the same layout as the working repo.
- `connection/.db_config.json`: local Premium connection config when present; ignored by git.
- `docs/`: v15 runbook and audit notes.
- `results_reference/current_schema_ddl.sql`: current DDL for 2 base tables + 6 pre-agg tables.
- `results_reference/current_tiflash_status.txt`: current TiFlash replica status.
- `results_reference/diag_200qps_mixed_traffic_1780002101.json`: latest 200-QPS-equivalent diagnostic result.

## Current Physical Design

- Base tables:
  - `pmt_txn_fact`
  - `deviceprofile_fact`
- Layout:
  - monthly partitioning
  - `SHARD_ROW_ID_BITS=4`
  - `PRE_SPLIT_REGIONS=3`
- Optimized covering indexes:
  - runtime payment indexes for merchant/card/routing/join paths
  - runtime device indexes for exact/smart/input/true IP paths
  - Group C payment-side covering join indexes for merchant/card/routing-account paths
- 180d pre-agg tables:
  - `group_a_180d_daily_rollup`
  - `group_a_180d_daily_distinct`
  - `group_b_180d_daily_rollup`
  - `group_b_180d_daily_distinct`
  - `group_c_180d_daily_rollup`
  - `group_c_180d_daily_distinct`
- TiFlash status at export time:
  - base tables enabled and synced
  - pre-agg tables TiKV-only

## Workload

- 1 event = 65 independent bundle queries. They share the same event bindings/reference time and can fan out in parallel; scoring waits for the combined fan-in result.
- Runtime-only windows: `1d`, `7d`, `30d`, `90d`.
- `180d` windows use the 6 consolidated pre-agg tables.
- Group C runtime joins include timestamp filters on both tables.
- Per-query cutoff: `READ_MAX_EXECUTION_TIME_MS=500`.
- Background writes are enabled by default in the mixed benchmark.

## Event QPS Target

The final SLA is stated in event QPS. The benchmark accepts events/sec and
issues 65 bundled SQLs for every event.

| Target | Events/sec | Bundle SQL/sec |
| --- | ---: | ---: |
| Normal | 100 | 6,500 |
| Peak | 1000 | 65,000 |

Formula: `bundle_sql_per_sec = events/sec * 65`.

## Setup

```bash
cd code
cp ../connection/.db_config.json .db_config.json
python3 -m py_compile *.py lib/*.py
ulimit -n 200000
```

For an already-built database, this safely applies any missing optimized
covering indexes:

```bash
python3 apply_optimized_indexes.py --execute
```

## Full Build

Warning: this drops/recreates the base tables in the configured database.

```bash
cd code
cp ../connection/.db_config.json .db_config.json
./run_v15_full_monthly_premium_build.sh
```

This runs:

1. `setup_schema.py`
2. full data load
3. `enable_tiflash.py`
4. `analyze_tables.py`
5. `verify_partitioning.py`
6. `profile_validation.py`
7. `run_prod_180d_preagg_parallel.sh`

## Correctness Gates

```bash
cd code
cp ../connection/.db_config.json .db_config.json
./run_prod180_correctness_gates.sh
```

This checks:

- Python compile
- pre-agg structural/data coverage
- raw-vs-preagg exact-result spotchecks

## Reusable Event Sample

Use this to avoid benchmarking event sampling/hot-key discovery:

```bash
cd code
cp ../connection/.db_config.json .db_config.json
python3 build_reuse_events_from_stats.py \
  --normal-events 11000 \
  --hot-events-per-field 100 \
  --output results/reuse_events_hua_fullscale.json
```

## Run Benchmark

200-QPS equivalent:

```bash
cd code
cp ../connection/.db_config.json .db_config.json
ulimit -n 200000
export READ_MAX_EXECUTION_TIME_MS=500
export TIDB_ISOLATION_READ_ENGINES=tikv,tidb
export INTUIT_FORCE_INLINE_CTE=0
export REUSE_EVENTS_JSON=results/reuse_events_hua_fullscale.json
export SUMMARY_ONLY=1
export POOL_SIZE=256
export BUNDLE_WORKERS=256
export EVENT_WORKERS=32
export MAX_PENDING_EVENTS=16
./run_v15_prod180_benchmark.sh 3 300
```

The benchmark wrapper now defaults to the optimized prod180 path:

- `PREAGG_LAYOUT=prod180`
- 180d bundles only use the consolidated pre-agg tables
- read sessions use `tidb_isolation_read_engines='tikv,tidb'`
- `tidb_opt_force_inline_cte=0`
- runtime SQL omits redundant `GROUP BY` when the group key is already fixed by equality predicates

Other event rates:

```bash
./run_v15_prod180_benchmark.sh 100 300
./run_v15_prod180_benchmark.sh 1000 300
```

Per Hua's request, stop if average latency exceeds `1s`.

## Client-Side Diagnostics

The packaged `mixed_traffic_test.py` records:

- event-level fan-out capacity: target bundle SQL/sec, client bundle slots,
  and 350/500ms slot requirements
- bundle task queue average/max per event
- DB connection wait average/max per event

In the 200-QPS-equivalent diagnostic run:

- bundle task queue avg p95: `38.3ms`
- bundle task queue max p95: `72.8ms`
- DB connection wait avg/max: `0.0ms`

This indicates the app was not waiting on the connection pool and client queueing was small relative to event latency.

## Security Note

The git repository ignores `.db_config.json`. If a local exported bundle includes
credentials beside the repo, do not forward those files externally or to the customer.
