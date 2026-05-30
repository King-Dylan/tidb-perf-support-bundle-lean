# Group B Helper TiFlash After-Sync Validation

- Date: 2026-05-30
- Environment: EC2 benchmark client against TiDB Cloud `intuit_risk`
- Tables checked:
  - `intuit_risk.group_b_180d_daily_distinct`
  - `intuit_risk.group_b_180d_daily_rollup`

## Replica Status

Both helper tables reached `AVAILABLE=1` and `PROGRESS=1.0`.

```text
group_b_180d_daily_distinct replica_count=1 available=1 progress=1.0
group_b_180d_daily_rollup   replica_count=1 available=1 progress=1.0
```

## Focused TiKV vs TiFlash A/B

Command artifact:

- JSON: `results/group_b_helper_tiflash_after_sync.json`
- Full SQL and `EXPLAIN ANALYZE`: `results/group_b_helper_tiflash_after_sync.md`

| bundle | hot field | TiKV-only | Cost mode | Forced TiFlash | Cost-mode plan |
|---|---|---:|---:|---:|---|
| `group_b_bundle_018` | `input_ip` | 1209.8 ms | 1150.2 ms | timeout at 10031.3 ms | `cop[tikv]`, `root` |
| `group_b_bundle_019` | `smart_id` | 740.8 ms | 700.9 ms | timeout at 10095.0 ms | `cop[tikv]`, `root` |
| `group_b_bundle_020` | `true_ip` | 1187.1 ms | 1114.6 ms | 7383.5 ms | `cop[tikv]`, `root` |

Interpretation:

- TiFlash replicas are visible and queryable.
- The optimizer still chooses TiKV for these focused helper queries in cost mode.
- Forcing TiFlash/MPP is not beneficial here. Two queries hit `max_execution_time`; the one that completed was much slower than TiKV.

## Full 65/65 Event Benchmark

Established 12 eps config:

```text
POOL_SIZE=600
BUNDLE_WORKERS=600
EVENT_WORKERS=128
MAX_PENDING_EVENTS=256
READ_MAX_EXECUTION_TIME_MS=500
REUSE_EVENTS_JSON=results/mixed_traffic_1780092771.json
```

| run | engines | result JSON | event p50 | event p95 | event p99 | 65/65 <=500ms | full 65/65 p99 | notes |
|---|---|---|---:|---:|---:|---:|---:|---|
| Before helper TiFlash, cost-mode reference | TiKV + TiFlash + TiDB | `results/mixed_traffic_1780125638.json` | 167.1 ms | 618.2 ms | 698.4 ms | 331/360 (91.9%) | 388.5 ms | Previous best cost-mode reference |
| After helper TiFlash, cost mode | TiKV + TiFlash + TiDB | `results/mixed_traffic_1780183072.json` | 556.5 ms | 619.1 ms | 5053.0 ms | 76/361 (21.1%) | 477.9 ms | Severe regression; 3060 bundle executions returned after 500ms/error |
| After helper TiFlash, TiKV-only control | TiKV + TiDB | `results/mixed_traffic_1780183238.json` | 177.3 ms | 575.7 ms | 633.8 ms | 333/361 (92.2%) | 432.9 ms | Healthy control; comparable to earlier TiKV-only runs |

## Decision

Do not enable TiFlash for the Group B helper distinct path in the production benchmark config yet.

Keep the full-event runtime on `tidb_isolation_read_engines=tikv,tidb` for now. The helper TiFlash replicas are synced, but the measured effect is:

- Focused forced-TiFlash helper queries are slower than TiKV.
- Full-event cost mode with TiFlash available regressed badly.
- TiKV-only after the replica sync remains stable at roughly the previous 12 eps result: `65/65 <=500ms` is 333/361 events.

Next useful optimization should target the remaining TiKV hot spots directly rather than forcing these helper distinct tables onto TiFlash. In this control run the recurring slowest groups were mostly Group A runtime bundles, plus a smaller tail from Group C and a few Group B 30d/runtime bundles.
