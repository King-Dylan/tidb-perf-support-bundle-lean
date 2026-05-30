# Group B Helper TiFlash Replica Change

Generated on 2026-05-30.

## DDL Applied

```sql
ALTER TABLE intuit_risk.group_b_180d_daily_distinct SET TIFLASH REPLICA 1;
ALTER TABLE intuit_risk.group_b_180d_daily_rollup SET TIFLASH REPLICA 1;
```

## Reason

The full 65/65 event runs still have 500ms failures on Group B 180d helper distinct bundles:

- `group_b_bundle_018`
- `group_b_bundle_019`
- `group_b_bundle_020`

Before this change, only the two base tables had TiFlash replicas. The helper table `group_b_180d_daily_distinct` had no TiFlash access path, so `tiflash,tidb` / forced MPP tests for B018/B020/B019 could not use TiFlash for the large helper scan.

## Table Size Snapshot

| Table | Approx rows | Approx size |
|---|---:|---:|
| `group_b_180d_daily_distinct` | 2,543,647,958 | 407.24 GB |
| `group_b_180d_daily_rollup` | 0 | 0.00 GB |

## Post-DDL Status Snapshot

Immediately after DDL:

| Table | Replica count | Available | Progress |
|---|---:|---:|---:|
| `group_b_180d_daily_distinct` | 1 | 0 | 0.01 |
| `group_b_180d_daily_rollup` | 1 | 0 | 0.00 |

The TiFlash replica metadata is present, but the large distinct table is still syncing. Wait for `AVAILABLE=1` and `PROGRESS=1.0` before re-running B018/B019/B020 TiFlash plan tests.

## Follow-Up Validation

When sync is complete, run focused A/B on:

```bash
python3 compare_tiflash_candidates.py \
  --mixed-json results/mixed_traffic_1780125439.json \
  --output-json results/group_b_helper_tiflash_after_sync.json \
  --output-md results/group_b_helper_tiflash_after_sync.md \
  --bundle-id group_b_bundle_018 \
  --bundle-id group_b_bundle_019 \
  --bundle-id group_b_bundle_020 \
  --max-events-per-bundle 1 \
  --variant tikv \
  --variant cost \
  --variant tiflash_only \
  --max-execution-time-ms 10000 \
  --no-distinct-agg-pushdown \
  --hashagg-final-concurrency 4 \
  --hashagg-partial-concurrency 4
```

Then, only if the focused plans improve and remain stable, test full-event traffic with targeted TiFlash for those bundles.
