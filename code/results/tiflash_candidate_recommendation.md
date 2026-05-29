# TiFlash Candidate Recommendation

Generated from EC2 `EXPLAIN ANALYZE` A/B runs on 2026-05-29.

## Verdict

Do not move the current residual slow bundles to TiFlash with hints.

The tested hot-key bundle SQLs either stayed on TiKV when TiFlash was merely allowed, or became slower/failed when TiFlash was forced. In this TiDB Cloud disaggregated TiFlash setup, disabling MPP is not viable because TiFlash requires `tidb_allow_mpp=ON`.

## Evidence Summary

### Residual slow bundles, current hybrid prod180 shape

Source: `results/tiflash_candidate_ab_residual_hot.md`

| bundle | shape | TiKV | cost with TiFlash allowed | forced TiFlash result | recommendation |
|---|---|---:|---:|---|---|
| `group_b_bundle_012` | runtime base table, hot `true_ip` | 2397.9 ms | 2381.9 ms, still TiKV | failed, TiFlash MPP memory limit | keep TiKV; needs SQL/feature reduction or pre-agg |
| `group_b_bundle_018` | prod180 helper distinct, hot `input_ip` | 1243.2 ms | 1215.4 ms, still TiKV | 1230.5 ms, still TiKV; TiFlash-only fails because helper table has only TiKV | keep TiKV/helper; TiFlash not solving bottleneck |
| `group_b_bundle_020` | prod180 helper distinct, hot `true_ip` | 1272.1 ms | 1235.0 ms, still TiKV | 1209.2 ms, still TiKV; TiFlash-only fails because helper table has only TiKV | keep TiKV/helper; TiFlash not solving bottleneck |
| `group_c_bundle_018` | runtime join, hot `true_ip` | 342.4 ms | 339.2 ms, still TiKV | timeout at 10s | keep TiKV |

### Runtime-only base-table shape

Source: `results/tiflash_candidate_ab_runtime_hot.md`

| bundle | TiKV | cost with TiFlash allowed | forced TiFlash result | recommendation |
|---|---:|---:|---|---|
| `group_b_bundle_012` | 2400.3 ms | 2379.2 ms, still TiKV | failed, TiFlash MPP memory limit | no TiFlash hint |
| `group_b_bundle_018` | 1848.9 ms | 1593.8 ms, still TiKV | failed, TiFlash MPP memory limit | no TiFlash hint |
| `group_b_bundle_020` | 1776.9 ms | 1532.8 ms, still TiKV | failed, TiFlash MPP memory limit | no TiFlash hint |
| `group_c_bundle_018` | 354.3 ms | 332.9 ms, still TiKV | failed, TiFlash MPP memory limit | no TiFlash hint |

### Group A hot payment candidates

Source: `results/tiflash_candidate_ab_group_a_hot.md`

| bundle | TiKV | cost with TiFlash allowed | forced TiFlash result | recommendation |
|---|---:|---:|---|---|
| `group_a_bundle_001` | 34.5 ms | 22.4 ms, still TiKV | timeout at 10s | no TiFlash hint |
| `group_a_bundle_002` | 58.2 ms | 54.6 ms, still TiKV | 528.3 ms on TiFlash | no TiFlash hint |
| `group_a_bundle_004` | 31.6 ms | 30.5 ms, still TiKV | 321.8 ms on TiFlash | no TiFlash hint |
| `group_a_bundle_005` | 22.6 ms | 22.5 ms, still TiKV | failed | no TiFlash hint |
| `group_a_bundle_006` | 284.6 ms | 287.3 ms, still TiKV | failed, TiFlash MPP memory limit | no TiFlash hint |
| `group_a_bundle_008` | 57.0 ms | 57.6 ms, still TiKV | failed | no TiFlash hint |
| `group_a_bundle_009` | 21.3 ms | 23.1 ms, still TiKV | failed | no TiFlash hint |
| `group_a_bundle_010` | 87.6 ms | 87.5 ms, still TiKV | failed, TiFlash MPP memory limit | no TiFlash hint |
| `group_a_bundle_012` | 122.3 ms | 118.8 ms, still TiKV | failed | no TiFlash hint |

### MPP-off test

Source: `results/tiflash_candidate_ab_mpp_off_hot.md`

`tiflash_hint_mpp_off` failed immediately for representative Group A, B, and C bundles with:

```text
cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch
```

So the safe TiFlash path here is MPP only, and the MPP path is exactly where the hot-key aggregate queries hit memory pressure.

### MPP memory knob tests

Sources:

- `results/tiflash_mpp_memory_quota8g.md`
- `results/tiflash_mpp_memory_threads2.md`
- `results/tiflash_mpp_memory_spill256m.md`
- `results/tiflash_mpp_memory_combo.md`

I tested the representative failing forced-TiFlash queries `group_b_bundle_012` and `group_a_bundle_006` with these session-level knobs:

| test | session knobs | result |
|---|---|---|
| quota8g | `tiflash_mem_quota_query_per_node=8589934592` | still failed with TiFlash MPP memory limit |
| threads2 | `tidb_max_tiflash_threads=2` | still failed with TiFlash MPP memory limit |
| spill256m | `tidb_max_bytes_before_tiflash_external_group_by/sort/join=268435456` | still failed with TiFlash MPP memory limit |
| combo | all three settings above | still failed with TiFlash MPP memory limit |

The error stayed around:

```text
process memory size would be 4.8 GiB ... limit of memory for data computing : 3.76 GiB
```

This means the exposed session knobs do not lift the actual TiFlash process/node data-computing limit for these forced-MPP scans. In this environment, solving it by parameter would likely require cluster-level TiFlash sizing/configuration changes rather than a SQL/session variable.

## Why TiFlash Does Not Help These Queries

1. The current covering TiKV indexes are highly targeted for the bundle predicates, so the optimizer keeps choosing `IndexRangeScan` on TiKV even when TiFlash is available.
2. The worst Group B 180d hybrid queries are dominated by exact `COUNT(DISTINCT)` over `group_b_180d_daily_distinct`. That helper table does not have a TiFlash replica, and forcing `tiflash,tidb` produces “No access path for table `x`”.
3. For runtime-only hot-key scans, forced TiFlash uses MPP and frequently exceeds TiFlash memory limits before it can beat TiKV.
4. For Group C hot true_ip joins, the covering TiKV access path is already around 330-350 ms in the focused test; forced TiFlash is much worse.
5. Session-level memory/concurrency/spill knobs did not remove the TiFlash MPP memory failure in focused tests.

## Recommendation

Keep the mixed benchmark default as TiKV/TiDB for these candidates. Do not add targeted TiFlash hints to `mixed_traffic_test.py`.

If we need more improvement, the next work should be on:

- reducing the hot `true_ip` 30d Group B feature breadth, especially exact distincts;
- considering selective pre-agg beyond 180d only for the few hot 30d Group B paths if the customer accepts it;
- making score-ready fallback/defer policy explicit at the app layer rather than trying to force every hot-key bundle under 350-500 ms;
- only considering TiFlash replicas for the large 180d helper distinct tables after capacity approval, because `group_b_180d_daily_distinct` is very large and the tested bottleneck is exact distinct aggregation, not just base-table scan throughput.
