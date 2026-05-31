# PR: Runtime-Safe Query Pruning And Fan-Out Efficiency

## Context

The original PDF defines a real-time risk enrichment workload:

- Two base tables: `pmt_txn_fact` and `deviceprofile_fact`.
- Runtime aggregate lookups by event entity keys such as merchant, card holder, bank routing/account, exact ID, smart ID, input IP, and true IP.
- Three query groups: transaction-only, device-only, and joined transaction/device queries.
- Up to 1000 event requests/sec, with all event features returned inside the latency budget.

The current support bundle already maps the original feature set into 65 bundle SQLs per event and uses exact 180d closed-day pre-aggregation as a practical middle ground. The next optimization pass should preserve production semantics and avoid benchmark-only tricks.

## Non-Goals

- Do not cache final event decisions.
- Do not precompute results for a known benchmark event sample.
- Do not broaden pre-aggregation beyond evidence-backed windows or keys.
- Do not force TiFlash for helper distinct tables unless focused A/B shows a win.

## Optimization Strategy

1. Keep the default benchmark event model honest:
   - unique events for high-rate runs
   - hot-key mix as an explicit parameter
   - full 65/65 fan-in metrics reported separately from 60/65 fallback metrics

2. Prefer runtime-safe SQL shape changes first:
   - remove redundant `GROUP BY` when equality predicates fix the group key
   - avoid making every predicate a `CASE WHEN` if a more selective shape is available
   - pre-aggregate inside a single runtime SQL over low-cardinality dimensions when it reduces root aggregation cost without materializing data

3. Use exact pre-aggregation only where it is a production-serving layout:
   - closed historical windows
   - exact daily rollups or exact distinct helper values
   - online query still combines closed-day history with runtime boundary rows

4. Validate one candidate at a time:
   - focused `EXPLAIN ANALYZE` for the affected bundle(s)
   - compare elapsed time, `actRows`/processed keys, storage task type, memory, and disk
   - keep the change only if it improves or remains neutral on representative events

## First Candidate In This Branch

Generalize the already-validated Group A runtime dimension-rollup shape beyond the two routing bundles where it was manually enabled.

This keeps all data in the base table at query time. It rewrites a wide `CASE WHEN` aggregate over many low-cardinality predicates into:

```sql
SELECT ... CASE aggregation ...
FROM (
  SELECT low_cardinality_dimensions,
         COUNT(*) AS row_count,
         SUM(amount) AS amount_sum,
         MIN(amount) AS amount_min,
         MAX(amount) AS amount_max
  FROM pmt_txn_fact
  WHERE entity_key = ? AND event_date >= ?
  GROUP BY low_cardinality_dimensions
) b
```

Expected benefit:

- Keeps the same result semantics.
- Avoids repeatedly evaluating many `CASE WHEN` expressions over every raw row.
- Reduces root aggregation work for hot keys while staying runtime-only.

Implementation details:

- The runtime rollup remains enabled by default only for Group A bundles `010` and `014`.
- Broader candidates are still available as an explicit experiment, but the full 65/65 fan-out benchmark showed that enabling them broadly shifts many formerly fast Group A bundles into the 200-400ms range and hurts the 350ms SLA path.
- The v15 prod180 run script now defaults `TIFLASH_MPP_BUNDLE_IDS` to `group_b_bundle_012 group_c_bundle_018`, hot-field-only. Focused A/B showed those true-IP paths improve with TiFlash MPP, while broader TiFlash forcing was neutral or worse.
- Set `INTUIT_GROUP_A_DIMENSION_ROLLUP_BUNDLES=none` to run the legacy wide `CASE WHEN` shape.
- Set `INTUIT_GROUP_A_DIMENSION_ROLLUP_BUNDLES=group_a_bundle_006,group_a_bundle_010` to test a focused subset.

Validation scope:

- Focused Group A bundles that show up often in top-3 slowest event paths:
  - `group_a_bundle_001`
  - `group_a_bundle_005`
  - `group_a_bundle_006`
  - `group_a_bundle_009`
  - `group_a_bundle_010`
  - `group_a_bundle_012`
- Then a full 65/65 event benchmark at the established 12 eps control configuration.

## Validation Results

Focused `EXPLAIN ANALYZE` A/B showed that a broader Group A rollup can make single SQLs faster, but the full 65-way fan-out is the deciding test. The broad rollup set was therefore rejected as a default.

| Candidate | Focused result | Full fan-out decision |
| --- | ---: | --- |
| `group_a_bundle_010` | 483.8ms -> 96.8ms | Keep |
| `group_a_bundle_014` | 225.7ms -> 59.7ms | Keep |
| `group_a_bundle_002` | 191.3ms -> 54.5ms | Reject; full fan-out p95 worsened |
| `group_a_bundle_006` | 294.0ms -> 71.2ms | Reject as rollup; full fan-out p95 worsened |
| `group_a_bundle_012` | 138.8ms -> 31.2ms | Reject; full fan-out p95 worsened |
| `group_a_bundle_003/004/005/009/011` | no material focused win | Reject |

Focused TiFlash A/B on residual slow bundles:

| Bundle | TiKV | Best TiFlash/Mixed | Decision |
| --- | ---: | ---: | --- |
| `group_b_bundle_012` hot `true_ip` | 2588.2ms | 307.6ms | Keep as hot-field-only TiFlash MPP |
| `group_c_bundle_018` hot `true_ip` | 315.0ms | 184.9ms | Keep as hot-field-only TiFlash MPP |
| `group_a_bundle_006` hot routing | 283.9ms | 157.6ms | Reject default; full fan-out did not improve |
| `group_b_bundle_018/019/020` | 680-1186ms | neutral/worse | Reject |
| `group_c_bundle_004/011` | 21-72ms | slower on TiFlash | Reject |
| `group_c_bundle_023/025` | 528-918ms | worse on TiFlash | Reject |

Full 65/65 validation at 12 eps, 30s, prod180 hybrid, no writes, TiKV/TiDB default with targeted TiFlash only where noted:

| Run | Event p50 | Event p95 | 65/65 <=350ms | 65/65 <=500ms | Full 65/65 p95 | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Legacy wide CASE, no targeted TiFlash | 544.4ms | 624.5ms | 133/361 (36.8%) | 151/361 (41.8%) | 532.6ms | baseline control |
| Keep only A010/A014 runtime rollup | 179.3ms | 593.4ms | 314/360 (87.2%) | 327/360 (90.8%) | 335.7ms | rejected broad rollup |
| A010/A014 + B012/C018 hot TiFlash | 170.5ms | 555.1ms | 319/360 (88.6%) | 332/360 (92.2%) | 341.9ms | current script default |
| Add A006 hot TiFlash | 184.6ms | 555.6ms | 313/360 (86.9%) | 331/360 (91.9%) | 350.8ms | rejected |

The current default meets the 500ms fallback target for `>=60/65` bundles at 99.7% in this run and keeps full 65/65 p95 near the 350ms target, but full 65/65 p99 is still around 445ms. The remaining tail is mostly hot `input_ip`/`smart_id`/`true_ip` Group B/C paths and occasional context deadline cancellations, not the original A010/A014 payment rollup issue.
