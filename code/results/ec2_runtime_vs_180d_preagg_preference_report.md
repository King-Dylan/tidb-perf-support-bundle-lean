# EC2 Runtime-Only vs 180d-Only Pre-Agg Preference Report

## Customer Preference Order

1. Best: all 65 bundle queries runtime-only against the 2 base tables, `pmt_txn_fact` and `deviceprofile_fact`.
2. Acceptable: runtime for 1d / 7d / 30d / 90d, with pre-aggregation only for 180d bundles.
3. Less ideal: broader pre-aggregation only if absolutely required.

The current `prod180` path maps to preference 2. The code only allows prod180 pre-agg for `window_days = 180`; non-180d bundles raise an error. The enabled bundle set is:

- Group A 180d: `group_a_bundle_017`, `group_a_bundle_018`, `group_a_bundle_019`, `group_a_bundle_020`
- Group B 180d: `group_b_bundle_017`, `group_b_bundle_018`, `group_b_bundle_019`, `group_b_bundle_020`
- Group C 180d: `group_c_bundle_022`, `group_c_bundle_023`, `group_c_bundle_024`, `group_c_bundle_025`

Physically this uses 6 pre-agg tables: rollup + distinct tables for Group A, Group B, and Group C.

## Test Conditions

- EC2 host: `ec2-44-242-164-82.us-west-2.compute.amazonaws.com`
- TiDB session engines: `tikv,tidb`; TiFlash excluded.
- Workload: 65 independent bundle SQLs per event, 5% hot-key events, unique event bindings, no writes.
- Read protection: `max_execution_time = 500ms`; client socket timeout `INTUIT_DB_SOCKET_TIMEOUT_SEC=5`.
- Scoring metric: `60/65 completion` is wall-clock time from event dispatch until the 60th successful bundle result is available. This is the score-ready latency for the 500ms fallback model.
- Important interpretation: `60/65 p99` is calculated only for events that reached 60 successful bundle results. The coverage columns show whether events actually reached 60/65 by 350ms and 500ms.

## Summary

| Preference | Run | Achieved eps | Events | >=60/65 by 350ms | >=60/65 by 500ms | 60/65 p95 | 60/65 p99 | 60/65 >500 | 65/65 by 500ms | Evidence |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| P1 runtime-only | 8 eps / 30s | 8.0 | 241 | 238/241 (98.8%) | 241/241 (100.0%) | 233.6ms | 344.4ms | 0 | 221/241 (91.7%) | `mixed_traffic_1780091850.json` |
| P1 runtime-only | 10 eps / 30s | 10.0 | 301 | 269/301 (89.4%) | 272/301 (90.4%) | 185.9ms | 355.0ms | 0 | 154/301 (51.2%) | `mixed_traffic_1780091788.json` |
| P1 runtime-only | 12 eps / 30s | 12.0 | 361 | 341/361 (94.5%) | 345/361 (95.6%) | 226.3ms | 407.6ms | 1 | 189/361 (52.4%) | `mixed_traffic_1780091720.json` |
| P1 runtime-only | 13 eps / 15s | 13.1 | 196 | 86/196 (43.9%) | 111/196 (56.6%) | 724.1ms | 824.2ms | 62 | 19/196 (9.7%) | `mixed_traffic_1780091672.json` |
| P2 180d-only pre-agg | 12 eps / 30s | 12.0 | 360 | 356/360 (98.9%) | 357/360 (99.2%) | 260.9ms | 342.6ms | 2 | 288/360 (80.0%) | `mixed_traffic_1780091427.json` |
| P2 180d-only pre-agg | 13 eps / 15s | 13.1 | 196 | 30/196 (15.3%) | 33/196 (16.8%) | 1480.0ms | 1673.5ms | 168 | 26/196 (13.3%) | `mixed_traffic_1780091325.json` |
| P2 180d-only pre-agg | 15 eps / 15s | 15.0 | 225 | 37/225 (16.4%) | 82/225 (36.4%) | 8168.5ms | 9408.0ms | 214 | 28/225 (12.4%) | `mixed_traffic_1780091225.json` |

## Findings

- Preference 1, runtime-only, is viable at low event QPS. The defensible sustained point from this EC2 run is 8 eps: score-ready p99 was 344.4ms and every event reached 60/65 by 500ms.
- Runtime-only is not stable at 10 eps sustained. It submitted/completed 10 eps, but only 272/301 events reached 60/65 by 500ms.
- Preference 2, 180d-only pre-agg, improves the sustained boundary to roughly 12 eps in this environment. At 12 eps / 30s, score-ready p99 was 342.6ms and 357/360 events reached 60/65 by 500ms.
- 13 eps is past the observed knee for both tested shapes. Queueing and straggler effects appear abruptly, and score-ready latency moves from sub-350ms to seconds.
- The 180d-only pre-agg compromise helps, but it does not change the order of magnitude. We are still far from the stated normal target of 100 events/sec and peak target of 1000 events/sec on this single-EC2 load generator and current SQL shape.

## Recommendation

- Present preference 1 as the clean customer-preferred shape, currently demonstrated at about 8 eps sustained on this environment.
- Present preference 2 as the practical middle ground, currently demonstrated at about 12 eps sustained with near-SLA behavior.
- Do not recommend broader pre-agg yet. The current data says the next bottleneck is the 65-query fan-out and hot-key tail behavior, not simply the absence of more pre-agg tables.
- To reach 100+ events/sec, we likely need a larger architectural change: reduce round trips/fan-out, split hot-key heavy features into fallback or async paths, add more TiDB/TiKV capacity, or introduce a feature-serving/cache layer for heavy long-window aggregates.
