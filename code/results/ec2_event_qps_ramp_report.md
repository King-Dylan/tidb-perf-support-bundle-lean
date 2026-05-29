# EC2 Event-QPS Ramp Validation

Host: `ec2-44-242-164-82.us-west-2.compute.amazonaws.com`  
Workload: prod180 hybrid, TiKV/TiDB only, 65 independent bundle SQLs per event, `READ_MAX_EXECUTION_TIME_MS=500`, unique events from `reuse_events_ec2_70k.json`, `--no-writes`.

## Summary

Follow-up score-ready testing is captured in `ec2_runtime_vs_180d_preagg_preference_report.md`. That newer report separates customer preference 1 runtime-only from preference 2 180d-only pre-agg and uses explicit `60/65 completion` latency.

| Run | Target eps | Achieved eps | Events | Event p50 | Event p95 | Event p99 | >500 events | >=60/65 by 350ms | >=60/65 by 500ms | 65/65 by 500ms | Queue avg p95 | Queue max p95 | Conn wait max p95 | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 3 eps / 30s / pool 256 | 3.0 | 3.0 | 91 | 82.5 | 553.5 | 583.1 | 8 | 91/91 | 91/91 | 83/91 | 7.1 | 13.1 | 0.018 | completed |
| 10 eps / 30s / pool 600 | 10.0 | 10.0 | 301 | 93.0 | 520.7 | 591.3 | 20 | 301/301 | 301/301 | 281/301 | 26.5 | 52.4 | 0.018 | completed |
| 25 eps / 30s / pool 600 | 25.0 | 19.6 | 588 | 11514.1 | 20714.4 | 23896.4 | 586 | 103/588 | 366/588 | 64/588 | 378.7 | 6461.5 | 0.018 | completed but overloaded |

## Observations

- `3 eps` and `10 eps` completed at target rate. Client connection wait stayed near zero, so these runs were not blocked on the DB connection pool.
- `10 eps` met the fallback criterion in this run: `301/301` events had at least `60/65` bundle results by 500ms. Full `65/65` by 500ms was `281/301`.
- `25 eps` overloaded the current end-to-end harness/cluster combination: achieved rate fell to `19.6 eps`, event p50 jumped to `11.5s`, and bundle queue max p95 reached `6.46s`.
- Increasing `25 eps` to `1200` client bundle slots did not recover; it drained even slower and was stopped. This points to cluster/query-side saturation under high fan-out, not just local connection count.
- `12 eps` and `15 eps` with `600` slots also failed to drain promptly after the 30s submit window and were stopped. The practical all-65 fan-in sustainable point in this run is therefore around `10 eps`.

## Important Measurement Caveat

This initial report used event latency that waited for all 65 bundle tasks to finish. The benchmark now records separate score-ready latency as `60/65 completion` and full completion as `65/65 completion`; see `ec2_runtime_vs_180d_preagg_preference_report.md` for the updated SLA-aligned view.

## Result JSONs

- 3 eps / 30s / pool 256: `results/mixed_traffic_1780089553.json`
- 10 eps / 30s / pool 600: `results/mixed_traffic_1780089619.json`
- 25 eps / 30s / pool 600: `results/mixed_traffic_1780089746.json`

## Next Optimization Targets

- Reduce high-fan-out pressure from the remaining slow/tail bundles, especially `group_b_bundle_012`, `group_c_bundle_018`, and 180d distinct helper scans.
- Continue using score-ready metrics from `mixed_traffic_test.py` so event TP99 can be evaluated as `60/65` fallback and `65/65` complete separately.
- For 100+ eps, one EC2 client process is not enough in the current shape. We either need multiple load generators, much lower per-bundle latency, or a server-side/batched execution model that avoids 65 independent round trips per event.
