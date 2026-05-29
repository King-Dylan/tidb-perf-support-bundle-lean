# Scan/CASE-Pruning Rewrite Attempts

- Generated: `2026-05-29T13:27:16`
- Mixed JSON: `/Users/dylanliu/Downloads/tidb_intuit_perf_support_bundle_lean/code/results/mixed_traffic_1780029697.json`
- Slow CSV: `/Users/dylanliu/Downloads/tidb_intuit_perf_support_bundle_lean/code/results/slow_bundles_post_index_3eps_60s.csv`
- Session baseline for all tests: TiKV/TiDB only, CTE force-inline off, distinct pushdown off unless candidate timing variant enables it.

## group_b_bundle_012

- Group/window/filter: `B` / `30d` / `d.true_ip = %s`
- Chosen event: `INV0039365135` kind=`hot_true_ip` bundle_ms=`515.3` event_ms=`612.3`
- Bundle stats: n=`181` >350=`1` >500=`1` p95=`151.0ms` max=`515.3ms`

### Current Optimized

- SELECT time: `1814.3ms` result=`ok`
- EXPLAIN ANALYZE: `1949.2ms`, scan_sum=`172835`, scan_max=`172835`

### Candidate: group_b_numeric_projection

- Result check: `same`; SELECT time: `1435.7ms`
- Best EXPLAIN ANALYZE: `1404.6ms` variant=`optimized_hashagg_32_8` scan_sum=`172835` scan_max=`172835` accepted=`True`

| Variant | Time | Scan Sum | Scan Max | Result |
| --- | ---: | ---: | ---: | --- |
| `candidate_default` | 1421.5 ms | 172835 | 172835 | ok |
| `optimized_hashagg_16_8` | 1411.9 ms | 172835 | 172835 | ok |
| `optimized_hashagg_32_8` | 1404.6 ms | 172835 | 172835 | ok |
| `optimized_distinct_pushdown` | 2530.9 ms | 172835 | 172835 | ok |
| `optimized_distinct_pushdown_hashagg_16_8` | 2515.4 ms | 172835 | 172835 | ok |

#### Candidate SQL

```sql
SELECT
  COUNT(*) AS `metric__b_0011`,
  SUM(CASE WHEN d.agent_type = 'browser_computer' THEN 1 ELSE 0 END) AS `metric__b_0057`,
  SUM(CASE WHEN d.agent_type = 'browser_computer' THEN 1 ELSE 0 END) AS `present__b_0057`,
  SUM(CASE WHEN d.agent_type = 'browser_mobile' THEN 1 ELSE 0 END) AS `metric__b_0060`,
  SUM(CASE WHEN d.agent_type = 'browser_mobile' THEN 1 ELSE 0 END) AS `present__b_0060`,
  SUM(CASE WHEN d.agent_type = 'mobile_app' THEN 1 ELSE 0 END) AS `metric__b_0063`,
  SUM(CASE WHEN d.agent_type = 'mobile_app' THEN 1 ELSE 0 END) AS `present__b_0063`,
  SUM(CASE WHEN d.agent_type = 'tablet' THEN 1 ELSE 0 END) AS `metric__b_0066`,
  SUM(CASE WHEN d.agent_type = 'tablet' THEN 1 ELSE 0 END) AS `present__b_0066`,
  SUM(CASE WHEN d.agent_type = 'desktop' THEN 1 ELSE 0 END) AS `metric__b_0069`,
  SUM(CASE WHEN d.agent_type = 'desktop' THEN 1 ELSE 0 END) AS `present__b_0069`,
  SUM(CASE WHEN d.agent_type = 'unknown' THEN 1 ELSE 0 END) AS `metric__b_0072`,
  SUM(CASE WHEN d.agent_type = 'unknown' THEN 1 ELSE 0 END) AS `present__b_0072`,
  COUNT(DISTINCT(d.exact_id)) AS `metric__b_0161`,
  COUNT(DISTINCT(d.smart_id)) AS `metric__b_0165`,
  COUNT(DISTINCT(d.input_ip)) AS `metric__b_0169`,
  COUNT(DISTINCT(d.proxy_ip)) AS `metric__b_0173`,
  COUNT(DISTINCT(d.agent_type)) AS `metric__b_0177`,
  MIN(d.device_score__num) AS `metric__b_0275`,
  SUM(CASE WHEN d.device_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0275`,
  MAX(d.device_score__num) AS `metric__b_0276`,
  SUM(CASE WHEN d.device_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0276`,
  AVG(d.device_score__num) AS `metric__b_0277`,
  SUM(CASE WHEN d.device_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0277`,
  MIN(d.device_fingerprint_score__num) AS `metric__b_0284`,
  SUM(CASE WHEN d.device_fingerprint_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0284`,
  MAX(d.device_fingerprint_score__num) AS `metric__b_0285`,
  SUM(CASE WHEN d.device_fingerprint_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0285`,
  AVG(d.device_fingerprint_score__num) AS `metric__b_0286`,
  SUM(CASE WHEN d.device_fingerprint_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0286`,
  MIN(d.device_worst_score__num) AS `metric__b_0293`,
  SUM(CASE WHEN d.device_worst_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0293`,
  MAX(d.device_worst_score__num) AS `metric__b_0294`,
  SUM(CASE WHEN d.device_worst_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0294`,
  AVG(d.device_worst_score__num) AS `metric__b_0295`,
  SUM(CASE WHEN d.device_worst_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0295`,
  MIN(d.true_ip_score__num) AS `metric__b_0302`,
  SUM(CASE WHEN d.true_ip_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0302`,
  MAX(d.true_ip_score__num) AS `metric__b_0303`,
  SUM(CASE WHEN d.true_ip_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0303`,
  AVG(d.true_ip_score__num) AS `metric__b_0304`,
  SUM(CASE WHEN d.true_ip_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0304`,
  MIN(d.input_ip_score__num) AS `metric__b_0311`,
  SUM(CASE WHEN d.input_ip_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0311`,
  MAX(d.input_ip_score__num) AS `metric__b_0312`,
  SUM(CASE WHEN d.input_ip_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0312`,
  AVG(d.input_ip_score__num) AS `metric__b_0313`,
  SUM(CASE WHEN d.input_ip_score__num IS NOT NULL THEN 1 ELSE 0 END) AS `present__b_0313`
FROM (
  SELECT d.agent_type, d.exact_id, d.smart_id, d.input_ip, d.proxy_ip, CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END AS `device_score__num`, CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END AS `device_fingerprint_score__num`, CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END AS `device_worst_score__num`, CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END AS `true_ip_score__num`, CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END AS `input_ip_score__num`
  FROM deviceprofile_fact d
  WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000'
) d
HAVING COUNT(*) > 0;
```

#### Best Candidate EXPLAIN ANALYZE

```text
-- variant=optimized_hashagg_32_8
-- explain_analyze_elapsed_ms=1404.6
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_8	0.80	1	root		time:1.37s, open:99.6µs, close:42.4µs, loops:2, RU:720.63, Concurrency:OFF	Column#65->Column#97, Column#66->Column#98, Column#66->Column#99, Column#67->Column#100, Column#67->Column#101, Column#68->Column#102, Column#68->Column#103, Column#69->Column#104, Column#69->Column#105, Column#70->Column#106, Column#70->Column#107, Column#71->Column#108, Column#71->Column#109, Column#72->Column#110, Column#73->Column#111, Column#74->Column#112, Column#75->Column#113, Column#76->Column#114, Column#77->Column#115, Column#78->Column#116, Column#79->Column#117, Column#78->Column#118, Column#80->Column#119, Column#78->Column#120, Column#81->Column#121, Column#82->Column#122, Column#83->Column#123, Column#82->Column#124, Column#84->Column#125, Column#82->Column#126, Column#85->Column#127, Column#86->Column#128, Column#87->Column#129, Column#86->Column#130, Column#88->Column#131, Column#86->Column#132, Column#89->Column#133, Column#90->Column#134, Column#91->Column#135, Column#90->Column#136, Column#92->Column#137, Column#90->Column#138, Column#93->Column#139, Column#94->Column#140, Column#95->Column#141, Column#94->Column#142, Column#96->Column#143, Column#94->Column#144	660.6 KB	N/A
└─Selection_10	0.80	1	root		time:1.37s, open:97.1µs, close:40.8µs, loops:2	gt(Column#65, ?)	427.4 KB	N/A
  └─HashAgg_14	1.00	1	root		time:1.37s, open:94.4µs, close:40µs, loops:3	funcs:count(?)->Column#65, funcs:sum(Column#218)->Column#66, funcs:sum(Column#219)->Column#67, funcs:sum(Column#220)->Column#68, funcs:sum(Column#221)->Column#69, funcs:sum(Column#222)->Column#70, funcs:sum(Column#223)->Column#71, funcs:count(distinct Column#224)->Column#72, funcs:count(distinct Column#225)->Column#73, funcs:count(distinct Column#226)->Column#74, funcs:count(distinct Column#227)->Column#75, funcs:count(distinct Column#228)->Column#76, funcs:min(Column#229)->Column#77, funcs:sum(Column#230)->Column#78, funcs:max(Column#231)->Column#79, funcs:avg(Column#232)->Column#80, funcs:min(Column#233)->Column#81, funcs:sum(Column#234)->Column#82, funcs:max(Column#235)->Column#83, funcs:avg(Column#236)->Column#84, funcs:min(Column#237)->Column#85, funcs:sum(Column#238)->Column#86, funcs:max(Column#239)->Column#87, funcs:avg(Column#240)->Column#88, funcs:min(Column#241)->Column#89, funcs:sum(Column#242)->Column#90, funcs:max(Column#243)->Column#91, funcs:avg(Column#244)->Column#92, funcs:min(Column#245)->Column#93, funcs:sum(Column#246)->Column#94, funcs:max(Column#247)->Column#95, funcs:avg(Column#248)->Column#96	23.3 MB	0 Bytes
    └─Projection_29	346177.19	172835	root		time:1.08s, open:83.9µs, close:39.1µs, loops:172, Concurrency:5	cast(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?), decimal(20,0) BINARY)->Column#218, cast(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?), decimal(20,0) BINARY)->Column#219, cast(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?), decimal(20,0) BINARY)->Column#220, cast(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?), decimal(20,0) BINARY)->Column#221, cast(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?), decimal(20,0) BINARY)->Column#222, cast(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?), decimal(20,0) BINARY)->Column#223, intuit_risk.deviceprofile_fact.exact_id->Column#224, intuit_risk.deviceprofile_fact.smart_id->Column#225, intuit_risk.deviceprofile_fact.input_ip->Column#226, intuit_risk.deviceprofile_fact.proxy_ip->Column#227, intuit_risk.deviceprofile_fact.agent_type->Column#228, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY))->Column#229, cast(case(not(isnull(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))), ?, ?), decimal(20,0) BINARY)->Column#230, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY))->Column#231, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY))->Column#232, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY))->Column#233, cast(case(not(isnull(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))), ?, ?), decimal(20,0) BINARY)->Column#234, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY))->Column#235, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY))->Column#236, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY))->Column#237, cast(case(not(isnull(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))), ?, ?), decimal(20,0) BINARY)->Column#238, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY))->Column#239, case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY))->Column#240, case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY))->Column#241, cast(case(not(isnull(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))), ?, ?), decimal(20,0) BINARY)->Column#242, case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY))->Column#243, case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY))->Column#244, case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY))->Column#245, cast(case(not(isnull(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))), ?, ?), decimal(20,0) BINARY)->Column#246, case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY))->Column#247, case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY))->Column#248	6.62 MB	N/A
      └─IndexReader_22	346177.19	172835	root	partition:p20260401,p20260501,p20260601,pmax	time:1.09s, open:82.7µs, close:11.7µs, loops:172, cop_task: {num: 18, max: 1.15s, min: 567.2µs, avg: 69.3ms, p95: 1.15s, max_proc_keys: 105587, p95_proc_keys: 105587, tot_proc: 184.3ms, tot_wait: 589.3µs, copr_cache: disabled, build_task_duration: 31.1µs, max_distsql_concurrency: 4}, fetch_resp_duration: 1.09s, rpc_info:{Cop:{num_rpc:18, total_time:1.25s}}	index:IndexRangeScan_21	21.0 MB	N/A
        └─IndexRangeScan_21	346177.19	172835	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{proc max:120ms, min:0s, avg: 7.22ms, p80:0s, p95:120ms, iters:233, tasks:18}, scan_detail: {total_process_keys: 172835, total_process_keys_size: 42640965, total_keys: 67265, get_snapshot_time: 266µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 184.3ms, total_suspend_time: 171.9µs, total_wait_time: 589.3µs, total_kv_read_wall_time: 10ms}	range:[? ?,? +inf], keep order:false	N/A	N/A
```
