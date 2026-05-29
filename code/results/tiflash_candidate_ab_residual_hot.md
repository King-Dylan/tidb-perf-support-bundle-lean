# TiKV vs TiFlash Candidate A/B

- Generated: `2026-05-29T22:23:12`
- Mixed JSON: `/home/ec2-user/tidb_intuit_perf_support_bundle_lean/code/results/mixed_traffic_1780091850.json`
- Pre-agg layout: `prod180`
- Pre-agg bundle count: `12`
- Session knobs: distinct_pushdown=`True`, force_inline_cte=`0`, hashagg_final=`16`, hashagg_partial=`8`

## Summary

| bundle | group | event | variant | engines | elapsed | storage tasks | result |
|---|---:|---|---|---|---:|---|---|
| group_b_bundle_012 | B | hot_true_ip:true_ip | tikv | tikv,tidb | 2397.9 ms | cop[tikv], root | ok |
| group_b_bundle_012 | B | hot_true_ip:true_ip | cost | tikv,tiflash,tidb | 2381.9 ms | cop[tikv], root | ok |
| group_b_bundle_012 | B | hot_true_ip:true_ip | tiflash_hint | tikv,tiflash,tidb | 4173.3 ms |  | (1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code |
| group_b_bundle_012 | B | hot_true_ip:true_ip | tiflash_only | tiflash,tidb | 141.8 ms |  | (1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code |
| group_b_bundle_018 | B | hot_input_ip:input_ip | tikv | tikv,tidb | 1243.2 ms | cop[tikv], root | ok |
| group_b_bundle_018 | B | hot_input_ip:input_ip | cost | tikv,tiflash,tidb | 1215.4 ms | cop[tikv], root | ok |
| group_b_bundle_018 | B | hot_input_ip:input_ip | tiflash_hint | tikv,tiflash,tidb | 1230.5 ms | cop[tikv], root | ok |
| group_b_bundle_018 | B | hot_input_ip:input_ip | tiflash_only | tiflash,tidb | 1.8 ms |  | (1815, "Internal : No access path for table 'x' is found with 'tidb_isolation_read_engines' = 'tiflash,tidb', valid valu |
| group_b_bundle_020 | B | hot_true_ip:true_ip | tikv | tikv,tidb | 1272.1 ms | cop[tikv], root | ok |
| group_b_bundle_020 | B | hot_true_ip:true_ip | cost | tikv,tiflash,tidb | 1235.0 ms | cop[tikv], root | ok |
| group_b_bundle_020 | B | hot_true_ip:true_ip | tiflash_hint | tikv,tiflash,tidb | 1209.2 ms | cop[tikv], root | ok |
| group_b_bundle_020 | B | hot_true_ip:true_ip | tiflash_only | tiflash,tidb | 1.9 ms |  | (1815, "Internal : No access path for table 'x' is found with 'tidb_isolation_read_engines' = 'tiflash,tidb', valid valu |
| group_c_bundle_018 | C | hot_true_ip:true_ip | tikv | tikv,tidb | 342.4 ms | cop[tikv], root | ok |
| group_c_bundle_018 | C | hot_true_ip:true_ip | cost | tikv,tiflash,tidb | 339.2 ms | cop[tikv], root | ok |
| group_c_bundle_018 | C | hot_true_ip:true_ip | tiflash_hint | tikv,tiflash,tidb | 10047.1 ms |  | (3024, 'Query execution was interrupted, maximum statement execution time exceeded') |
| group_c_bundle_018 | C | hot_true_ip:true_ip | tiflash_only | tiflash,tidb | 10093.2 ms |  | (3024, 'Query execution was interrupted, maximum statement execution time exceeded') |

## 1. group_b_bundle_012

- Group/window/filter: `B` / `30d` / `d.true_ip = %s`
- Preagg applied: `False`
- Event: invoice=`INV0007589128` kind=`hot_true_ip` hot_field=`true_ip` hot_count=`738824` ref=`2026-04-10T23:06:57.563000`

### Params

```json
[
  "72.153.231.69"
]
```

### tikv

- Engines: `tikv,tidb`
- Elapsed: `2397.9 ms`
- Storage tasks: `cop[tikv], root`

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
  MIN(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END) AS `metric__b_0275`,
  SUM(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN 1 ELSE 0 END) AS `present__b_0275`,
  MAX(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END) AS `metric__b_0276`,
  SUM(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN 1 ELSE 0 END) AS `present__b_0276`,
  AVG(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END) AS `metric__b_0277`,
  SUM(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN 1 ELSE 0 END) AS `present__b_0277`,
  MIN(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END) AS `metric__b_0284`,
  SUM(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN 1 ELSE 0 END) AS `present__b_0284`,
  MAX(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END) AS `metric__b_0285`,
  SUM(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN 1 ELSE 0 END) AS `present__b_0285`,
  AVG(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END) AS `metric__b_0286`,
  SUM(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN 1 ELSE 0 END) AS `present__b_0286`,
  MIN(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END) AS `metric__b_0293`,
  SUM(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN 1 ELSE 0 END) AS `present__b_0293`,
  MAX(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END) AS `metric__b_0294`,
  SUM(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN 1 ELSE 0 END) AS `present__b_0294`,
  AVG(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END) AS `metric__b_0295`,
  SUM(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN 1 ELSE 0 END) AS `present__b_0295`,
  MIN(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0302`,
  SUM(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0302`,
  MAX(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0303`,
  SUM(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0303`,
  AVG(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0304`,
  SUM(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0304`,
  MIN(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0311`,
  SUM(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0311`,
  MAX(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0312`,
  SUM(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0312`,
  AVG(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0313`,
  SUM(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0313`
FROM deviceprofile_fact d
WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 23:06:57.563000'
HAVING COUNT(*) > 0;
```

```text
-- explain_analyze_elapsed_ms=2397.9
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_7	0.80	1	root		time:2.39s, open:179.8µs, close:14.2µs, loops:2, RU:863.81, Concurrency:OFF	Column#60->Column#92, Column#61->Column#93, Column#61->Column#94, Column#62->Column#95, Column#62->Column#96, Column#63->Column#97, Column#63->Column#98, Column#64->Column#99, Column#64->Column#100, Column#65->Column#101, Column#65->Column#102, Column#66->Column#103, Column#66->Column#104, Column#67->Column#105, Column#68->Column#106, Column#69->Column#107, Column#70->Column#108, Column#71->Column#109, Column#72->Column#110, Column#73->Column#111, Column#74->Column#112, Column#73->Column#113, Column#75->Column#114, Column#73->Column#115, Column#76->Column#116, Column#77->Column#117, Column#78->Column#118, Column#77->Column#119, Column#79->Column#120, Column#77->Column#121, Column#80->Column#122, Column#81->Column#123, Column#82->Column#124, Column#81->Column#125, Column#83->Column#126, Column#81->Column#127, Column#84->Column#128, Column#85->Column#129, Column#86->Column#130, Column#85->Column#131, Column#87->Column#132, Column#85->Column#133, Column#88->Column#134, Column#89->Column#135, Column#90->Column#136, Column#89->Column#137, Column#91->Column#138, Column#89->Column#139	38.7 KB	N/A
└─Selection_9	0.80	1	root		time:2.39s, open:166.2µs, close:12.3µs, loops:2	gt(Column#60, ?)	38.7 KB	N/A
  └─HashAgg_16	1.00	1	root		time:2.39s, open:153.5µs, close:11.8µs, loops:3	funcs:count(Column#141)->Column#60, funcs:sum(Column#142)->Column#61, funcs:sum(Column#143)->Column#62, funcs:sum(Column#144)->Column#63, funcs:sum(Column#145)->Column#64, funcs:sum(Column#146)->Column#65, funcs:sum(Column#147)->Column#66, funcs:count(distinct intuit_risk.deviceprofile_fact.exact_id)->Column#67, funcs:count(distinct intuit_risk.deviceprofile_fact.smart_id)->Column#68, funcs:count(distinct intuit_risk.deviceprofile_fact.input_ip)->Column#69, funcs:count(distinct intuit_risk.deviceprofile_fact.proxy_ip)->Column#70, funcs:count(distinct intuit_risk.deviceprofile_fact.agent_type)->Column#71, funcs:min(Column#148)->Column#72, funcs:sum(Column#149)->Column#73, funcs:max(Column#150)->Column#74, funcs:avg(Column#151, Column#152)->Column#75, funcs:min(Column#153)->Column#76, funcs:sum(Column#154)->Column#77, funcs:max(Column#155)->Column#78, funcs:avg(Column#156, Column#157)->Column#79, funcs:min(Column#158)->Column#80, funcs:sum(Column#159)->Column#81, funcs:max(Column#160)->Column#82, funcs:avg(Column#161, Column#162)->Column#83, funcs:min(Column#163)->Column#84, funcs:sum(Column#164)->Column#85, funcs:max(Column#165)->Column#86, funcs:avg(Column#166, Column#167)->Column#87, funcs:min(Column#168)->Column#88, funcs:sum(Column#169)->Column#89, funcs:max(Column#170)->Column#90, funcs:avg(Column#171, Column#172)->Column#91	152.0 MB	0 Bytes
    └─IndexReader_17	1.00	167600	root	partition:p20260401,p20260501,p20260601,pmax	time:2.09s, open:114.2µs, close:11.1µs, loops:11, cop_task: {num: 12, max: 2.19s, min: 1.36ms, avg: 244.2ms, p95: 2.19s, max_proc_keys: 116680, p95_proc_keys: 116680, tot_proc: 716.8ms, tot_wait: 3.73ms, copr_cache: disabled, build_task_duration: 38.3µs, max_distsql_concurrency: 4}, fetch_resp_duration: 2.09s, rpc_info:{Cop:{num_rpc:12, total_time:2.93s}}	index:HashAgg_11	147.1 MB	N/A
      └─HashAgg_11	1.00	167600	cop[tikv]		tikv_task:{proc max:1.08s, min:0s, avg: 133.3ms, p80:140ms, p95:1.08s, iters:171, tasks:12}, scan_detail: {total_process_keys: 172346, total_process_keys_size: 40578683, total_keys: 55677, get_snapshot_time: 4.54ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 716.8ms, total_suspend_time: 392.8µs, total_wait_time: 3.73ms, total_kv_read_wall_time: 30ms}	group by:intuit_risk.deviceprofile_fact.agent_type, intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.smart_id, funcs:count(?)->Column#141, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#142, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#143, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#144, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#145, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#146, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#147, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#148, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), ?, ?))->Column#149, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#150, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#151, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#152, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#153, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), ?, ?))->Column#154, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#155, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#156, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#157, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#158, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), ?, ?))->Column#159, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#160, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#161, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#162, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#163, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), ?, ?))->Column#164, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#165, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#166, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#167, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#168, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), ?, ?))->Column#169, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#170, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#171, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#172	N/A	N/A
        └─IndexRangeScan_15	359561.64	172346	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{proc max:130ms, min:0s, avg: 13.3ms, p80:10ms, p95:130ms, iters:171, tasks:12}	range:[? ?,? +inf], keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `2381.9 ms`
- Storage tasks: `cop[tikv], root`

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
  MIN(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END) AS `metric__b_0275`,
  SUM(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN 1 ELSE 0 END) AS `present__b_0275`,
  MAX(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END) AS `metric__b_0276`,
  SUM(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN 1 ELSE 0 END) AS `present__b_0276`,
  AVG(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END) AS `metric__b_0277`,
  SUM(CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN 1 ELSE 0 END) AS `present__b_0277`,
  MIN(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END) AS `metric__b_0284`,
  SUM(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN 1 ELSE 0 END) AS `present__b_0284`,
  MAX(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END) AS `metric__b_0285`,
  SUM(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN 1 ELSE 0 END) AS `present__b_0285`,
  AVG(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END) AS `metric__b_0286`,
  SUM(CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN 1 ELSE 0 END) AS `present__b_0286`,
  MIN(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END) AS `metric__b_0293`,
  SUM(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN 1 ELSE 0 END) AS `present__b_0293`,
  MAX(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END) AS `metric__b_0294`,
  SUM(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN 1 ELSE 0 END) AS `present__b_0294`,
  AVG(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END) AS `metric__b_0295`,
  SUM(CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN 1 ELSE 0 END) AS `present__b_0295`,
  MIN(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0302`,
  SUM(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0302`,
  MAX(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0303`,
  SUM(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0303`,
  AVG(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0304`,
  SUM(CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0304`,
  MIN(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0311`,
  SUM(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0311`,
  MAX(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0312`,
  SUM(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0312`,
  AVG(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END) AS `metric__b_0313`,
  SUM(CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN 1 ELSE 0 END) AS `present__b_0313`
FROM deviceprofile_fact d
WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 23:06:57.563000'
HAVING COUNT(*) > 0;
```

```text
-- explain_analyze_elapsed_ms=2381.9
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_7	0.80	1	root		time:2.38s, open:141.6µs, close:12.9µs, loops:2, RU:868.84, Concurrency:OFF	Column#60->Column#92, Column#61->Column#93, Column#61->Column#94, Column#62->Column#95, Column#62->Column#96, Column#63->Column#97, Column#63->Column#98, Column#64->Column#99, Column#64->Column#100, Column#65->Column#101, Column#65->Column#102, Column#66->Column#103, Column#66->Column#104, Column#67->Column#105, Column#68->Column#106, Column#69->Column#107, Column#70->Column#108, Column#71->Column#109, Column#72->Column#110, Column#73->Column#111, Column#74->Column#112, Column#73->Column#113, Column#75->Column#114, Column#73->Column#115, Column#76->Column#116, Column#77->Column#117, Column#78->Column#118, Column#77->Column#119, Column#79->Column#120, Column#77->Column#121, Column#80->Column#122, Column#81->Column#123, Column#82->Column#124, Column#81->Column#125, Column#83->Column#126, Column#81->Column#127, Column#84->Column#128, Column#85->Column#129, Column#86->Column#130, Column#85->Column#131, Column#87->Column#132, Column#85->Column#133, Column#88->Column#134, Column#89->Column#135, Column#90->Column#136, Column#89->Column#137, Column#91->Column#138, Column#89->Column#139	26.5 KB	N/A
└─Selection_9	0.80	1	root		time:2.38s, open:138.1µs, close:11.1µs, loops:2	gt(Column#60, ?)	38.7 KB	N/A
  └─HashAgg_17	1.00	1	root		time:2.38s, open:135.2µs, close:10.6µs, loops:3	funcs:count(Column#141)->Column#60, funcs:sum(Column#142)->Column#61, funcs:sum(Column#143)->Column#62, funcs:sum(Column#144)->Column#63, funcs:sum(Column#145)->Column#64, funcs:sum(Column#146)->Column#65, funcs:sum(Column#147)->Column#66, funcs:count(distinct intuit_risk.deviceprofile_fact.exact_id)->Column#67, funcs:count(distinct intuit_risk.deviceprofile_fact.smart_id)->Column#68, funcs:count(distinct intuit_risk.deviceprofile_fact.input_ip)->Column#69, funcs:count(distinct intuit_risk.deviceprofile_fact.proxy_ip)->Column#70, funcs:count(distinct intuit_risk.deviceprofile_fact.agent_type)->Column#71, funcs:min(Column#148)->Column#72, funcs:sum(Column#149)->Column#73, funcs:max(Column#150)->Column#74, funcs:avg(Column#151, Column#152)->Column#75, funcs:min(Column#153)->Column#76, funcs:sum(Column#154)->Column#77, funcs:max(Column#155)->Column#78, funcs:avg(Column#156, Column#157)->Column#79, funcs:min(Column#158)->Column#80, funcs:sum(Column#159)->Column#81, funcs:max(Column#160)->Column#82, funcs:avg(Column#161, Column#162)->Column#83, funcs:min(Column#163)->Column#84, funcs:sum(Column#164)->Column#85, funcs:max(Column#165)->Column#86, funcs:avg(Column#166, Column#167)->Column#87, funcs:min(Column#168)->Column#88, funcs:sum(Column#169)->Column#89, funcs:max(Column#170)->Column#90, funcs:avg(Column#171, Column#172)->Column#91	152.0 MB	0 Bytes
    └─IndexReader_18	1.00	167600	root	partition:p20260401,p20260501,p20260601,pmax	time:2.08s, open:122.2µs, close:9.85µs, loops:11, cop_task: {num: 12, max: 2.17s, min: 1.08ms, avg: 242.8ms, p95: 2.17s, max_proc_keys: 116680, p95_proc_keys: 116680, tot_proc: 731.9ms, tot_wait: 510.1µs, copr_cache: disabled, build_task_duration: 36.8µs, max_distsql_concurrency: 4}, fetch_resp_duration: 2.08s, rpc_info:{Cop:{num_rpc:12, total_time:2.91s}}	index:HashAgg_11	147.1 MB	N/A
      └─HashAgg_11	1.00	167600	cop[tikv]		tikv_task:{proc max:1.08s, min:0s, avg: 129.2ms, p80:140ms, p95:1.08s, iters:171, tasks:12}, scan_detail: {total_process_keys: 172346, total_process_keys_size: 40578683, total_keys: 55677, get_snapshot_time: 272.3µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 731.9ms, total_suspend_time: 419.8µs, total_wait_time: 510.1µs, total_kv_read_wall_time: 40ms}	group by:intuit_risk.deviceprofile_fact.agent_type, intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.smart_id, funcs:count(?)->Column#141, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#142, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#143, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#144, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#145, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#146, funcs:sum(case(eq(intuit_risk.deviceprofile_fact.agent_type, ?), ?, ?))->Column#147, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#148, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), ?, ?))->Column#149, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#150, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#151, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_score)), ne(intuit_risk.deviceprofile_fact.device_score, ?)), cast(intuit_risk.deviceprofile_fact.device_score, decimal(10,2) BINARY)))->Column#152, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#153, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), ?, ?))->Column#154, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#155, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#156, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_fingerprint_score)), ne(intuit_risk.deviceprofile_fact.device_fingerprint_score, ?)), cast(intuit_risk.deviceprofile_fact.device_fingerprint_score, decimal(10,2) BINARY)))->Column#157, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#158, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), ?, ?))->Column#159, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#160, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#161, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.device_worst_score)), ne(intuit_risk.deviceprofile_fact.device_worst_score, ?)), cast(intuit_risk.deviceprofile_fact.device_worst_score, decimal(10,2) BINARY)))->Column#162, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#163, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), ?, ?))->Column#164, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#165, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#166, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.true_ip_score)), ne(intuit_risk.deviceprofile_fact.true_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.true_ip_score, decimal(10,2) BINARY)))->Column#167, funcs:min(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#168, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), ?, ?))->Column#169, funcs:max(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#170, funcs:count(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#171, funcs:sum(case(and(not(isnull(intuit_risk.deviceprofile_fact.input_ip_score)), ne(intuit_risk.deviceprofile_fact.input_ip_score, ?)), cast(intuit_risk.deviceprofile_fact.input_ip_score, decimal(10,2) BINARY)))->Column#172	N/A	N/A
        └─IndexRangeScan_16	359561.64	172346	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{proc max:130ms, min:0s, avg: 14.2ms, p80:10ms, p95:130ms, iters:171, tasks:12}	range:[? ?,? +inf], keep order:false	N/A	N/A
```

### tiflash_hint

- Engines: `tikv,tiflash,tidb`
- Elapsed: `4173.3 ms`
- Error: `(1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code: 0, e.displayText() = DB::Exception: Memory limit (total) exceeded caused by 'RSS(Resident Set Size) much larger than limit' : process memory size would be 4.77 GiB for (attempt to allocate chunk of 1050121 bytes), limit of memory for data computing : 3.76 GiB. Memory Usage of Storage: non-query: peak=0.00 B, amount=0.00 B; kvstore: peak=0.00 B, amount=0.00 B; query-storage-task: peak=977.21 MiB, amount=97.68 MiB; fetch-pages: peak=1.48 MiB, amount=0.00 B; shared-column-data: peak=977.21 MiB, amount=97.68 MiB., e.what() = DB::Exception,, e.what() = DB::Exception,")`

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `141.8 ms`
- Error: `(1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code: 0, e.displayText() = DB::Exception: Memory limit (total) exceeded caused by 'RSS(Resident Set Size) much larger than limit' : process memory size would be 4.77 GiB for (attempt to allocate chunk of 1564779 bytes), limit of memory for data computing : 3.76 GiB. Memory Usage of Storage: non-query: peak=0.00 B, amount=0.00 B; kvstore: peak=0.00 B, amount=0.00 B; query-storage-task: peak=977.21 MiB, amount=3.52 MiB; fetch-pages: peak=1.48 MiB, amount=0.00 B; shared-column-data: peak=977.21 MiB, amount=3.52 MiB., e.what() = DB::Exception,, e.what() = DB::Exception,")`

## 2. group_b_bundle_018

- Group/window/filter: `B` / `180d` / `d.input_ip = %s`
- Preagg applied: `True`
- Event: invoice=`INV0019249439` kind=`hot_input_ip` hot_field=`input_ip` hot_count=`719377` ref=`2026-04-10T22:19:54.592000`

### Params

```json
[
  "74.179.68.52",
  "74.179.68.52"
]
```

### tikv

- Engines: `tikv,tidb`
- Elapsed: `1243.2 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.exact_id AS `raw_distinct_0`,
    d.smart_id AS `raw_distinct_1`,
    d.true_ip AS `raw_distinct_2`,
    d.agent_type AS `raw_distinct_3`
  FROM deviceprofile_fact d
  WHERE d.input_ip = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 22:19:54.592000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_018'
    AND x.template_id IN ('b_0146', 'b_0150', 'b_0154', 'b_0158')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0146' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0150' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0154' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0158' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
)
SELECT
  COUNT(DISTINCT CASE WHEN template_id = 'b_0146' THEN distinct_value END) AS `metric__b_0146`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0150' THEN distinct_value END) AS `metric__b_0150`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0154' THEN distinct_value END) AS `metric__b_0154`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0158' THEN distinct_value END) AS `metric__b_0158`
FROM distinct_values;
```

```text
-- explain_analyze_elapsed_ms=1243.2
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_63	1.00	1	root		time:1.24s, open:12.6µs, close:62.1µs, loops:2, RU:3449.65	funcs:count(distinct Column#161)->Column#128, funcs:count(distinct Column#162)->Column#129, funcs:count(distinct Column#163)->Column#130, funcs:count(distinct Column#164)->Column#131	56.9 MB	0 Bytes
└─Projection_106	2340270.98	1533348	root		time:547ms, open:2.1µs, close:61µs, loops:1502, Concurrency:5	case(eq(Column#126, ?), Column#127)->Column#161, case(eq(Column#126, ?), Column#127)->Column#162, case(eq(Column#126, ?), Column#127)->Column#163, case(eq(Column#126, ?), Column#127)->Column#164	881.7 KB	N/A
  └─Union_65	2340270.98	1533348	root		time:558.5ms, open:834ns, close:37.8µs, loops:1502		N/A	N/A
    ├─Projection_67	2340266.27	1533348	root		time:554.8ms, open:109.9µs, close:21.8µs, loops:1502, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#126, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	909.4 KB	N/A
    │ └─IndexReader_70	2340266.27	1533348	root		time:560.9ms, open:108.3µs, close:12.5µs, loops:1502, cop_task: {num: 29, max: 529.6ms, min: 1.22ms, avg: 83.3ms, p95: 509.7ms, max_proc_keys: 345056, p95_proc_keys: 289760, tot_proc: 1.07s, tot_wait: 3.99ms, copr_cache: disabled, build_task_duration: 43.9µs, max_distsql_concurrency: 4}, fetch_resp_duration: 555.7ms, rpc_info:{Cop:{num_rpc:30, total_time:2.42s}, rpc_errors:{not_leader:1}}, backoff{regionMiss: 16ms}	index:IndexRangeScan_69	96.2 MB	N/A
    │   └─IndexRangeScan_69	2340266.27	1533348	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:210ms, min:0s, avg: 35.9ms, p80:30ms, p95:200ms, iters:1610, tasks:29}, scan_detail: {total_process_keys: 1533348, total_process_keys_size: 201682727, total_keys: 169419, get_snapshot_time: 3.64ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 1.07s, total_suspend_time: 421.6µs, total_wait_time: 3.99ms, total_kv_read_wall_time: 170ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_71	1.18	0	root		time:1.13ms, open:80.4µs, close:10.2µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_73	1.18	0	root		time:1.13ms, open:77.1µs, close:8.22µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	2.48 KB	N/A
    │   └─CTEFullScan_75	1.47	0	root	CTE:raw_boundary	time:1.12ms, open:72.8µs, close:6.9µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_79	1.18	0	root		time:1.13ms, open:1.12ms, close:1.73µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_81	1.18	0	root		time:1.12ms, open:1.12ms, close:500ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	2.48 KB	N/A
    │   └─CTEFullScan_83	1.47	0	root	CTE:raw_boundary	time:1.12ms, open:1.11ms, close:77ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_87	1.18	0	root		time:1.13ms, open:1.13ms, close:1.42µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_89	1.18	0	root		time:1.13ms, open:1.13ms, close:495ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	2.48 KB	N/A
    │   └─CTEFullScan_91	1.47	0	root	CTE:raw_boundary	time:1.12ms, open:1.12ms, close:103ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_95	1.18	0	root		time:1.14ms, open:1.14ms, close:1.37µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
      └─Selection_97	1.18	0	root		time:1.14ms, open:1.13ms, close:470ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	2.48 KB	N/A
        └─CTEFullScan_99	1.47	0	root	CTE:raw_boundary	time:1.13ms, open:1.13ms, close:110ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.47	0	root		time:1.12ms, open:72.8µs, close:6.9µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_50(Seed Part)	1.47	0	root		time:1.1ms, open:69.4µs, close:5.17µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.agent_type	3.48 KB	N/A
  └─IndexReader_55	1.83	0	root	partition:p20251101	time:1.09ms, open:63µs, close:3.89µs, loops:1, cop_task: {num: 1, max: 1ms, proc_keys: 0, tot_proc: 27.3µs, tot_wait: 531.2µs, copr_cache: disabled, build_task_duration: 23µs, max_distsql_concurrency: 1}, fetch_resp_duration: 1.02ms, rpc_info:{Cop:{num_rpc:1, total_time:984.7µs}}	index:Selection_54	255 Bytes	N/A
    └─Selection_54	1.83	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 499.7µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 27.3µs, total_wait_time: 531.2µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type))))	N/A	N/A
      └─IndexRangeScan_53	1.84	0	cop[tikv]	table:d, index:idx_dev_input_runtime_cov(input_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip_score, smart_id, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1215.4 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.exact_id AS `raw_distinct_0`,
    d.smart_id AS `raw_distinct_1`,
    d.true_ip AS `raw_distinct_2`,
    d.agent_type AS `raw_distinct_3`
  FROM deviceprofile_fact d
  WHERE d.input_ip = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 22:19:54.592000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_018'
    AND x.template_id IN ('b_0146', 'b_0150', 'b_0154', 'b_0158')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0146' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0150' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0154' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0158' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
)
SELECT
  COUNT(DISTINCT CASE WHEN template_id = 'b_0146' THEN distinct_value END) AS `metric__b_0146`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0150' THEN distinct_value END) AS `metric__b_0150`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0154' THEN distinct_value END) AS `metric__b_0154`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0158' THEN distinct_value END) AS `metric__b_0158`
FROM distinct_values;
```

```text
-- explain_analyze_elapsed_ms=1215.4
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_72	1.00	1	root		time:1.21s, open:9.9µs, close:70.2µs, loops:2, RU:3426.50	funcs:count(distinct Column#161)->Column#128, funcs:count(distinct Column#162)->Column#129, funcs:count(distinct Column#163)->Column#130, funcs:count(distinct Column#164)->Column#131	56.9 MB	0 Bytes
└─Projection_115	2340270.98	1533348	root		time:530.4ms, open:2.32µs, close:69µs, loops:1502, Concurrency:5	case(eq(Column#126, ?), Column#127)->Column#161, case(eq(Column#126, ?), Column#127)->Column#162, case(eq(Column#126, ?), Column#127)->Column#163, case(eq(Column#126, ?), Column#127)->Column#164	891.2 KB	N/A
  └─Union_74	2340270.98	1533348	root		time:539.8ms, open:806ns, close:47.2µs, loops:1502		N/A	N/A
    ├─Projection_76	2340266.27	1533348	root		time:540.4ms, open:114.5µs, close:26.6µs, loops:1502, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#126, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	908.1 KB	N/A
    │ └─IndexReader_79	2340266.27	1533348	root		time:545.7ms, open:113.6µs, close:9.53µs, loops:1502, cop_task: {num: 29, max: 479.6ms, min: 1.17ms, avg: 79ms, p95: 472.2ms, max_proc_keys: 345056, p95_proc_keys: 289760, tot_proc: 1s, tot_wait: 725.8µs, copr_cache: disabled, build_task_duration: 44.1µs, max_distsql_concurrency: 4}, fetch_resp_duration: 540.7ms, rpc_info:{Cop:{num_rpc:29, total_time:2.29s}}	index:IndexRangeScan_78	96.2 MB	N/A
    │   └─IndexRangeScan_78	2340266.27	1533348	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:200ms, min:0s, avg: 33.4ms, p80:30ms, p95:180ms, iters:1610, tasks:29}, scan_detail: {total_process_keys: 1533348, total_process_keys_size: 201682727, total_keys: 169419, get_snapshot_time: 379.6µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 1s, total_suspend_time: 394.1µs, total_wait_time: 725.8µs, total_kv_read_wall_time: 140ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_80	1.18	0	root		time:639.9µs, open:626.3µs, close:12µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	39.7 KB	N/A
    │ └─Selection_82	1.18	0	root		time:633µs, open:622.3µs, close:9.6µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	2.48 KB	N/A
    │   └─CTEFullScan_84	1.47	0	root	CTE:raw_boundary	time:626.8µs, open:618.1µs, close:8.09µs, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_88	1.18	0	root		time:613µs, open:93.1µs, close:2.4µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_90	1.18	0	root		time:606.5µs, open:89.6µs, close:669ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	2.48 KB	N/A
    │   └─CTEFullScan_92	1.47	0	root	CTE:raw_boundary	time:600.2µs, open:85.8µs, close:176ns, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_96	1.18	0	root		time:616.9µs, open:610.9µs, close:2.66µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_98	1.18	0	root		time:611.7µs, open:608.5µs, close:1.08µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	2.48 KB	N/A
    │   └─CTEFullScan_100	1.47	0	root	CTE:raw_boundary	time:604.3µs, open:603.1µs, close:168ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_104	1.18	0	root		time:620.1µs, open:616.5µs, close:1.86µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	3.11 KB	N/A
      └─Selection_106	1.18	0	root		time:616.9µs, open:615.1µs, close:649ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	6.86 KB	N/A
        └─CTEFullScan_108	1.47	0	root	CTE:raw_boundary	time:613.2µs, open:612.6µs, close:74ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.47	0	root		time:626.8µs, open:618.1µs, close:8.09µs, loops:1	Non-Recursive CTE	N/A	N/A
└─Projection_50(Seed Part)	1.47	0	root		time:590.3µs, open:82.8µs, close:6.1µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.agent_type	3.48 KB	N/A
  └─IndexReader_59	1.83	0	root	partition:p20251101	time:581.6µs, open:77.4µs, close:4.11µs, loops:1, cop_task: {num: 1, max: 470.5µs, proc_keys: 0, tot_proc: 28.8µs, tot_wait: 48.3µs, copr_cache: disabled, build_task_duration: 23.9µs, max_distsql_concurrency: 1}, fetch_resp_duration: 486.7µs, rpc_info:{Cop:{num_rpc:1, total_time:453.8µs}}	index:Selection_58	255 Bytes	N/A
    └─Selection_58	1.83	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 24.6µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 28.8µs, total_wait_time: 48.3µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type))))	N/A	N/A
      └─IndexRangeScan_57	1.84	0	cop[tikv]	table:d, index:idx_dev_input_runtime_cov(input_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip_score, smart_id, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_hint

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1230.5 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.exact_id AS `raw_distinct_0`,
    d.smart_id AS `raw_distinct_1`,
    d.true_ip AS `raw_distinct_2`,
    d.agent_type AS `raw_distinct_3`
  FROM deviceprofile_fact d
  WHERE d.input_ip = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 22:19:54.592000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_018'
    AND x.template_id IN ('b_0146', 'b_0150', 'b_0154', 'b_0158')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0146' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0150' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0154' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0158' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
)
SELECT
  COUNT(DISTINCT CASE WHEN template_id = 'b_0146' THEN distinct_value END) AS `metric__b_0146`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0150' THEN distinct_value END) AS `metric__b_0150`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0154' THEN distinct_value END) AS `metric__b_0154`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0158' THEN distinct_value END) AS `metric__b_0158`
FROM distinct_values;
```

```text
-- explain_analyze_elapsed_ms=1230.5
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_72	1.00	1	root		time:1.22s, open:9.31µs, close:61.2µs, loops:2, RU:3422.60	funcs:count(distinct Column#161)->Column#128, funcs:count(distinct Column#162)->Column#129, funcs:count(distinct Column#163)->Column#130, funcs:count(distinct Column#164)->Column#131	56.9 MB	0 Bytes
└─Projection_115	2340270.98	1533348	root		time:517.3ms, open:2.05µs, close:60.1µs, loops:1502, Concurrency:5	case(eq(Column#126, ?), Column#127)->Column#161, case(eq(Column#126, ?), Column#127)->Column#162, case(eq(Column#126, ?), Column#127)->Column#163, case(eq(Column#126, ?), Column#127)->Column#164	888.9 KB	N/A
  └─Union_74	2340270.98	1533348	root		time:526.1ms, open:863ns, close:38.7µs, loops:1502		N/A	N/A
    ├─Projection_76	2340266.27	1533348	root		time:526.8ms, open:118.2µs, close:19.5µs, loops:1502, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#126, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	908.1 KB	N/A
    │ └─IndexReader_79	2340266.27	1533348	root		time:532.4ms, open:116.9µs, close:11.6µs, loops:1502, cop_task: {num: 29, max: 477ms, min: 1.06ms, avg: 78.4ms, p95: 470.5ms, max_proc_keys: 345056, p95_proc_keys: 289760, tot_proc: 992.7ms, tot_wait: 726.5µs, copr_cache: disabled, build_task_duration: 45.7µs, max_distsql_concurrency: 4}, fetch_resp_duration: 527.3ms, rpc_info:{Cop:{num_rpc:29, total_time:2.27s}}	index:IndexRangeScan_78	96.2 MB	N/A
    │   └─IndexRangeScan_78	2340266.27	1533348	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:210ms, min:0s, avg: 32.8ms, p80:30ms, p95:190ms, iters:1610, tasks:29}, scan_detail: {total_process_keys: 1533348, total_process_keys_size: 201682727, total_keys: 169419, get_snapshot_time: 378.8µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 992.7ms, total_suspend_time: 395.4µs, total_wait_time: 726.5µs, total_kv_read_wall_time: 130ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_80	1.18	0	root		time:616.8µs, open:81.1µs, close:11.1µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_82	1.18	0	root		time:609.2µs, open:77.3µs, close:8.8µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	2.48 KB	N/A
    │   └─CTEFullScan_84	1.47	0	root	CTE:raw_boundary	time:602.2µs, open:73.3µs, close:7.42µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_88	1.18	0	root		time:613.6µs, open:607.3µs, close:2.99µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_90	1.18	0	root		time:608.7µs, open:605.4µs, close:974ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	2.48 KB	N/A
    │   └─CTEFullScan_92	1.47	0	root	CTE:raw_boundary	time:601µs, open:599.8µs, close:86ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_96	1.18	0	root		time:617.4µs, open:613.3µs, close:2.05µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	3.11 KB	N/A
    │ └─Selection_98	1.18	0	root		time:613.8µs, open:611.5µs, close:1.06µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	6.48 KB	N/A
    │   └─CTEFullScan_100	1.47	0	root	CTE:raw_boundary	time:609.9µs, open:609.3µs, close:133ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_104	1.18	0	root		time:625.6µs, open:621.6µs, close:1.96µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	39.7 KB	N/A
      └─Selection_106	1.18	0	root		time:620.4µs, open:618.2µs, close:806ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	2.48 KB	N/A
        └─CTEFullScan_108	1.47	0	root	CTE:raw_boundary	time:614.9µs, open:614.2µs, close:134ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.47	0	root		time:602.2µs, open:73.3µs, close:7.42µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_50(Seed Part)	1.47	0	root		time:584µs, open:69.7µs, close:5.58µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.agent_type	3.48 KB	N/A
  └─IndexReader_59	1.83	0	root	partition:p20251101	time:577.8µs, open:66.2µs, close:4.15µs, loops:1, cop_task: {num: 1, max: 481.7µs, proc_keys: 0, tot_proc: 29.2µs, tot_wait: 45.4µs, copr_cache: disabled, build_task_duration: 21.9µs, max_distsql_concurrency: 1}, fetch_resp_duration: 495.6µs, rpc_info:{Cop:{num_rpc:1, total_time:463.5µs}}	index:Selection_58	255 Bytes	N/A
    └─Selection_58	1.83	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 23.4µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 29.2µs, total_wait_time: 45.4µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type))))	N/A	N/A
      └─IndexRangeScan_57	1.84	0	cop[tikv]	table:d, index:idx_dev_input_runtime_cov(input_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip_score, smart_id, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `1.8 ms`
- Error: `(1815, "Internal : No access path for table 'x' is found with 'tidb_isolation_read_engines' = 'tiflash,tidb', valid values can be 'tikv'.")`

## 3. group_b_bundle_020

- Group/window/filter: `B` / `180d` / `d.true_ip = %s`
- Preagg applied: `True`
- Event: invoice=`INV0007589128` kind=`hot_true_ip` hot_field=`true_ip` hot_count=`738824` ref=`2026-04-10T23:06:57.563000`

### Params

```json
[
  "72.153.231.69",
  "72.153.231.69"
]
```

### tikv

- Engines: `tikv,tidb`
- Elapsed: `1272.1 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.exact_id AS `raw_distinct_0`,
    d.smart_id AS `raw_distinct_1`,
    d.input_ip AS `raw_distinct_2`,
    d.proxy_ip AS `raw_distinct_3`,
    d.agent_type AS `raw_distinct_4`
  FROM deviceprofile_fact d
  WHERE d.true_ip = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 23:06:57.563000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_020'
    AND x.template_id IN ('b_0162', 'b_0166', 'b_0170', 'b_0174', 'b_0178')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0162' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0166' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0170' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0174' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
  UNION ALL
  SELECT 'b_0178' AS template_id, CAST(`raw_distinct_4` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_4` IS NOT NULL
)
SELECT
  COUNT(DISTINCT CASE WHEN template_id = 'b_0162' THEN distinct_value END) AS `metric__b_0162`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0166' THEN distinct_value END) AS `metric__b_0166`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0170' THEN distinct_value END) AS `metric__b_0170`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0174' THEN distinct_value END) AS `metric__b_0174`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0178' THEN distinct_value END) AS `metric__b_0178`
FROM distinct_values;
```

```text
-- explain_analyze_elapsed_ms=1272.1
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_71	1.00	1	root		time:1.27s, open:11.2µs, close:63µs, loops:2, RU:3557.26	funcs:count(distinct Column#188)->Column#150, funcs:count(distinct Column#189)->Column#151, funcs:count(distinct Column#190)->Column#152, funcs:count(distinct Column#191)->Column#153, funcs:count(distinct Column#192)->Column#154	57.1 MB	0 Bytes
└─Projection_122	2121046.92	1557792	root		time:525.2ms, open:2.22µs, close:62.2µs, loops:1527, Concurrency:5	case(eq(Column#148, ?), Column#149)->Column#188, case(eq(Column#148, ?), Column#149)->Column#189, case(eq(Column#148, ?), Column#149)->Column#190, case(eq(Column#148, ?), Column#149)->Column#191, case(eq(Column#148, ?), Column#149)->Column#192	1011.5 KB	N/A
  └─Union_73	2121046.92	1557792	root		time:540.5ms, open:887ns, close:41.6µs, loops:1527		N/A	N/A
    ├─Projection_75	2121041.42	1557792	root		time:539.9ms, open:118.3µs, close:22.5µs, loops:1527, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#148, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	950.1 KB	N/A
    │ └─IndexReader_78	2121041.42	1557792	root		time:547.7ms, open:117µs, close:12.2µs, loops:1527, cop_task: {num: 38, max: 478ms, min: 1.1ms, avg: 63.2ms, p95: 468.8ms, max_proc_keys: 341984, p95_proc_keys: 286688, tot_proc: 1.09s, tot_wait: 5.74ms, copr_cache: disabled, build_task_duration: 48.6µs, max_distsql_concurrency: 5}, fetch_resp_duration: 541.8ms, rpc_info:{Cop:{num_rpc:40, total_time:2.4s}, rpc_errors:{not_leader:2}}, backoff{regionMiss: 54ms}	index:IndexRangeScan_77	96.1 MB	N/A
    │   └─IndexRangeScan_77	2121041.42	1557792	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:190ms, min:0s, avg: 27.9ms, p80:30ms, p95:190ms, iters:1668, tasks:38}, scan_detail: {total_process_keys: 1557792, total_process_keys_size: 208062197, total_keys: 198097, get_snapshot_time: 5.22ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 1.09s, total_suspend_time: 467.5µs, total_wait_time: 5.74ms, total_kv_read_wall_time: 220ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_79	1.10	0	root		time:682.5µs, open:90.7µs, close:10.5µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_81	1.10	0	root		time:672.6µs, open:84.2µs, close:8.56µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.11 KB	N/A
    │   └─CTEFullScan_83	1.37	0	root	CTE:raw_boundary	time:664.4µs, open:79.8µs, close:6.75µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_87	1.10	0	root		time:686µs, open:682.4µs, close:1.76µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_89	1.10	0	root		time:682.3µs, open:680.5µs, close:593ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	3.11 KB	N/A
    │   └─CTEFullScan_91	1.37	0	root	CTE:raw_boundary	time:676.8µs, open:676.2µs, close:131ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_95	1.10	0	root		time:692.3µs, open:688.9µs, close:1.76µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_97	1.10	0	root		time:687.5µs, open:686µs, close:480ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	3.11 KB	N/A
    │   └─CTEFullScan_99	1.37	0	root	CTE:raw_boundary	time:683.1µs, open:682.4µs, close:121ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_103	1.10	0	root		time:698.7µs, open:695.5µs, close:1.67µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_105	1.10	0	root		time:695.2µs, open:693.7µs, close:551ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.11 KB	N/A
    │   └─CTEFullScan_107	1.37	0	root	CTE:raw_boundary	time:690.1µs, open:689.5µs, close:139ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_111	1.10	0	root		time:14.3µs, open:9.15µs, close:1.85µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
      └─Selection_113	1.10	0	root		time:8.67µs, open:5.43µs, close:1.08µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.11 KB	N/A
        └─CTEFullScan_115	1.37	0	root	CTE:raw_boundary	time:1.43µs, open:353ns, close:129ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.37	0	root		time:664.4µs, open:79.8µs, close:6.75µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_58(Seed Part)	1.37	0	root		time:648.3µs, open:76.4µs, close:5.04µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.agent_type	4.10 KB	N/A
  └─IndexReader_63	1.71	0	root	partition:p20251101	time:639.3µs, open:70.2µs, close:3.51µs, loops:1, cop_task: {num: 1, max: 537.9µs, proc_keys: 0, tot_proc: 28.8µs, tot_wait: 49.1µs, copr_cache: disabled, build_task_duration: 23.7µs, max_distsql_concurrency: 1}, fetch_resp_duration: 552.6µs, rpc_info:{Cop:{num_rpc:1, total_time:519.3µs}}	index:Selection_62	255 Bytes	N/A
    └─Selection_62	1.71	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 23.8µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 28.8µs, total_wait_time: 49.1µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type)))))	N/A	N/A
      └─IndexRangeScan_61	1.72	0	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1235.0 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.exact_id AS `raw_distinct_0`,
    d.smart_id AS `raw_distinct_1`,
    d.input_ip AS `raw_distinct_2`,
    d.proxy_ip AS `raw_distinct_3`,
    d.agent_type AS `raw_distinct_4`
  FROM deviceprofile_fact d
  WHERE d.true_ip = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 23:06:57.563000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_020'
    AND x.template_id IN ('b_0162', 'b_0166', 'b_0170', 'b_0174', 'b_0178')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0162' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0166' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0170' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0174' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
  UNION ALL
  SELECT 'b_0178' AS template_id, CAST(`raw_distinct_4` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_4` IS NOT NULL
)
SELECT
  COUNT(DISTINCT CASE WHEN template_id = 'b_0162' THEN distinct_value END) AS `metric__b_0162`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0166' THEN distinct_value END) AS `metric__b_0166`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0170' THEN distinct_value END) AS `metric__b_0170`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0174' THEN distinct_value END) AS `metric__b_0174`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0178' THEN distinct_value END) AS `metric__b_0178`
FROM distinct_values;
```

```text
-- explain_analyze_elapsed_ms=1235.0
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_80	1.00	1	root		time:1.23s, open:9.91µs, close:65.3µs, loops:2, RU:3527.05	funcs:count(distinct Column#188)->Column#150, funcs:count(distinct Column#189)->Column#151, funcs:count(distinct Column#190)->Column#152, funcs:count(distinct Column#191)->Column#153, funcs:count(distinct Column#192)->Column#154	57.1 MB	0 Bytes
└─Projection_131	2121046.92	1557792	root		time:491.2ms, open:2.06µs, close:64.1µs, loops:1526, Concurrency:5	case(eq(Column#148, ?), Column#149)->Column#188, case(eq(Column#148, ?), Column#149)->Column#189, case(eq(Column#148, ?), Column#149)->Column#190, case(eq(Column#148, ?), Column#149)->Column#191, case(eq(Column#148, ?), Column#149)->Column#192	1015.8 KB	N/A
  └─Union_82	2121046.92	1557792	root		time:506.5ms, open:839ns, close:45.3µs, loops:1526		N/A	N/A
    ├─Projection_84	2121041.42	1557792	root		time:506.2ms, open:123.6µs, close:25µs, loops:1526, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#148, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	954.1 KB	N/A
    │ └─IndexReader_87	2121041.42	1557792	root		time:514ms, open:122.1µs, close:12.7µs, loops:1526, cop_task: {num: 38, max: 454.8ms, min: 1.13ms, avg: 60.3ms, p95: 446.5ms, max_proc_keys: 341984, p95_proc_keys: 286688, tot_proc: 1s, tot_wait: 1.08ms, copr_cache: disabled, build_task_duration: 47.6µs, max_distsql_concurrency: 5}, fetch_resp_duration: 508.3ms, rpc_info:{Cop:{num_rpc:38, total_time:2.29s}}	index:IndexRangeScan_86	96.1 MB	N/A
    │   └─IndexRangeScan_86	2121041.42	1557792	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:180ms, min:0s, avg: 25ms, p80:20ms, p95:170ms, iters:1668, tasks:38}, scan_detail: {total_process_keys: 1557792, total_process_keys_size: 208062197, total_keys: 198097, get_snapshot_time: 525.4µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 1s, total_suspend_time: 417µs, total_wait_time: 1.08ms, total_kv_read_wall_time: 170ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_88	1.10	0	root		time:723.5µs, open:88.7µs, close:10.7µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_90	1.10	0	root		time:715.8µs, open:85µs, close:8.42µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.11 KB	N/A
    │   └─CTEFullScan_92	1.37	0	root	CTE:raw_boundary	time:709µs, open:81.4µs, close:7.22µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_96	1.10	0	root		time:731.6µs, open:726.8µs, close:2.27µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_98	1.10	0	root		time:727.9µs, open:725.1µs, close:986ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	3.11 KB	N/A
    │   └─CTEFullScan_100	1.37	0	root	CTE:raw_boundary	time:723.8µs, open:722.6µs, close:82ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_104	1.10	0	root		time:735.8µs, open:732.4µs, close:1.66µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.98 KB	N/A
    │ └─Selection_106	1.10	0	root		time:733µs, open:731.4µs, close:462ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	8.23 KB	N/A
    │   └─CTEFullScan_108	1.37	0	root	CTE:raw_boundary	time:728.4µs, open:727.8µs, close:72ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_112	1.10	0	root		time:739.3µs, open:735.2µs, close:2.23µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	62.0 KB	N/A
    │ └─Selection_114	1.10	0	root		time:735µs, open:733.1µs, close:775ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.11 KB	N/A
    │   └─CTEFullScan_116	1.37	0	root	CTE:raw_boundary	time:732µs, open:731.4µs, close:80ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_120	1.10	0	root		time:13.3µs, open:8.04µs, close:2.19µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
      └─Selection_122	1.10	0	root		time:9.29µs, open:6.44µs, close:854ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.11 KB	N/A
        └─CTEFullScan_124	1.37	0	root	CTE:raw_boundary	time:1.38µs, open:290ns, close:175ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.37	0	root		time:709µs, open:81.4µs, close:7.22µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_58(Seed Part)	1.37	0	root		time:689.8µs, open:77.6µs, close:5.63µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.agent_type	4.10 KB	N/A
  └─IndexReader_67	1.71	0	root	partition:p20251101	time:682.2µs, open:72.4µs, close:4.31µs, loops:1, cop_task: {num: 1, max: 576.4µs, proc_keys: 0, tot_proc: 28.3µs, tot_wait: 50µs, copr_cache: disabled, build_task_duration: 23.6µs, max_distsql_concurrency: 1}, fetch_resp_duration: 592.1µs, rpc_info:{Cop:{num_rpc:1, total_time:555.5µs}}	index:Selection_66	255 Bytes	N/A
    └─Selection_66	1.71	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 28µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 28.3µs, total_wait_time: 50µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type)))))	N/A	N/A
      └─IndexRangeScan_65	1.72	0	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_hint

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1209.2 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.exact_id AS `raw_distinct_0`,
    d.smart_id AS `raw_distinct_1`,
    d.input_ip AS `raw_distinct_2`,
    d.proxy_ip AS `raw_distinct_3`,
    d.agent_type AS `raw_distinct_4`
  FROM deviceprofile_fact d
  WHERE d.true_ip = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 23:06:57.563000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_020'
    AND x.template_id IN ('b_0162', 'b_0166', 'b_0170', 'b_0174', 'b_0178')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0162' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0166' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0170' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0174' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
  UNION ALL
  SELECT 'b_0178' AS template_id, CAST(`raw_distinct_4` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_4` IS NOT NULL
)
SELECT
  COUNT(DISTINCT CASE WHEN template_id = 'b_0162' THEN distinct_value END) AS `metric__b_0162`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0166' THEN distinct_value END) AS `metric__b_0166`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0170' THEN distinct_value END) AS `metric__b_0170`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0174' THEN distinct_value END) AS `metric__b_0174`,
  COUNT(DISTINCT CASE WHEN template_id = 'b_0178' THEN distinct_value END) AS `metric__b_0178`
FROM distinct_values;
```

```text
-- explain_analyze_elapsed_ms=1209.2
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_80	1.00	1	root		time:1.2s, open:11.3µs, close:78.1µs, loops:2, RU:3548.97	funcs:count(distinct Column#188)->Column#150, funcs:count(distinct Column#189)->Column#151, funcs:count(distinct Column#190)->Column#152, funcs:count(distinct Column#191)->Column#153, funcs:count(distinct Column#192)->Column#154	57.1 MB	0 Bytes
└─Projection_131	2121046.92	1557792	root		time:491.1ms, open:2.02µs, close:77.1µs, loops:1527, Concurrency:5	case(eq(Column#148, ?), Column#149)->Column#188, case(eq(Column#148, ?), Column#149)->Column#189, case(eq(Column#148, ?), Column#149)->Column#190, case(eq(Column#148, ?), Column#149)->Column#191, case(eq(Column#148, ?), Column#149)->Column#192	1014.1 KB	N/A
  └─Union_82	2121046.92	1557792	root		time:504.2ms, open:800ns, close:53.2µs, loops:1527		N/A	N/A
    ├─Projection_84	2121041.42	1557792	root		time:505.3ms, open:118.6µs, close:30.9µs, loops:1527, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#148, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	952.1 KB	N/A
    │ └─IndexReader_87	2121041.42	1557792	root		time:513.9ms, open:117.6µs, close:8.96µs, loops:1527, cop_task: {num: 38, max: 463.1ms, min: 1.06ms, avg: 60.3ms, p95: 447.4ms, max_proc_keys: 341984, p95_proc_keys: 286688, tot_proc: 1.07s, tot_wait: 998.5µs, copr_cache: disabled, build_task_duration: 49.2µs, max_distsql_concurrency: 5}, fetch_resp_duration: 508.3ms, rpc_info:{Cop:{num_rpc:38, total_time:2.29s}}	index:IndexRangeScan_86	96.1 MB	N/A
    │   └─IndexRangeScan_86	2121041.42	1557792	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:200ms, min:0s, avg: 27.6ms, p80:20ms, p95:190ms, iters:1668, tasks:38}, scan_detail: {total_process_keys: 1557792, total_process_keys_size: 208062197, total_keys: 198097, get_snapshot_time: 471µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 1.07s, total_suspend_time: 410.7µs, total_wait_time: 998.5µs, total_kv_read_wall_time: 200ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_88	1.10	0	root		time:938.1µs, open:85.9µs, close:11.5µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_90	1.10	0	root		time:930.1µs, open:81.8µs, close:9.12µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.11 KB	N/A
    │   └─CTEFullScan_92	1.37	0	root	CTE:raw_boundary	time:921.7µs, open:77.3µs, close:7.44µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_96	1.10	0	root		time:945.8µs, open:940.7µs, close:2.98µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_98	1.10	0	root		time:940.8µs, open:938.4µs, close:1.01µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	3.11 KB	N/A
    │   └─CTEFullScan_100	1.37	0	root	CTE:raw_boundary	time:936.3µs, open:935.6µs, close:172ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_104	1.10	0	root		time:950.5µs, open:946.9µs, close:1.75µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.98 KB	N/A
    │ └─Selection_106	1.10	0	root		time:947.5µs, open:945.3µs, close:934ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	8.61 KB	N/A
    │   └─CTEFullScan_108	1.37	0	root	CTE:raw_boundary	time:941µs, open:940.3µs, close:178ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_112	1.10	0	root		time:955.5µs, open:951.3µs, close:2.48µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	62.0 KB	N/A
    │ └─Selection_114	1.10	0	root		time:950.1µs, open:948.1µs, close:817ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.11 KB	N/A
    │   └─CTEFullScan_116	1.37	0	root	CTE:raw_boundary	time:946.5µs, open:945.7µs, close:176ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_120	1.10	0	root		time:16µs, open:9.33µs, close:2.07µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
      └─Selection_122	1.10	0	root		time:10.5µs, open:6.63µs, close:892ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.11 KB	N/A
        └─CTEFullScan_124	1.37	0	root	CTE:raw_boundary	time:2.01µs, open:377ns, close:170ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.37	0	root		time:921.7µs, open:77.3µs, close:7.44µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_58(Seed Part)	1.37	0	root		time:901.9µs, open:74.1µs, close:5.79µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.agent_type	4.10 KB	N/A
  └─IndexReader_67	1.71	0	root	partition:p20251101	time:894.4µs, open:70µs, close:4.28µs, loops:1, cop_task: {num: 1, max: 790.3µs, proc_keys: 0, tot_proc: 26.9µs, tot_wait: 44.4µs, copr_cache: disabled, build_task_duration: 23.3µs, max_distsql_concurrency: 1}, fetch_resp_duration: 806.4µs, rpc_info:{Cop:{num_rpc:1, total_time:770.5µs}}	index:Selection_66	255 Bytes	N/A
    └─Selection_66	1.71	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 22.3µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 26.9µs, total_wait_time: 44.4µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type)))))	N/A	N/A
      └─IndexRangeScan_65	1.72	0	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `1.9 ms`
- Error: `(1815, "Internal : No access path for table 'x' is found with 'tidb_isolation_read_engines' = 'tiflash,tidb', valid values can be 'tikv'.")`

## 4. group_c_bundle_018

- Group/window/filter: `C` / `30d` / `d.true_ip = %s`
- Preagg applied: `False`
- Event: invoice=`INV0007589128` kind=`hot_true_ip` hot_field=`true_ip` hot_count=`738824` ref=`2026-04-10T23:06:57.563000`

### Params

```json
[
  "72.153.231.69"
]
```

### tikv

- Engines: `tikv,tidb`
- Elapsed: `342.4 ms`
- Storage tasks: `cop[tikv], root`

```sql
SELECT
  COUNT(*) AS `metric__c_0102`,
  SUM(p.amount) AS `metric__c_0103`,
  COUNT(DISTINCT(p.merchant_account_number)) AS `metric__c_0104`,
  COUNT(DISTINCT(p.card_holder_number_sha512)) AS `metric__c_0105`
FROM pmt_txn_fact p
LEFT OUTER JOIN deviceprofile_fact d
  ON p.parsed_interaction_id = d.interaction_id
WHERE d.true_ip = %s AND p.event_date >= 1773270417563
  AND d.jms_timestamp >= '2026-03-11 23:06:57.563000'
HAVING COUNT(*) > 0;
```

```text
-- explain_analyze_elapsed_ms=342.4
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Selection_13	0.80	1	root		time:338.4ms, open:108.4µs, close:13.4µs, loops:2, RU:1017.36	gt(Column#106, ?)	2.52 KB	N/A
└─HashAgg_17	1.00	1	root		time:338.4ms, open:105.7µs, close:12.8µs, loops:3	funcs:count(?)->Column#106, funcs:sum(intuit_risk.pmt_txn_fact.amount)->Column#107, funcs:count(distinct intuit_risk.pmt_txn_fact.merchant_account_number)->Column#108, funcs:count(distinct intuit_risk.pmt_txn_fact.card_holder_number_sha512)->Column#109	539.9 KB	0 Bytes
  └─IndexHashJoin_28	380878.24	4558	root		time:336.8ms, open:95.7µs, close:12.1µs, loops:8, inner:{total:650.6ms, concurrency:5, task:7, construct:94.9ms, fetch:551.1ms, build:13.5ms, join:4.58ms}	inner join, inner:IndexReader_56, outer key:intuit_risk.deviceprofile_fact.interaction_id, inner key:intuit_risk.pmt_txn_fact.parsed_interaction_id, equal cond:eq(intuit_risk.deviceprofile_fact.interaction_id, intuit_risk.pmt_txn_fact.parsed_interaction_id)	18.8 MB	N/A
    ├─IndexReader_53(Build)	243544.67	98842	root	partition:p20260401,p20260501,p20260601,pmax	time:167.9ms, open:93.8µs, close:9.3µs, loops:99, cop_task: {num: 11, max: 167.2ms, min: 1.18ms, avg: 20.4ms, p95: 167.2ms, max_proc_keys: 116680, p95_proc_keys: 116680, tot_proc: 165.8ms, tot_wait: 3.41ms, copr_cache: disabled, build_task_duration: 37.4µs, max_distsql_concurrency: 4}, fetch_resp_duration: 166.2ms, rpc_info:{Cop:{num_rpc:11, total_time:224.7ms}}	index:Selection_52	4.77 MB	N/A
    │ └─Selection_52	243544.67	98842	cop[tikv]		tikv_task:{proc max:120ms, min:0s, avg: 13.6ms, p80:10ms, p95:120ms, iters:205, tasks:11}, scan_detail: {total_process_keys: 172346, total_process_keys_size: 40578683, total_keys: 55676, get_snapshot_time: 4.1ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 165.8ms, total_suspend_time: 128.7µs, total_wait_time: 3.41ms, total_kv_read_wall_time: 30ms}	not(isnull(intuit_risk.deviceprofile_fact.interaction_id))	N/A	N/A
    │   └─IndexRangeScan_51	359561.64	172346	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{proc max:120ms, min:0s, avg: 13.6ms, p80:10ms, p95:120ms, iters:205, tasks:11}	range:[? ?,? +inf], keep order:false	N/A	N/A
    └─IndexReader_56(Probe)	380878.24	4558	root	partition:p20260401,p20260501,p20260601,pmax	total_time:509.5ms, total_open:169.7ms, total_close:69.6µs, loops:16, cop_task: {num: 137, max: 85.6ms, min: 741.1µs, avg: 10.4ms, p95: 61.4ms, max_proc_keys: 195, p95_proc_keys: 133, tot_proc: 758.2ms, tot_wait: 18.6ms, copr_cache: disabled, build_task_duration: 30.8ms, max_distsql_concurrency: 15}, fetch_resp_duration: 334.5ms, rpc_info:{Cop:{num_rpc:137, total_time:1.43s}}	index:Selection_55	7.63 KB	N/A
      └─Selection_55	380878.24	4558	cop[tikv]		tikv_task:{proc max:70ms, min:0s, avg: 5.84ms, p80:10ms, p95:40ms, iters:197, tasks:137}, scan_detail: {total_process_keys: 4558, total_process_keys_size: 1302932, total_keys: 40635, get_snapshot_time: 16.4ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 758.2ms, total_suspend_time: 419.8µs, total_wait_time: 18.6ms, total_kv_read_wall_time: 280ms}	not(isnull(intuit_risk.pmt_txn_fact.parsed_interaction_id))	N/A	N/A
        └─IndexRangeScan_54	516115.01	4558	cop[tikv]	table:p, index:idx_pmt_join_runtime_cov(parsed_interaction_id, event_date, amount, merchant_account_number, card_holder_number_sha512, card_type, entry_method, mt_gateway, check_bank_routing_number, transaction_type)	tikv_task:{proc max:70ms, min:0s, avg: 5.77ms, p80:10ms, p95:40ms, iters:197, tasks:137}	range: decided by [eq(intuit_risk.pmt_txn_fact.parsed_interaction_id, intuit_risk.deviceprofile_fact.interaction_id) ge(intuit_risk.pmt_txn_fact.event_date, ?)], keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `339.2 ms`
- Storage tasks: `cop[tikv], root`

```sql
SELECT
  COUNT(*) AS `metric__c_0102`,
  SUM(p.amount) AS `metric__c_0103`,
  COUNT(DISTINCT(p.merchant_account_number)) AS `metric__c_0104`,
  COUNT(DISTINCT(p.card_holder_number_sha512)) AS `metric__c_0105`
FROM pmt_txn_fact p
LEFT OUTER JOIN deviceprofile_fact d
  ON p.parsed_interaction_id = d.interaction_id
WHERE d.true_ip = %s AND p.event_date >= 1773270417563
  AND d.jms_timestamp >= '2026-03-11 23:06:57.563000'
HAVING COUNT(*) > 0;
```

```text
-- explain_analyze_elapsed_ms=339.2
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Selection_13	0.80	1	root		time:335.1ms, open:108µs, close:11.4µs, loops:2, RU:1031.76	gt(Column#106, ?)	6.07 KB	N/A
└─HashAgg_17	1.00	1	root		time:335ms, open:105.3µs, close:10.7µs, loops:3	funcs:count(?)->Column#106, funcs:sum(intuit_risk.pmt_txn_fact.amount)->Column#107, funcs:count(distinct intuit_risk.pmt_txn_fact.merchant_account_number)->Column#108, funcs:count(distinct intuit_risk.pmt_txn_fact.card_holder_number_sha512)->Column#109	536.5 KB	0 Bytes
  └─IndexHashJoin_28	380878.24	4558	root		time:333.1ms, open:97.1µs, close:9.99µs, loops:8, inner:{total:636.7ms, concurrency:5, task:7, construct:97ms, fetch:535.2ms, build:13.7ms, join:4.56ms}	inner join, inner:IndexReader_69, outer key:intuit_risk.deviceprofile_fact.interaction_id, inner key:intuit_risk.pmt_txn_fact.parsed_interaction_id, equal cond:eq(intuit_risk.deviceprofile_fact.interaction_id, intuit_risk.pmt_txn_fact.parsed_interaction_id)	18.8 MB	N/A
    ├─IndexReader_66(Build)	243544.67	98842	root	partition:p20260401,p20260501,p20260601,pmax	time:163.5ms, open:95.9µs, close:7.63µs, loops:99, cop_task: {num: 11, max: 162.6ms, min: 487.1µs, avg: 19.3ms, p95: 162.6ms, max_proc_keys: 116680, p95_proc_keys: 116680, tot_proc: 183.3ms, tot_wait: 337.4µs, copr_cache: disabled, build_task_duration: 37.7µs, max_distsql_concurrency: 4}, fetch_resp_duration: 161.8ms, rpc_info:{Cop:{num_rpc:11, total_time:212.7ms}}	index:Selection_65	4.77 MB	N/A
    │ └─Selection_65	243544.67	98842	cop[tikv]		tikv_task:{proc max:140ms, min:0s, avg: 16.4ms, p80:10ms, p95:140ms, iters:205, tasks:11}, scan_detail: {total_process_keys: 172346, total_process_keys_size: 40578683, total_keys: 55676, get_snapshot_time: 166.6µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 183.3ms, total_suspend_time: 126.1µs, total_wait_time: 337.4µs, total_kv_read_wall_time: 40ms}	not(isnull(intuit_risk.deviceprofile_fact.interaction_id))	N/A	N/A
    │   └─IndexRangeScan_64	359561.64	172346	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{proc max:140ms, min:0s, avg: 16.4ms, p80:10ms, p95:140ms, iters:205, tasks:11}	range:[? ?,? +inf], keep order:false	N/A	N/A
    └─IndexReader_69(Probe)	380878.24	4558	root	partition:p20260401,p20260501,p20260601,pmax	total_time:493.4ms, total_open:176.5ms, total_close:75.5µs, loops:16, cop_task: {num: 137, max: 78ms, min: 650.2µs, avg: 9.93ms, p95: 57.9ms, max_proc_keys: 195, p95_proc_keys: 133, tot_proc: 783.9ms, tot_wait: 3.65ms, copr_cache: disabled, build_task_duration: 37.3ms, max_distsql_concurrency: 15}, fetch_resp_duration: 311.8ms, rpc_info:{Cop:{num_rpc:137, total_time:1.36s}}	index:Selection_68	7.31 KB	N/A
      └─Selection_68	380878.24	4558	cop[tikv]		tikv_task:{proc max:70ms, min:0s, avg: 5.84ms, p80:10ms, p95:50ms, iters:197, tasks:137}, scan_detail: {total_process_keys: 4558, total_process_keys_size: 1302932, total_keys: 40635, get_snapshot_time: 1.5ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 783.9ms, total_suspend_time: 453.4µs, total_wait_time: 3.65ms, total_kv_read_wall_time: 260ms}	not(isnull(intuit_risk.pmt_txn_fact.parsed_interaction_id))	N/A	N/A
        └─IndexRangeScan_67	516115.01	4558	cop[tikv]	table:p, index:idx_pmt_join_runtime_cov(parsed_interaction_id, event_date, amount, merchant_account_number, card_holder_number_sha512, card_type, entry_method, mt_gateway, check_bank_routing_number, transaction_type)	tikv_task:{proc max:70ms, min:0s, avg: 5.84ms, p80:10ms, p95:50ms, iters:197, tasks:137}	range: decided by [eq(intuit_risk.pmt_txn_fact.parsed_interaction_id, intuit_risk.deviceprofile_fact.interaction_id) ge(intuit_risk.pmt_txn_fact.event_date, ?)], keep order:false	N/A	N/A
```

### tiflash_hint

- Engines: `tikv,tiflash,tidb`
- Elapsed: `10047.1 ms`
- Error: `(3024, 'Query execution was interrupted, maximum statement execution time exceeded')`

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `10093.2 ms`
- Error: `(3024, 'Query execution was interrupted, maximum statement execution time exceeded')`
