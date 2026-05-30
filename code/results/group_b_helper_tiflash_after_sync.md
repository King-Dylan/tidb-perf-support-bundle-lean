# TiKV vs TiFlash Candidate A/B

- Generated: `2026-05-30T23:16:37`
- Mixed JSON: `/home/ec2-user/tidb_intuit_perf_support_bundle_lean/code/results/mixed_traffic_1780125439.json`
- Pre-agg layout: `prod180`
- Pre-agg bundle count: `12`
- Session knobs: distinct_pushdown=`False`, force_inline_cte=`0`, hashagg_final=`4`, hashagg_partial=`4`

## Summary

| bundle | group | event | variant | engines | elapsed | storage tasks | result |
|---|---:|---|---|---|---:|---|---|
| group_b_bundle_018 | B | hot_input_ip:input_ip | tikv | tikv,tidb | 1209.8 ms | cop[tikv], root | ok |
| group_b_bundle_018 | B | hot_input_ip:input_ip | cost | tikv,tiflash,tidb | 1150.2 ms | cop[tikv], root | ok |
| group_b_bundle_018 | B | hot_input_ip:input_ip | tiflash_only | tiflash,tidb | 10031.3 ms |  | (3024, 'Query execution was interrupted, maximum statement execution time exceeded') |
| group_b_bundle_019 | B | hot_smart_id:smart_id | tikv | tikv,tidb | 740.8 ms | cop[tikv], root | ok |
| group_b_bundle_019 | B | hot_smart_id:smart_id | cost | tikv,tiflash,tidb | 700.9 ms | cop[tikv], root | ok |
| group_b_bundle_019 | B | hot_smart_id:smart_id | tiflash_only | tiflash,tidb | 10095.0 ms |  | (3024, 'Query execution was interrupted, maximum statement execution time exceeded') |
| group_b_bundle_020 | B | hot_true_ip:true_ip | tikv | tikv,tidb | 1187.1 ms | cop[tikv], root | ok |
| group_b_bundle_020 | B | hot_true_ip:true_ip | cost | tikv,tiflash,tidb | 1114.6 ms | cop[tikv], root | ok |
| group_b_bundle_020 | B | hot_true_ip:true_ip | tiflash_only | tiflash,tidb | 7383.5 ms | mpp[tiflash], root | ok |

## 1. group_b_bundle_018

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
- Elapsed: `1209.8 ms`
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
-- explain_analyze_elapsed_ms=1209.8
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_63	1.00	1	root		time:1.2s, open:15µs, close:68.3µs, loops:2, RU:3428.43	funcs:count(distinct Column#161)->Column#128, funcs:count(distinct Column#162)->Column#129, funcs:count(distinct Column#163)->Column#130, funcs:count(distinct Column#164)->Column#131	56.9 MB	0 Bytes
└─Projection_147	2340270.98	1533348	root		time:571.3ms, open:3.94µs, close:67.2µs, loops:1502, Concurrency:5	case(eq(Column#126, ?), Column#127)->Column#161, case(eq(Column#126, ?), Column#127)->Column#162, case(eq(Column#126, ?), Column#127)->Column#163, case(eq(Column#126, ?), Column#127)->Column#164	891.9 KB	N/A
  └─Union_67	2340270.98	1533348	root		time:576.8ms, open:779ns, close:46.2µs, loops:1502		N/A	N/A
    ├─Projection_69	2340266.27	1533348	root		time:577.1ms, open:1.04ms, close:28.9µs, loops:1502, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#126, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	911.3 KB	N/A
    │ └─IndexReader_72	2340266.27	1533348	root		time:581.2ms, open:1.03ms, close:13µs, loops:1502, cop_task: {num: 29, max: 529.3ms, min: 1.11ms, avg: 87.7ms, p95: 524.4ms, max_proc_keys: 345056, p95_proc_keys: 289760, tot_proc: 1.01s, tot_wait: 5.39ms, copr_cache: disabled, build_task_duration: 967µs, max_distsql_concurrency: 4}, fetch_resp_duration: 575.6ms, rpc_info:{Cop:{num_rpc:29, total_time:2.54s}}	index:IndexRangeScan_71	96.2 MB	N/A
    │   └─IndexRangeScan_71	2340266.27	1533348	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:190ms, min:0s, avg: 33.8ms, p80:30ms, p95:180ms, iters:1610, tasks:29}, scan_detail: {total_process_keys: 1533348, total_process_keys_size: 201682727, total_keys: 169419, get_snapshot_time: 5.01ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 1.01s, total_suspend_time: 475.2µs, total_wait_time: 5.39ms, total_kv_read_wall_time: 220ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_73	1.18	0	root		time:2.34ms, open:844.9µs, close:10.3µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_75	1.18	0	root		time:2.33ms, open:841µs, close:8.45µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	2.48 KB	N/A
    │   └─CTEFullScan_77	1.47	0	root	CTE:raw_boundary	time:2.32ms, open:835.2µs, close:7.23µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_81	1.18	0	root		time:2.35ms, open:2.34ms, close:2.45µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_83	1.18	0	root		time:2.34ms, open:2.34ms, close:836ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	2.48 KB	N/A
    │   └─CTEFullScan_85	1.47	0	root	CTE:raw_boundary	time:2.33ms, open:2.33ms, close:177ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_89	1.18	0	root		time:2.35ms, open:2.35ms, close:1.79µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_91	1.18	0	root		time:2.35ms, open:2.35ms, close:538ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	2.48 KB	N/A
    │   └─CTEFullScan_93	1.47	0	root	CTE:raw_boundary	time:2.35ms, open:2.34ms, close:177ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_97	1.18	0	root		time:2.36ms, open:2.36ms, close:1.44µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
      └─Selection_99	1.18	0	root		time:2.35ms, open:2.35ms, close:404ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	2.48 KB	N/A
        └─CTEFullScan_101	1.47	0	root	CTE:raw_boundary	time:2.35ms, open:2.35ms, close:73ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.47	0	root		time:2.32ms, open:835.2µs, close:7.23µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_50(Seed Part)	1.47	0	root		time:2.31ms, open:831.4µs, close:5.15µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.agent_type	3.48 KB	N/A
  └─IndexReader_55	1.83	0	root	partition:p20251101	time:2.3ms, open:824.4µs, close:3.92µs, loops:1, cop_task: {num: 1, max: 1.44ms, proc_keys: 0, tot_proc: 57.3µs, tot_wait: 620.3µs, copr_cache: disabled, build_task_duration: 775.3µs, max_distsql_concurrency: 1}, fetch_resp_duration: 1.46ms, rpc_info:{Cop:{num_rpc:1, total_time:1.42ms}}	index:Selection_54	255 Bytes	N/A
    └─Selection_54	1.83	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 596.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 57.3µs, total_wait_time: 620.3µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type))))	N/A	N/A
      └─IndexRangeScan_53	1.84	0	cop[tikv]	table:d, index:idx_dev_input_runtime_cov(input_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip_score, smart_id, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1150.2 ms`
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
-- explain_analyze_elapsed_ms=1150.2
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_72	1.00	1	root		time:1.14s, open:8.79µs, close:61.3µs, loops:2, RU:3415.14	funcs:count(distinct Column#161)->Column#128, funcs:count(distinct Column#162)->Column#129, funcs:count(distinct Column#163)->Column#130, funcs:count(distinct Column#164)->Column#131	56.9 MB	0 Bytes
└─Projection_174	2340270.98	1533348	root		time:509.5ms, open:2.01µs, close:60.4µs, loops:1502, Concurrency:5	case(eq(Column#126, ?), Column#127)->Column#161, case(eq(Column#126, ?), Column#127)->Column#162, case(eq(Column#126, ?), Column#127)->Column#163, case(eq(Column#126, ?), Column#127)->Column#164	893.9 KB	N/A
  └─Union_76	2340270.98	1533348	root		time:521.8ms, open:722ns, close:40.6µs, loops:1502		N/A	N/A
    ├─Projection_78	2340266.27	1533348	root		time:513.6ms, open:84µs, close:21.7µs, loops:1502, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#126, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	913.3 KB	N/A
    │ └─IndexReader_85	2340266.27	1533348	root		time:517.7ms, open:80.7µs, close:12.3µs, loops:1502, cop_task: {num: 29, max: 524.6ms, min: 1.05ms, avg: 82.8ms, p95: 514ms, max_proc_keys: 345056, p95_proc_keys: 289760, tot_proc: 970.3ms, tot_wait: 794.9µs, copr_cache: disabled, build_task_duration: 25.2µs, max_distsql_concurrency: 4}, fetch_resp_duration: 512.6ms, rpc_info:{Cop:{num_rpc:29, total_time:2.4s}}	index:IndexRangeScan_84	96.2 MB	N/A
    │   └─IndexRangeScan_84	2340266.27	1533348	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:210ms, min:0s, avg: 34.5ms, p80:40ms, p95:170ms, iters:1610, tasks:29}, scan_detail: {total_process_keys: 1533348, total_process_keys_size: 201682727, total_keys: 169419, get_snapshot_time: 416.2µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 970.3ms, total_suspend_time: 422.8µs, total_wait_time: 794.9µs, total_kv_read_wall_time: 210ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_90	1.18	0	root		time:937.8µs, open:925.8µs, close:9.85µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	3.11 KB	N/A
    │ └─Selection_92	1.18	0	root		time:933.4µs, open:924.4µs, close:7.54µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	6.48 KB	N/A
    │   └─CTEFullScan_94	1.47	0	root	CTE:raw_boundary	time:930µs, open:922.7µs, close:6.7µs, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_98	1.18	0	root		time:974.8µs, open:969.4µs, close:2.43µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_100	1.18	0	root		time:970.4µs, open:967.2µs, close:1.17µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	2.48 KB	N/A
    │   └─CTEFullScan_102	1.47	0	root	CTE:raw_boundary	time:963.8µs, open:962.6µs, close:195ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_106	1.18	0	root		time:961.8µs, open:79.8µs, close:1.7µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	2.48 KB	N/A
    │ └─Selection_108	1.18	0	root		time:958.3µs, open:78.5µs, close:687ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	2.48 KB	N/A
    │   └─CTEFullScan_110	1.47	0	root	CTE:raw_boundary	time:953.4µs, open:76µs, close:167ns, loops:1	data:CTE_0	0 Bytes	0 Bytes
    └─Projection_114	1.18	0	root		time:935.4µs, open:929.7µs, close:3.7µs, loops:1, Concurrency:OFF	?->Column#126, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#127	39.7 KB	N/A
      └─Selection_116	1.18	0	root		time:929.3µs, open:927.3µs, close:746ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	2.48 KB	N/A
        └─CTEFullScan_118	1.47	0	root	CTE:raw_boundary	time:924.2µs, open:923.4µs, close:161ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.47	0	root		time:930µs, open:922.7µs, close:6.7µs, loops:1	Non-Recursive CTE	N/A	N/A
└─Projection_50(Seed Part)	1.47	0	root		time:938.7µs, open:68.4µs, close:5.17µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.agent_type	3.48 KB	N/A
  └─IndexReader_59	1.83	0	root	partition:p20251101	time:934.3µs, open:66.8µs, close:3.53µs, loops:1, cop_task: {num: 1, max: 835.4µs, proc_keys: 0, tot_proc: 30.8µs, tot_wait: 50.4µs, copr_cache: disabled, build_task_duration: 22.7µs, max_distsql_concurrency: 1}, fetch_resp_duration: 851.3µs, rpc_info:{Cop:{num_rpc:1, total_time:816.2µs}}	index:Selection_58	255 Bytes	N/A
    └─Selection_58	1.83	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 26.1µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 30.8µs, total_wait_time: 50.4µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type))))	N/A	N/A
      └─IndexRangeScan_57	1.84	0	cop[tikv]	table:d, index:idx_dev_input_runtime_cov(input_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip_score, smart_id, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `10031.3 ms`
- Error: `(3024, 'Query execution was interrupted, maximum statement execution time exceeded')`

## 2. group_b_bundle_019

- Group/window/filter: `B` / `180d` / `d.smart_id = %s`
- Preagg applied: `True`
- Event: invoice=`INV0010730024` kind=`hot_smart_id` hot_field=`smart_id` hot_count=`382582` ref=`2026-04-10T21:03:14.101000`

### Params

```json
[
  "3b452b7fd9bd4ddcb27e0067970d6a1a",
  "3b452b7fd9bd4ddcb27e0067970d6a1a",
  "3b452b7fd9bd4ddcb27e0067970d6a1a",
  "3b452b7fd9bd4ddcb27e0067970d6a1a",
  "3b452b7fd9bd4ddcb27e0067970d6a1a",
  "3b452b7fd9bd4ddcb27e0067970d6a1a"
]
```

### tikv

- Engines: `tikv,tidb`
- Elapsed: `740.8 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.input_ip AS `raw_distinct_0`,
    d.true_ip AS `raw_distinct_1`,
    d.proxy_ip AS `raw_distinct_2`,
    d.exact_id AS `raw_distinct_3`,
    d.agent_type AS `raw_distinct_4`,
    d.agent_os AS `raw_distinct_5`
  FROM deviceprofile_fact d
  WHERE d.smart_id = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 21:03:14.101000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
),
distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_019'
    AND x.template_id IN ('b_0122', 'b_0126', 'b_0130', 'b_0134', 'b_0138', 'b_0142')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0122' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0126' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0130' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0134' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
  UNION ALL
  SELECT 'b_0138' AS template_id, CAST(`raw_distinct_4` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_4` IS NOT NULL
  UNION ALL
  SELECT 'b_0142' AS template_id, CAST(`raw_distinct_5` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_5` IS NOT NULL
),
unfiltered_counts AS (
  SELECT
    COUNT(DISTINCT CASE WHEN template_id = 'b_0122' THEN distinct_value END) AS `metric__b_0122`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0126' THEN distinct_value END) AS `metric__b_0126`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0130' THEN distinct_value END) AS `metric__b_0130`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0134' THEN distinct_value END) AS `metric__b_0134`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0138' THEN distinct_value END) AS `metric__b_0138`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0142' THEN distinct_value END) AS `metric__b_0142`
  FROM distinct_values
),
filtered_0 AS (
  SELECT
    (SELECT COUNT(DISTINCT u.distinct_value) FROM (
      SELECT x.distinct_value
      FROM `group_b_180d_daily_distinct` x
      WHERE x.bundle_id = 'group_b_bundle_019'
        AND x.template_id = 'b_0018'
        AND x.key1 = %s AND x.key2 = ''
        AND x.event_day > '2025-10-12'
      UNION ALL
      SELECT CAST(d.exact_id AS CHAR(256)) AS distinct_value
      FROM deviceprofile_fact d
      WHERE d.smart_id = %s AND d.exact_id IS NOT NULL AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 21:03:14.101000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000' AND ((d.request_result LIKE '%%pass%%' OR d.request_result LIKE '%%success%%') AND d.business_transaction REGEXP 'challenge_type=.*idp')
    ) u) AS `metric__b_0018`,
    COALESCE((SELECT SUM(u.presence_count) FROM (
      SELECT x.`present__b_0018` AS presence_count
      FROM `group_b_180d_daily_rollup` x
      WHERE x.bundle_id = 'group_b_bundle_019'
        AND x.key1 = %s AND x.key2 = ''
        AND x.event_day > '2025-10-12'
      UNION ALL
      SELECT COUNT(*) AS presence_count
      FROM deviceprofile_fact d
      WHERE d.smart_id = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 21:03:14.101000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000' AND ((d.request_result LIKE '%%pass%%' OR d.request_result LIKE '%%success%%') AND d.business_transaction REGEXP 'challenge_type=.*idp')
    ) u), 0) AS `present__b_0018`
)
SELECT
  filtered_0.`metric__b_0018`,
  filtered_0.`present__b_0018`,
  unfiltered_counts.`metric__b_0122`,
  unfiltered_counts.`metric__b_0126`,
  unfiltered_counts.`metric__b_0130`,
  unfiltered_counts.`metric__b_0134`,
  unfiltered_counts.`metric__b_0138`,
  unfiltered_counts.`metric__b_0142`
FROM unfiltered_counts
CROSS JOIN filtered_0;
```

```text
-- explain_analyze_elapsed_ms=740.8
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_451	1.00	1	root		time:698.4ms, open:19.9µs, close:73.3µs, loops:2, RU:1940.70, Concurrency:OFF	Column#1179, Column#1328, Column#784, Column#785, Column#786, Column#787, Column#788, Column#789	4 KB	N/A
└─Projection_453	1.00	1	root		time:698.4ms, open:17.6µs, close:71.4µs, loops:2, Concurrency:OFF	Column#784, Column#785, Column#786, Column#787, Column#788, Column#789, Column#1179, Column#1328	4 KB	N/A
  └─HashJoin_468	1.00	1	root		time:698.4ms, open:12.5µs, close:70.6µs, loops:2, build_hash_table:{total:698.2ms, fetch:698.2ms, build:6.56µs}, probe:{concurrency:5, total:3.49s, max:698.2ms, probe:16.8µs, fetch and wait:3.49s}	CARTESIAN inner join	50.7 KB	0 Bytes
    ├─HashAgg_492(Build)	1.00	1	root		time:698.2ms, open:8.95µs, close:61.2µs, loops:2	funcs:count(distinct Column#1387)->Column#784, funcs:count(distinct Column#1388)->Column#785, funcs:count(distinct Column#1389)->Column#786, funcs:count(distinct Column#1390)->Column#787, funcs:count(distinct Column#1391)->Column#788, funcs:count(distinct Column#1392)->Column#789	31.5 MB	0 Bytes
    │ └─Projection_609	572762.11	768780	root		time:366.8ms, open:1.29µs, close:60.4µs, loops:755, Concurrency:5	case(eq(Column#782, ?), Column#783)->Column#1387, case(eq(Column#782, ?), Column#783)->Column#1388, case(eq(Column#782, ?), Column#783)->Column#1389, case(eq(Column#782, ?), Column#783)->Column#1390, case(eq(Column#782, ?), Column#783)->Column#1391, case(eq(Column#782, ?), Column#783)->Column#1392	987.6 KB	N/A
    │   └─Union_496	572762.11	768780	root		time:378.9ms, open:648ns, close:46.1µs, loops:755		N/A	N/A
    │     ├─Projection_498	572755.74	768780	root		time:379.6ms, open:750.4µs, close:24µs, loops:755, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#782, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	986.8 KB	N/A
    │     │ └─IndexReader_501	572755.74	768780	root		time:383.7ms, open:748.3µs, close:9.48µs, loops:755, cop_task: {num: 33, max: 464.9ms, min: 1.21ms, avg: 42ms, p95: 369.4ms, max_proc_keys: 246752, p95_proc_keys: 234495, tot_proc: 566.5ms, tot_wait: 5.49ms, copr_cache: disabled, build_task_duration: 706.2µs, max_distsql_concurrency: 6}, fetch_resp_duration: 379.7ms, rpc_info:{Cop:{num_rpc:33, total_time:1.38s}}	index:IndexRangeScan_500	58.3 MB	N/A
    │     │   └─IndexRangeScan_500	572755.74	768780	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:160ms, min:0s, avg: 17.6ms, p80:10ms, p95:130ms, iters:877, tasks:33}, scan_detail: {total_process_keys: 768780, total_process_keys_size: 113149312, total_keys: 68132, get_snapshot_time: 5ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 566.5ms, total_suspend_time: 267.3µs, total_wait_time: 5.49ms, total_kv_read_wall_time: 170ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    │     ├─Projection_502	1.06	0	root		time:721µs, open:61.6µs, close:10.9µs, loops:1, Concurrency:OFF	?->Column#782, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	3.73 KB	N/A
    │     │ └─Selection_504	1.06	0	root		time:714.1µs, open:58.4µs, close:8.57µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	3.73 KB	N/A
    │     │   └─CTEFullScan_506	1.33	0	root	CTE:raw_boundary	time:707.5µs, open:54.7µs, close:7.56µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    │     ├─Projection_510	1.06	0	root		time:733.7µs, open:729.8µs, close:2.05µs, loops:1, Concurrency:OFF	?->Column#782, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	3.73 KB	N/A
    │     │ └─Selection_512	1.06	0	root		time:728.8µs, open:726.9µs, close:755ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	3.73 KB	N/A
    │     │   └─CTEFullScan_514	1.33	0	root	CTE:raw_boundary	time:724.8µs, open:724µs, close:192ns, loops:1	data:CTE_0	N/A	N/A
    │     ├─Projection_518	1.06	0	root		time:740.1µs, open:735.7µs, close:2.9µs, loops:1, Concurrency:OFF	?->Column#782, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	3.73 KB	N/A
    │     │ └─Selection_520	1.06	0	root		time:735.6µs, open:733.8µs, close:847ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.73 KB	N/A
    │     │   └─CTEFullScan_522	1.33	0	root	CTE:raw_boundary	time:730.2µs, open:729.6µs, close:167ns, loops:1	data:CTE_0	N/A	N/A
    │     ├─Projection_526	1.06	0	root		time:744.7µs, open:741.2µs, close:1.79µs, loops:1, Concurrency:OFF	?->Column#782, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	3.73 KB	N/A
    │     │ └─Selection_528	1.06	0	root		time:741.1µs, open:739.5µs, close:632ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.73 KB	N/A
    │     │   └─CTEFullScan_530	1.33	0	root	CTE:raw_boundary	time:738.1µs, open:737.4µs, close:192ns, loops:1	data:CTE_0	N/A	N/A
    │     ├─Projection_534	1.06	0	root		time:14.5µs, open:10.4µs, close:1.35µs, loops:1, Concurrency:OFF	?->Column#782, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	3.73 KB	N/A
    │     │ └─Selection_536	1.06	0	root		time:8.29µs, open:5.96µs, close:608ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.73 KB	N/A
    │     │   └─CTEFullScan_538	1.33	0	root	CTE:raw_boundary	time:1.34µs, open:320ns, close:169ns, loops:1	data:CTE_0	N/A	N/A
    │     └─Projection_542	1.06	0	root		time:7.99µs, open:4.78µs, close:1.29µs, loops:1, Concurrency:OFF	?->Column#782, cast(cast(intuit_risk.deviceprofile_fact.agent_os, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#783	3.73 KB	N/A
    │       └─Selection_544	1.06	0	root		time:4.23µs, open:2.62µs, close:442ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_os))	3.73 KB	N/A
    │         └─CTEFullScan_546	1.33	0	root	CTE:raw_boundary	time:800ns, open:180ns, close:73ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_474(Probe)	1.00	1	root		time:13.4µs, open:2.16µs, close:917ns, loops:2, Concurrency:OFF	?->Column#1179, ?->Column#1328	0 Bytes	N/A
      └─TableDual_476	1.00	1	root		time:3.29µs, open:188ns, close:238ns, loops:2	rows:1	N/A	N/A
CTE_0	1.33	0	root		time:707.5µs, open:54.7µs, close:7.56µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_440(Seed Part)	1.33	0	root		time:694µs, open:51.5µs, close:5.52µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.agent_type, intuit_risk.deviceprofile_fact.agent_os	4.72 KB	N/A
  └─IndexReader_445	1.65	0	root	partition:p20251101	time:685µs, open:45.4µs, close:3.7µs, loops:1, cop_task: {num: 1, max: 616.7µs, proc_keys: 0, tot_proc: 18.8µs, tot_wait: 32µs, copr_cache: disabled, build_task_duration: 13µs, max_distsql_concurrency: 1}, fetch_resp_duration: 626.9µs, rpc_info:{Cop:{num_rpc:1, total_time:604.5µs}}	index:Selection_444	280 Bytes	N/A
    └─Selection_444	1.65	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 15.7µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 18.8µs, total_wait_time: 32µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)))), or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), or(not(isnull(intuit_risk.deviceprofile_fact.agent_type)), not(isnull(intuit_risk.deviceprofile_fact.agent_os)))))	N/A	N/A
      └─IndexRangeScan_443	1.66	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_147	N/A	0	root			Output: ScalarQueryCol#568	N/A	N/A
└─MaxOneRow_102	1.00	1	root		time:18.3ms, open:8.44µs, close:15.7µs, loops:1		N/A	N/A
  └─StreamAgg_110	1.00	1	root		time:18.3ms, open:7.73µs, close:15.4µs, loops:2	funcs:count(distinct Column#515)->Column#516	888 Bytes	N/A
    └─Union_129	2.00	0	root		time:18.2ms, open:776ns, close:14.8µs, loops:1		N/A	N/A
      ├─Projection_131	1.00	0	root		time:18.2ms, open:511.6µs, close:11.1µs, loops:1, Concurrency:OFF	cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#515	3.48 KB	N/A
      │ └─IndexReader_134	1.00	0	root		time:18.2ms, open:505.2µs, close:8.85µs, loops:1, cop_task: {num: 1, max: 17.7ms, proc_keys: 0, tot_proc: 15.9ms, tot_wait: 897µs, copr_cache: disabled, build_task_duration: 454.7µs, max_distsql_concurrency: 1}, fetch_resp_duration: 17.7ms, rpc_info:{Cop:{num_rpc:1, total_time:17.6ms}}	index:IndexRangeScan_133	339 Bytes	N/A
      │   └─IndexRangeScan_133	1.00	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{time:20ms, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 871.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 15.9ms, total_wait_time: 897µs, total_kv_read_wall_time: 20ms}	range:(? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
      └─Projection_135	1.00	0	root		time:1.72ms, open:67.1µs, close:3.2µs, loops:1, Concurrency:OFF	cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#515	2.86 KB	N/A
        └─IndexReader_144	1.00	0	root	partition:p20251101	time:1.71ms, open:61.6µs, close:2.22µs, loops:1, cop_task: {num: 1, max: 1.62ms, proc_keys: 0, tot_proc: 30.7µs, tot_wait: 643.2µs, copr_cache: disabled, build_task_duration: 24.1µs, max_distsql_concurrency: 1}, fetch_resp_duration: 1.63ms, rpc_info:{Cop:{num_rpc:1, total_time:1.6ms}}	index:Selection_143	282 Bytes	N/A
          └─Selection_143	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 617.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 30.7µs, total_wait_time: 643.2µs}	not(isnull(intuit_risk.deviceprofile_fact.exact_id)), or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
            └─IndexRangeScan_142	1.00	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_233	N/A	0	root			Output: ScalarQueryCol#717	N/A	N/A
└─MaxOneRow_161	1.00	1	root		time:2.34ms, open:7µs, close:57µs, loops:1		N/A	N/A
  └─StreamAgg_169	1.00	1	root		time:2.34ms, open:6.49µs, close:56.6µs, loops:2	funcs:sum(Column#636)->Column#637	1.45 KB	N/A
    └─Union_204	2.00	1	root		time:2.33ms, open:833ns, close:56.1µs, loops:2		N/A	N/A
      ├─Projection_206	1.00	0	root		time:2.3ms, open:10.1µs, close:47.6µs, loops:1, Concurrency:OFF	intuit_risk.group_b_180d_daily_rollup.present__b_0018->Column#636	3.64 KB	N/A
      │ └─IndexLookUp_211	1.25	0	root		time:2.29ms, open:6.96µs, close:46µs, loops:1		442 Bytes	N/A
      │   ├─IndexRangeScan_209(Build)	1.25	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, key1, key2, event_day)	time:1.14ms, open:0s, close:0s, loops:1, cop_task: {num: 1, max: 1.11ms, proc_keys: 0, tot_proc: 35µs, tot_wait: 456.9µs, copr_cache: disabled, build_task_duration: 1.03ms, max_distsql_concurrency: 1}, fetch_resp_duration: 1.13ms, rpc_info:{Cop:{num_rpc:1, total_time:1.1ms}}, tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 440.7µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 35µs, total_wait_time: 456.9µs}	range:(? ? ? ?,? ? ? +inf], keep order:false, stats:pseudo	N/A	N/A
      │   └─TableRowIDScan_210(Probe)	1.25	0	cop[tikv]	table:x		keep order:false, stats:pseudo	N/A	N/A
      └─Projection_212	1.00	1	root		time:1.07ms, open:90.2µs, close:7.72µs, loops:2, Concurrency:OFF	cast(Column#635, decimal(38,6) BINARY)->Column#636	380 Bytes	N/A
        └─StreamAgg_224	1.00	1	root		time:1.05ms, open:88.6µs, close:6.04µs, loops:2	funcs:count(Column#704)->Column#635	388 Bytes	N/A
          └─IndexReader_225	1.00	0	root	partition:p20251101	time:1.04ms, open:83.4µs, close:5.57µs, loops:1, cop_task: {num: 1, max: 925.9µs, proc_keys: 0, tot_proc: 25.2µs, tot_wait: 44.3µs, copr_cache: disabled, build_task_duration: 19.7µs, max_distsql_concurrency: 1}, fetch_resp_duration: 941.8µs, rpc_info:{Cop:{num_rpc:1, total_time:907.9µs}}	index:StreamAgg_218	290 Bytes	N/A
            └─StreamAgg_218	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 22.6µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 25.2µs, total_wait_time: 44.3µs}	funcs:count(?)->Column#704	N/A	N/A
              └─Selection_223	1.03	0	cop[tikv]		tikv_task:{time:0s, loops:1}	or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
                └─IndexRangeScan_222	1.28	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_350	N/A	0	root			Output: ScalarQueryCol#1178	N/A	N/A
└─MaxOneRow_305	1.00	1	root		time:1.06ms, open:5.96µs, close:11.7µs, loops:1		N/A	N/A
  └─StreamAgg_313	1.00	1	root		time:1.06ms, open:5.45µs, close:11.4µs, loops:2	funcs:count(distinct Column#1125)->Column#1126	888 Bytes	N/A
    └─Union_332	2.00	0	root		time:1.05ms, open:714ns, close:10.8µs, loops:1		N/A	N/A
      ├─Projection_334	1.00	0	root		time:1.02ms, open:60.5µs, close:6.95µs, loops:1, Concurrency:OFF	cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#1125	3.48 KB	N/A
      │ └─IndexReader_337	1.00	0	root		time:1.01ms, open:55.5µs, close:5.26µs, loops:1, cop_task: {num: 1, max: 932.8µs, proc_keys: 0, tot_proc: 76.1µs, tot_wait: 37.6µs, copr_cache: disabled, build_task_duration: 18.4µs, max_distsql_concurrency: 1}, fetch_resp_duration: 944.4µs, rpc_info:{Cop:{num_rpc:1, total_time:918.8µs}}	index:IndexRangeScan_336	322 Bytes	N/A
      │   └─IndexRangeScan_336	1.00	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 18.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 76.1µs, total_wait_time: 37.6µs}	range:(? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
      └─Projection_338	1.00	0	root		time:942.4µs, open:60.4µs, close:2.98µs, loops:1, Concurrency:OFF	cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#1125	2.86 KB	N/A
        └─IndexReader_347	1.00	0	root	partition:p20251101	time:935µs, open:55.7µs, close:1.99µs, loops:1, cop_task: {num: 1, max: 842.9µs, proc_keys: 0, tot_proc: 19µs, tot_wait: 46µs, copr_cache: disabled, build_task_duration: 15µs, max_distsql_concurrency: 1}, fetch_resp_duration: 867µs, rpc_info:{Cop:{num_rpc:1, total_time:826.3µs}}	index:Selection_346	282 Bytes	N/A
          └─Selection_346	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 22.7µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 19µs, total_wait_time: 46µs}	not(isnull(intuit_risk.deviceprofile_fact.exact_id)), or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
            └─IndexRangeScan_345	1.00	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_436	N/A	0	root			Output: ScalarQueryCol#1327	N/A	N/A
└─MaxOneRow_364	1.00	1	root		time:956.6µs, open:4.71µs, close:9.7µs, loops:1		N/A	N/A
  └─StreamAgg_372	1.00	1	root		time:953.1µs, open:4.34µs, close:9.28µs, loops:2	funcs:sum(Column#1246)->Column#1247	1.45 KB	N/A
    └─Union_407	2.00	1	root		time:945.7µs, open:598ns, close:8.89µs, loops:2		N/A	N/A
      ├─Projection_409	1.00	0	root		time:715.4µs, open:9.91µs, close:4.06µs, loops:1, Concurrency:OFF	intuit_risk.group_b_180d_daily_rollup.present__b_0018->Column#1246	3.64 KB	N/A
      │ └─IndexLookUp_414	1.25	0	root		time:708.6µs, open:5.91µs, close:2.55µs, loops:1		442 Bytes	N/A
      │   ├─IndexRangeScan_412(Build)	1.25	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, key1, key2, event_day)	time:638.6µs, open:0s, close:0s, loops:1, cop_task: {num: 1, max: 606.9µs, proc_keys: 0, tot_proc: 15.5µs, tot_wait: 28.2µs, copr_cache: disabled, build_task_duration: 12.6µs, max_distsql_concurrency: 1}, fetch_resp_duration: 631.5µs, rpc_info:{Cop:{num_rpc:1, total_time:593.8µs}}, tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 12.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 15.5µs, total_wait_time: 28.2µs}	range:(? ? ? ?,? ? ? +inf], keep order:false, stats:pseudo	N/A	N/A
      │   └─TableRowIDScan_413(Probe)	1.25	0	cop[tikv]	table:x		keep order:false, stats:pseudo	N/A	N/A
      └─Projection_415	1.00	1	root		time:911.1µs, open:57.5µs, close:4.31µs, loops:2, Concurrency:OFF	cast(Column#1245, decimal(38,6) BINARY)->Column#1246	380 Bytes	N/A
        └─StreamAgg_427	1.00	1	root		time:899.4µs, open:55.8µs, close:3.54µs, loops:2	funcs:count(Column#1314)->Column#1245	388 Bytes	N/A
          └─IndexReader_428	1.00	0	root	partition:p20251101	time:893.8µs, open:52.5µs, close:3.06µs, loops:1, cop_task: {num: 1, max: 818.9µs, proc_keys: 0, tot_proc: 50.3µs, tot_wait: 36.7µs, copr_cache: disabled, build_task_duration: 15µs, max_distsql_concurrency: 1}, fetch_resp_duration: 827.8µs, rpc_info:{Cop:{num_rpc:1, total_time:807.5µs}}	index:StreamAgg_421	290 Bytes	N/A
            └─StreamAgg_421	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 18.5µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 50.3µs, total_wait_time: 36.7µs}	funcs:count(?)->Column#1314	N/A	N/A
              └─Selection_426	1.03	0	cop[tikv]		tikv_task:{time:0s, loops:1}	or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
                └─IndexRangeScan_425	1.28	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `700.9 ms`
- Storage tasks: `cop[tikv], root`

```sql
WITH raw_boundary AS (
  SELECT
    d.input_ip AS `raw_distinct_0`,
    d.true_ip AS `raw_distinct_1`,
    d.proxy_ip AS `raw_distinct_2`,
    d.exact_id AS `raw_distinct_3`,
    d.agent_type AS `raw_distinct_4`,
    d.agent_os AS `raw_distinct_5`
  FROM deviceprofile_fact d
  WHERE d.smart_id = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 21:03:14.101000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000'
),
distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM `group_b_180d_daily_distinct` x
  WHERE x.bundle_id = 'group_b_bundle_019'
    AND x.template_id IN ('b_0122', 'b_0126', 'b_0130', 'b_0134', 'b_0138', 'b_0142')
    AND x.key1 = %s AND x.key2 = ''
    AND x.event_day > '2025-10-12'
  UNION ALL
  SELECT 'b_0122' AS template_id, CAST(`raw_distinct_0` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_0` IS NOT NULL
  UNION ALL
  SELECT 'b_0126' AS template_id, CAST(`raw_distinct_1` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_1` IS NOT NULL
  UNION ALL
  SELECT 'b_0130' AS template_id, CAST(`raw_distinct_2` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_2` IS NOT NULL
  UNION ALL
  SELECT 'b_0134' AS template_id, CAST(`raw_distinct_3` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_3` IS NOT NULL
  UNION ALL
  SELECT 'b_0138' AS template_id, CAST(`raw_distinct_4` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_4` IS NOT NULL
  UNION ALL
  SELECT 'b_0142' AS template_id, CAST(`raw_distinct_5` AS CHAR(256)) AS distinct_value FROM raw_boundary WHERE `raw_distinct_5` IS NOT NULL
),
unfiltered_counts AS (
  SELECT
    COUNT(DISTINCT CASE WHEN template_id = 'b_0122' THEN distinct_value END) AS `metric__b_0122`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0126' THEN distinct_value END) AS `metric__b_0126`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0130' THEN distinct_value END) AS `metric__b_0130`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0134' THEN distinct_value END) AS `metric__b_0134`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0138' THEN distinct_value END) AS `metric__b_0138`,
    COUNT(DISTINCT CASE WHEN template_id = 'b_0142' THEN distinct_value END) AS `metric__b_0142`
  FROM distinct_values
),
filtered_0 AS (
  SELECT
    (SELECT COUNT(DISTINCT u.distinct_value) FROM (
      SELECT x.distinct_value
      FROM `group_b_180d_daily_distinct` x
      WHERE x.bundle_id = 'group_b_bundle_019'
        AND x.template_id = 'b_0018'
        AND x.key1 = %s AND x.key2 = ''
        AND x.event_day > '2025-10-12'
      UNION ALL
      SELECT CAST(d.exact_id AS CHAR(256)) AS distinct_value
      FROM deviceprofile_fact d
      WHERE d.smart_id = %s AND d.exact_id IS NOT NULL AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 21:03:14.101000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000' AND ((d.request_result LIKE '%%pass%%' OR d.request_result LIKE '%%success%%') AND d.business_transaction REGEXP 'challenge_type=.*idp')
    ) u) AS `metric__b_0018`,
    COALESCE((SELECT SUM(u.presence_count) FROM (
      SELECT x.`present__b_0018` AS presence_count
      FROM `group_b_180d_daily_rollup` x
      WHERE x.bundle_id = 'group_b_bundle_019'
        AND x.key1 = %s AND x.key2 = ''
        AND x.event_day > '2025-10-12'
      UNION ALL
      SELECT COUNT(*) AS presence_count
      FROM deviceprofile_fact d
      WHERE d.smart_id = %s AND d.jms_timestamp IS NOT NULL AND d.jms_timestamp >= '2025-10-12 21:03:14.101000' AND d.jms_timestamp < '2025-10-13 00:00:00.000000' AND ((d.request_result LIKE '%%pass%%' OR d.request_result LIKE '%%success%%') AND d.business_transaction REGEXP 'challenge_type=.*idp')
    ) u), 0) AS `present__b_0018`
)
SELECT
  filtered_0.`metric__b_0018`,
  filtered_0.`present__b_0018`,
  unfiltered_counts.`metric__b_0122`,
  unfiltered_counts.`metric__b_0126`,
  unfiltered_counts.`metric__b_0130`,
  unfiltered_counts.`metric__b_0134`,
  unfiltered_counts.`metric__b_0138`,
  unfiltered_counts.`metric__b_0142`
FROM unfiltered_counts
CROSS JOIN filtered_0;
```

```text
-- explain_analyze_elapsed_ms=700.9
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_666	1.00	1	root		time:674.1ms, open:23.2µs, close:61.4µs, loops:2, RU:1909.08, Concurrency:OFF	Column#1187, Column#1340, Column#790, Column#791, Column#792, Column#793, Column#794, Column#795	4 KB	N/A
└─Projection_668	1.00	1	root		time:674.1ms, open:19.3µs, close:59.7µs, loops:2, Concurrency:OFF	Column#790, Column#791, Column#792, Column#793, Column#794, Column#795, Column#1187, Column#1340	4 KB	N/A
  └─HashJoin_683	1.00	1	root		time:674.1ms, open:15.1µs, close:58.9µs, loops:2, build_hash_table:{total:673.9ms, fetch:673.9ms, build:5.65µs}, probe:{concurrency:5, total:3.37s, max:673.9ms, probe:16.1µs, fetch and wait:3.37s}	CARTESIAN inner join	50.7 KB	0 Bytes
    ├─HashAgg_707(Build)	1.00	1	root		time:673.9ms, open:11.1µs, close:47.6µs, loops:2	funcs:count(distinct Column#1399)->Column#790, funcs:count(distinct Column#1400)->Column#791, funcs:count(distinct Column#1401)->Column#792, funcs:count(distinct Column#1402)->Column#793, funcs:count(distinct Column#1403)->Column#794, funcs:count(distinct Column#1404)->Column#795	31.5 MB	0 Bytes
    │ └─Projection_842	572762.11	768780	root		time:336.3ms, open:1.3µs, close:46.4µs, loops:755, Concurrency:5	case(eq(Column#788, ?), Column#789)->Column#1399, case(eq(Column#788, ?), Column#789)->Column#1400, case(eq(Column#788, ?), Column#789)->Column#1401, case(eq(Column#788, ?), Column#789)->Column#1402, case(eq(Column#788, ?), Column#789)->Column#1403, case(eq(Column#788, ?), Column#789)->Column#1404	985.5 KB	N/A
    │   └─Union_711	572762.11	768780	root		time:352.3ms, open:604ns, close:37.1µs, loops:755		N/A	N/A
    │     ├─Projection_713	572755.74	768780	root		time:351.5ms, open:81.7µs, close:16.3µs, loops:755, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#788, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	982.3 KB	N/A
    │     │ └─IndexReader_720	572755.74	768780	root		time:355ms, open:80.3µs, close:9.04µs, loops:755, cop_task: {num: 33, max: 430.3ms, min: 916.8µs, avg: 38.1ms, p95: 368ms, max_proc_keys: 246752, p95_proc_keys: 234495, tot_proc: 487.3ms, tot_wait: 1ms, copr_cache: disabled, build_task_duration: 28.2µs, max_distsql_concurrency: 6}, fetch_resp_duration: 351.6ms, rpc_info:{Cop:{num_rpc:33, total_time:1.26s}}	index:IndexRangeScan_719	58.3 MB	N/A
    │     │   └─IndexRangeScan_719	572755.74	768780	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:150ms, min:0s, avg: 14.8ms, p80:10ms, p95:130ms, iters:877, tasks:33}, scan_detail: {total_process_keys: 768780, total_process_keys_size: 113149312, total_keys: 68132, get_snapshot_time: 443µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 487.3ms, total_suspend_time: 204.4µs, total_wait_time: 1ms, total_kv_read_wall_time: 90ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    │     ├─Projection_725	1.06	0	root		time:737.7µs, open:48.5µs, close:8.73µs, loops:1, Concurrency:OFF	?->Column#788, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	3.73 KB	N/A
    │     │ └─Selection_727	1.06	0	root		time:732µs, open:46.1µs, close:6.75µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	3.73 KB	N/A
    │     │   └─CTEFullScan_729	1.33	0	root	CTE:raw_boundary	time:725.4µs, open:42.6µs, close:5.67µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    │     ├─Projection_733	1.06	0	root		time:750.8µs, open:746.4µs, close:2.37µs, loops:1, Concurrency:OFF	?->Column#788, cast(cast(intuit_risk.deviceprofile_fact.true_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	68.6 KB	N/A
    │     │ └─Selection_735	1.06	0	root		time:746.5µs, open:744.4µs, close:797ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.true_ip))	35.0 KB	N/A
    │     │   └─CTEFullScan_737	1.33	0	root	CTE:raw_boundary	time:743.1µs, open:742.2µs, close:196ns, loops:1	data:CTE_0	N/A	N/A
    │     ├─Projection_741	1.06	0	root		time:755µs, open:750.1µs, close:2.89µs, loops:1, Concurrency:OFF	?->Column#788, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	3.73 KB	N/A
    │     │ └─Selection_743	1.06	0	root		time:751µs, open:748.6µs, close:1.21µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.73 KB	N/A
    │     │   └─CTEFullScan_745	1.33	0	root	CTE:raw_boundary	time:747.4µs, open:746.6µs, close:172ns, loops:1	data:CTE_0	N/A	N/A
    │     ├─Projection_749	1.06	0	root		time:761.5µs, open:757.6µs, close:1.92µs, loops:1, Concurrency:OFF	?->Column#788, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	3.73 KB	N/A
    │     │ └─Selection_751	1.06	0	root		time:755.9µs, open:753.9µs, close:786ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.73 KB	N/A
    │     │   └─CTEFullScan_753	1.33	0	root	CTE:raw_boundary	time:752.6µs, open:751.8µs, close:165ns, loops:1	data:CTE_0	N/A	N/A
    │     ├─Projection_757	1.06	0	root		time:12.4µs, open:7.33µs, close:1.71µs, loops:1, Concurrency:OFF	?->Column#788, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	3.73 KB	N/A
    │     │ └─Selection_759	1.06	0	root		time:7.65µs, open:4.86µs, close:666ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.73 KB	N/A
    │     │   └─CTEFullScan_761	1.33	0	root	CTE:raw_boundary	time:1.55µs, open:396ns, close:100ns, loops:1	data:CTE_0	N/A	N/A
    │     └─Projection_765	1.06	0	root		time:8.16µs, open:4.63µs, close:1.44µs, loops:1, Concurrency:OFF	?->Column#788, cast(cast(intuit_risk.deviceprofile_fact.agent_os, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#789	4.85 KB	N/A
    │       └─Selection_767	1.06	0	root		time:4.45µs, open:2.63µs, close:558ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_os))	9.23 KB	N/A
    │         └─CTEFullScan_769	1.33	0	root	CTE:raw_boundary	time:906ns, open:172ns, close:76ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_689(Probe)	1.00	1	root		time:13.8µs, open:2.54µs, close:1.48µs, loops:2, Concurrency:OFF	?->Column#1187, ?->Column#1340	0 Bytes	N/A
      └─TableDual_691	1.00	1	root		time:3.41µs, open:205ns, close:235ns, loops:2	rows:1	N/A	N/A
CTE_0	1.33	0	root		time:725.4µs, open:42.6µs, close:5.67µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_646(Seed Part)	1.33	0	root		time:709.3µs, open:39µs, close:4.25µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.true_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.agent_type, intuit_risk.deviceprofile_fact.agent_os	4.72 KB	N/A
  └─IndexReader_655	1.65	0	root	partition:p20251101	time:703.6µs, open:35.7µs, close:3.05µs, loops:1, cop_task: {num: 1, max: 645.6µs, proc_keys: 0, tot_proc: 18.7µs, tot_wait: 42.1µs, copr_cache: disabled, build_task_duration: 10.2µs, max_distsql_concurrency: 1}, fetch_resp_duration: 655.4µs, rpc_info:{Cop:{num_rpc:1, total_time:634.4µs}}	index:Selection_654	282 Bytes	N/A
    └─Selection_654	1.65	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 21.3µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 18.7µs, total_wait_time: 42.1µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.true_ip)), not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)))), or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), or(not(isnull(intuit_risk.deviceprofile_fact.agent_type)), not(isnull(intuit_risk.deviceprofile_fact.agent_os)))))	N/A	N/A
      └─IndexRangeScan_653	1.66	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_201	N/A	0	root			Output: ScalarQueryCol#570	N/A	N/A
└─MaxOneRow_102	1.00	1	root		time:1.37ms, open:6.48µs, close:15.1µs, loops:1		N/A	N/A
  └─StreamAgg_110	1.00	1	root		time:1.37ms, open:5.64µs, close:14.8µs, loops:2	funcs:count(distinct Column#515)->Column#516	888 Bytes	N/A
    └─Union_156	2.00	0	root		time:1.36ms, open:874ns, close:14.4µs, loops:1		N/A	N/A
      ├─Projection_158	1.00	0	root		time:1.32ms, open:76µs, close:9.87µs, loops:1, Concurrency:OFF	cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#515	11.3 KB	N/A
      │ └─IndexReader_165	1.00	0	root		time:1.32ms, open:73.9µs, close:7.84µs, loops:1, cop_task: {num: 1, max: 1.21ms, proc_keys: 0, tot_proc: 248.6µs, tot_wait: 52.3µs, copr_cache: disabled, build_task_duration: 25.5µs, max_distsql_concurrency: 1}, fetch_resp_duration: 1.23ms, rpc_info:{Cop:{num_rpc:1, total_time:1.19ms}}	index:IndexRangeScan_164	322 Bytes	N/A
      │   └─IndexRangeScan_164	1.00	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 20.2µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 248.6µs, total_wait_time: 52.3µs}	range:(? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
      └─Projection_170	1.00	0	root		time:1.09ms, open:81.1µs, close:3.88µs, loops:1, Concurrency:OFF	cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#515	10.7 KB	N/A
        └─IndexReader_183	1.00	0	root	partition:p20251101	time:1.09ms, open:77.5µs, close:2.75µs, loops:1, cop_task: {num: 1, max: 956.1µs, proc_keys: 0, tot_proc: 28.1µs, tot_wait: 63.3µs, copr_cache: disabled, build_task_duration: 20µs, max_distsql_concurrency: 1}, fetch_resp_duration: 995.6µs, rpc_info:{Cop:{num_rpc:1, total_time:932.4µs}}	index:Selection_182	282 Bytes	N/A
          └─Selection_182	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 27.3µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 28.1µs, total_wait_time: 63.3µs}	not(isnull(intuit_risk.deviceprofile_fact.exact_id)), or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
            └─IndexRangeScan_181	1.00	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_336	N/A	0	root			Output: ScalarQueryCol#723	N/A	N/A
└─MaxOneRow_215	1.00	1	root		time:1.01ms, open:4.36µs, close:11.6µs, loops:1		N/A	N/A
  └─StreamAgg_223	1.00	1	root		time:1ms, open:3.97µs, close:11.2µs, loops:2	funcs:sum(Column#638)->Column#639	1.45 KB	N/A
    └─Union_286	2.00	1	root		time:993.1µs, open:728ns, close:10.8µs, loops:2		N/A	N/A
      ├─Projection_288	1.00	0	root		time:648.2µs, open:9.1µs, close:5.2µs, loops:1, Concurrency:OFF	intuit_risk.group_b_180d_daily_rollup.present__b_0018->Column#638	3.64 KB	N/A
      │ └─IndexLookUp_297	1.25	0	root		time:642.3µs, open:6.31µs, close:2.99µs, loops:1		442 Bytes	N/A
      │   ├─IndexRangeScan_295(Build)	1.25	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, key1, key2, event_day)	time:568.3µs, open:0s, close:0s, loops:1, cop_task: {num: 1, max: 530.4µs, proc_keys: 0, tot_proc: 28.1µs, tot_wait: 38.6µs, copr_cache: disabled, build_task_duration: 16.9µs, max_distsql_concurrency: 1}, fetch_resp_duration: 562µs, rpc_info:{Cop:{num_rpc:1, total_time:517µs}}, tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 15.2µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 28.1µs, total_wait_time: 38.6µs}	range:(? ? ? ?,? ? ? +inf], keep order:false, stats:pseudo	N/A	N/A
      │   └─TableRowIDScan_296(Probe)	1.25	0	cop[tikv]	table:x		keep order:false, stats:pseudo	N/A	N/A
      └─Projection_303	1.00	1	root		time:954.2µs, open:44.1µs, close:5.06µs, loops:2, Concurrency:OFF	cast(Column#637, decimal(38,6) BINARY)->Column#638	8.24 KB	N/A
        └─StreamAgg_319	1.00	1	root		time:943.7µs, open:42.7µs, close:4.22µs, loops:2	funcs:count(Column#709)->Column#637	8.25 KB	N/A
          └─IndexReader_320	1.00	0	root	partition:p20251101	time:937.7µs, open:39.7µs, close:3.79µs, loops:1, cop_task: {num: 1, max: 877.1µs, proc_keys: 0, tot_proc: 50.4µs, tot_wait: 38.1µs, copr_cache: disabled, build_task_duration: 11.8µs, max_distsql_concurrency: 1}, fetch_resp_duration: 886.7µs, rpc_info:{Cop:{num_rpc:1, total_time:867.6µs}}	index:StreamAgg_309	290 Bytes	N/A
            └─StreamAgg_309	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 19.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 50.4µs, total_wait_time: 38.1µs}	funcs:count(?)->Column#709	N/A	N/A
              └─Selection_318	1.03	0	cop[tikv]		tikv_task:{time:0s, loops:1}	or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
                └─IndexRangeScan_317	1.28	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_507	N/A	0	root			Output: ScalarQueryCol#1186	N/A	N/A
└─MaxOneRow_408	1.00	1	root		time:1.07ms, open:4.78µs, close:10.3µs, loops:1		N/A	N/A
  └─StreamAgg_416	1.00	1	root		time:1.07ms, open:4.36µs, close:9.97µs, loops:2	funcs:count(distinct Column#1131)->Column#1132	888 Bytes	N/A
    └─Union_462	2.00	0	root		time:1.06ms, open:723ns, close:9.55µs, loops:1		N/A	N/A
      ├─Projection_464	1.00	0	root		time:1.03ms, open:54µs, close:6.68µs, loops:1, Concurrency:OFF	cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#1131	3.48 KB	N/A
      │ └─IndexReader_471	1.00	0	root		time:1.03ms, open:51.6µs, close:4.58µs, loops:1, cop_task: {num: 1, max: 933.4µs, proc_keys: 0, tot_proc: 81.1µs, tot_wait: 50.3µs, copr_cache: disabled, build_task_duration: 15.6µs, max_distsql_concurrency: 1}, fetch_resp_duration: 959.5µs, rpc_info:{Cop:{num_rpc:1, total_time:918.3µs}}	index:IndexRangeScan_470	322 Bytes	N/A
      │   └─IndexRangeScan_470	1.00	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 24µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 81.1µs, total_wait_time: 50.3µs}	range:(? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
      └─Projection_476	1.00	0	root		time:862.7µs, open:34.4µs, close:2.3µs, loops:1, Concurrency:OFF	cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#1131	10.7 KB	N/A
        └─IndexReader_489	1.00	0	root	partition:p20251101	time:859µs, open:32.3µs, close:1.63µs, loops:1, cop_task: {num: 1, max: 811.4µs, proc_keys: 0, tot_proc: 20µs, tot_wait: 37.6µs, copr_cache: disabled, build_task_duration: 9.14µs, max_distsql_concurrency: 1}, fetch_resp_duration: 818.2µs, rpc_info:{Cop:{num_rpc:1, total_time:803.9µs}}	index:Selection_488	282 Bytes	N/A
          └─Selection_488	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 18.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 20µs, total_wait_time: 37.6µs}	not(isnull(intuit_risk.deviceprofile_fact.exact_id)), or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
            └─IndexRangeScan_487	1.00	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
ScalarSubQuery_642	N/A	0	root			Output: ScalarQueryCol#1339	N/A	N/A
└─MaxOneRow_521	1.00	1	root		time:999.4µs, open:4.61µs, close:8.94µs, loops:1		N/A	N/A
  └─StreamAgg_529	1.00	1	root		time:996.2µs, open:4.24µs, close:8.6µs, loops:2	funcs:sum(Column#1254)->Column#1255	1.45 KB	N/A
    └─Union_592	2.00	1	root		time:988.7µs, open:683ns, close:8.17µs, loops:2		N/A	N/A
      ├─Projection_594	1.00	0	root		time:872.8µs, open:9.26µs, close:4.1µs, loops:1, Concurrency:OFF	intuit_risk.group_b_180d_daily_rollup.present__b_0018->Column#1254	3.64 KB	N/A
      │ └─IndexLookUp_603	1.25	0	root		time:867.4µs, open:6.76µs, close:2.53µs, loops:1		442 Bytes	N/A
      │   ├─IndexRangeScan_601(Build)	1.25	0	cop[tikv]	table:x, index:PRIMARY(bundle_id, key1, key2, event_day)	time:769.7µs, open:0s, close:0s, loops:1, cop_task: {num: 1, max: 745.1µs, proc_keys: 0, tot_proc: 18.3µs, tot_wait: 29.3µs, copr_cache: disabled, build_task_duration: 12.5µs, max_distsql_concurrency: 1}, fetch_resp_duration: 763.8µs, rpc_info:{Cop:{num_rpc:1, total_time:734.9µs}}, tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 11.2µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 18.3µs, total_wait_time: 29.3µs}	range:(? ? ? ?,? ? ? +inf], keep order:false, stats:pseudo	N/A	N/A
      │   └─TableRowIDScan_602(Probe)	1.25	0	cop[tikv]	table:x		keep order:false, stats:pseudo	N/A	N/A
      └─Projection_609	1.00	1	root		time:936.4µs, open:46.7µs, close:3.64µs, loops:2, Concurrency:OFF	cast(Column#1253, decimal(38,6) BINARY)->Column#1254	380 Bytes	N/A
        └─StreamAgg_625	1.00	1	root		time:925.9µs, open:45.5µs, close:2.98µs, loops:2	funcs:count(Column#1325)->Column#1253	388 Bytes	N/A
          └─IndexReader_626	1.00	0	root	partition:p20251101	time:921.9µs, open:43.8µs, close:2.64µs, loops:1, cop_task: {num: 1, max: 862µs, proc_keys: 0, tot_proc: 25.2µs, tot_wait: 39.6µs, copr_cache: disabled, build_task_duration: 14.7µs, max_distsql_concurrency: 1}, fetch_resp_duration: 869.9µs, rpc_info:{Cop:{num_rpc:1, total_time:851.3µs}}	index:StreamAgg_615	290 Bytes	N/A
            └─StreamAgg_615	1.00	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 20.1µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 25.2µs, total_wait_time: 39.6µs}	funcs:count(?)->Column#1325	N/A	N/A
              └─Selection_624	1.03	0	cop[tikv]		tikv_task:{time:0s, loops:1}	or(like(intuit_risk.deviceprofile_fact.request_result, ?, ?), like(intuit_risk.deviceprofile_fact.request_result, ?, ?)), regexp(intuit_risk.deviceprofile_fact.business_transaction, ?)	N/A	N/A
                └─IndexRangeScan_623	1.28	0	cop[tikv]	table:d, index:idx_dev_smart_runtime_cov(smart_id, jms_timestamp, interaction_id, agent_type, agent_os, request_result, business_transaction, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, true_ip, true_ip_score)	tikv_task:{time:0s, loops:1}	range:(? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `10095.0 ms`
- Error: `(3024, 'Query execution was interrupted, maximum statement execution time exceeded')`

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
- Elapsed: `1187.1 ms`
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
-- explain_analyze_elapsed_ms=1187.1
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_71	1.00	1	root		time:1.18s, open:14.5µs, close:60.7µs, loops:2, RU:3579.75	funcs:count(distinct Column#188)->Column#150, funcs:count(distinct Column#189)->Column#151, funcs:count(distinct Column#190)->Column#152, funcs:count(distinct Column#191)->Column#153, funcs:count(distinct Column#192)->Column#154	57.1 MB	0 Bytes
└─Projection_171	2121046.92	1557792	root		time:509.7ms, open:2.03µs, close:59.8µs, loops:1527, Concurrency:5	case(eq(Column#148, ?), Column#149)->Column#188, case(eq(Column#148, ?), Column#149)->Column#189, case(eq(Column#148, ?), Column#149)->Column#190, case(eq(Column#148, ?), Column#149)->Column#191, case(eq(Column#148, ?), Column#149)->Column#192	996.1 KB	N/A
  └─Union_75	2121046.92	1557792	root		time:520.9ms, open:879ns, close:41.2µs, loops:1527		N/A	N/A
    ├─Projection_77	2121041.42	1557792	root		time:521.6ms, open:1.16ms, close:21.3µs, loops:1527, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#148, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	915.2 KB	N/A
    │ └─IndexReader_80	2121041.42	1557792	root		time:527.4ms, open:1.16ms, close:11.4µs, loops:1527, cop_task: {num: 38, max: 485.3ms, min: 1.24ms, avg: 65.5ms, p95: 480.7ms, max_proc_keys: 341984, p95_proc_keys: 286688, tot_proc: 1.16s, tot_wait: 6.24ms, copr_cache: disabled, build_task_duration: 1.09ms, max_distsql_concurrency: 5}, fetch_resp_duration: 520.9ms, rpc_info:{Cop:{num_rpc:38, total_time:2.49s}}	index:IndexRangeScan_79	96.1 MB	N/A
    │   └─IndexRangeScan_79	2121041.42	1557792	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:200ms, min:0s, avg: 28.9ms, p80:20ms, p95:200ms, iters:1668, tasks:38}, scan_detail: {total_process_keys: 1557792, total_process_keys_size: 208062197, total_keys: 198097, get_snapshot_time: 5.7ms, rocksdb: {block: {}}}, time_detail: {total_process_time: 1.16s, total_suspend_time: 507.2µs, total_wait_time: 6.24ms, total_kv_read_wall_time: 250ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_81	1.10	0	root		time:1.46ms, open:96.8µs, close:10.7µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_83	1.10	0	root		time:1.45ms, open:94.6µs, close:8.51µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.11 KB	N/A
    │   └─CTEFullScan_85	1.37	0	root	CTE:raw_boundary	time:1.44ms, open:89.6µs, close:7.13µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_89	1.10	0	root		time:1.47ms, open:1.47ms, close:1.77µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_91	1.10	0	root		time:1.47ms, open:1.46ms, close:591ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	3.11 KB	N/A
    │   └─CTEFullScan_93	1.37	0	root	CTE:raw_boundary	time:1.46ms, open:1.46ms, close:190ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_97	1.10	0	root		time:1.48ms, open:1.48ms, close:2.04µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_99	1.10	0	root		time:1.48ms, open:1.47ms, close:722ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	3.11 KB	N/A
    │   └─CTEFullScan_101	1.37	0	root	CTE:raw_boundary	time:1.47ms, open:1.47ms, close:160ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_105	1.10	0	root		time:1.49ms, open:1.48ms, close:1.79µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_107	1.10	0	root		time:1.48ms, open:1.48ms, close:477ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.11 KB	N/A
    │   └─CTEFullScan_109	1.37	0	root	CTE:raw_boundary	time:1.48ms, open:1.48ms, close:162ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_113	1.10	0	root		time:15.7µs, open:10.8µs, close:1.83µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
      └─Selection_115	1.10	0	root		time:10.1µs, open:7.5µs, close:835ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.11 KB	N/A
        └─CTEFullScan_117	1.37	0	root	CTE:raw_boundary	time:1.24µs, open:259ns, close:165ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.37	0	root		time:1.44ms, open:89.6µs, close:7.13µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_58(Seed Part)	1.37	0	root		time:1.43ms, open:86.3µs, close:5.35µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.agent_type	4.10 KB	N/A
  └─IndexReader_63	1.71	0	root	partition:p20251101	time:1.42ms, open:80.1µs, close:3.7µs, loops:1, cop_task: {num: 1, max: 1.31ms, proc_keys: 0, tot_proc: 29.4µs, tot_wait: 599µs, copr_cache: disabled, build_task_duration: 22.5µs, max_distsql_concurrency: 1}, fetch_resp_duration: 1.32ms, rpc_info:{Cop:{num_rpc:1, total_time:1.29ms}}	index:Selection_62	255 Bytes	N/A
    └─Selection_62	1.71	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 570.8µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 29.4µs, total_wait_time: 599µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type)))))	N/A	N/A
      └─IndexRangeScan_61	1.72	0	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### cost

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1114.6 ms`
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
-- explain_analyze_elapsed_ms=1114.6
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_80	1.00	1	root		time:1.11s, open:11µs, close:63.6µs, loops:2, RU:3544.86	funcs:count(distinct Column#188)->Column#150, funcs:count(distinct Column#189)->Column#151, funcs:count(distinct Column#190)->Column#152, funcs:count(distinct Column#191)->Column#153, funcs:count(distinct Column#192)->Column#154	57.1 MB	0 Bytes
└─Projection_198	2121046.92	1557792	root		time:457.2ms, open:2.12µs, close:62.5µs, loops:1526, Concurrency:5	case(eq(Column#148, ?), Column#149)->Column#188, case(eq(Column#148, ?), Column#149)->Column#189, case(eq(Column#148, ?), Column#149)->Column#190, case(eq(Column#148, ?), Column#149)->Column#191, case(eq(Column#148, ?), Column#149)->Column#192	1000.1 KB	N/A
  └─Union_84	2121046.92	1557792	root		time:467.6ms, open:716ns, close:41.1µs, loops:1526		N/A	N/A
    ├─Projection_86	2121041.42	1557792	root		time:468.2ms, open:100.1µs, close:21.2µs, loops:1526, Concurrency:5	intuit_risk.group_b_180d_daily_distinct.template_id->Column#148, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	917.1 KB	N/A
    │ └─IndexReader_93	2121041.42	1557792	root		time:473.7ms, open:99.3µs, close:11.2µs, loops:1526, cop_task: {num: 38, max: 450.7ms, min: 832.2µs, avg: 60ms, p95: 444.8ms, max_proc_keys: 341984, p95_proc_keys: 286688, tot_proc: 1.05s, tot_wait: 1.09ms, copr_cache: disabled, build_task_duration: 35.1µs, max_distsql_concurrency: 5}, fetch_resp_duration: 468.4ms, rpc_info:{Cop:{num_rpc:38, total_time:2.28s}}	index:IndexRangeScan_92	96.1 MB	N/A
    │   └─IndexRangeScan_92	2121041.42	1557792	cop[tikv]	table:x, index:PRIMARY(bundle_id, template_id, key1, key2, event_day, distinct_value)	tikv_task:{proc max:200ms, min:0s, avg: 27.4ms, p80:20ms, p95:190ms, iters:1668, tasks:38}, scan_detail: {total_process_keys: 1557792, total_process_keys_size: 208062197, total_keys: 198097, get_snapshot_time: 547.3µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 1.05s, total_suspend_time: 447.1µs, total_wait_time: 1.09ms, total_kv_read_wall_time: 220ms}	range:(? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], (? ? ? ? ?,? ? ? ? +inf], keep order:false	N/A	N/A
    ├─Projection_98	1.10	0	root		time:896.2µs, open:79µs, close:10.5µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_100	1.10	0	root		time:889µs, open:75.5µs, close:8.41µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.11 KB	N/A
    │   └─CTEFullScan_102	1.37	0	root	CTE:raw_boundary	time:882.9µs, open:72.3µs, close:7.36µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_106	1.10	0	root		time:901.3µs, open:897µs, close:2.38µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_108	1.10	0	root		time:898µs, open:895.6µs, close:1.16µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	3.11 KB	N/A
    │   └─CTEFullScan_110	1.37	0	root	CTE:raw_boundary	time:894.3µs, open:893.6µs, close:126ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_114	1.10	0	root		time:905.6µs, open:901.8µs, close:1.78µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.98 KB	N/A
    │ └─Selection_116	1.10	0	root		time:902.5µs, open:900.7µs, close:498ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	8.61 KB	N/A
    │   └─CTEFullScan_118	1.37	0	root	CTE:raw_boundary	time:897.7µs, open:897.1µs, close:94ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_122	1.10	0	root		time:912.5µs, open:909.1µs, close:1.84µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	62.2 KB	N/A
    │ └─Selection_124	1.10	0	root		time:906.6µs, open:905µs, close:610ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.11 KB	N/A
    │   └─CTEFullScan_126	1.37	0	root	CTE:raw_boundary	time:903.7µs, open:903.1µs, close:129ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_130	1.10	0	root		time:10.4µs, open:5.66µs, close:1.88µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
      └─Selection_132	1.10	0	root		time:6.48µs, open:4.01µs, close:659ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.11 KB	N/A
        └─CTEFullScan_134	1.37	0	root	CTE:raw_boundary	time:1.34µs, open:312ns, close:111ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.37	0	root		time:882.9µs, open:72.3µs, close:7.36µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_58(Seed Part)	1.37	0	root		time:865.5µs, open:69µs, close:5.76µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.agent_type	4.10 KB	N/A
  └─IndexReader_67	1.71	0	root	partition:p20251101	time:856.9µs, open:63.1µs, close:4.3µs, loops:1, cop_task: {num: 1, max: 759.7µs, proc_keys: 0, tot_proc: 31µs, tot_wait: 49.7µs, copr_cache: disabled, build_task_duration: 19.4µs, max_distsql_concurrency: 1}, fetch_resp_duration: 774.1µs, rpc_info:{Cop:{num_rpc:1, total_time:741.8µs}}	index:Selection_66	255 Bytes	N/A
    └─Selection_66	1.71	0	cop[tikv]		tikv_task:{time:0s, loops:1}, scan_detail: {total_keys: 1, get_snapshot_time: 23.9µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 31µs, total_wait_time: 49.7µs}	or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type)))))	N/A	N/A
      └─IndexRangeScan_65	1.72	0	cop[tikv]	table:d, index:idx_dev_true_runtime_cov(true_ip, jms_timestamp, interaction_id, agent_type, device_fingerprint_score, device_score, device_worst_score, exact_id, input_ip, input_ip_score, proxy_ip, smart_id, true_ip_score)	tikv_task:{time:0s, loops:1}	range:[? ?,? ?), keep order:false	N/A	N/A
```

### tiflash_only

- Engines: `tiflash,tidb`
- Elapsed: `7383.5 ms`
- Storage tasks: `mpp[tiflash], root`

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
-- explain_analyze_elapsed_ms=7383.5
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
HashAgg_74	1.00	1	root		time:7.38s, open:9.78µs, close:51.4µs, loops:2, RU:1270510.33	funcs:count(distinct Column#176)->Column#150, funcs:count(distinct Column#177)->Column#151, funcs:count(distinct Column#178)->Column#152, funcs:count(distinct Column#179)->Column#153, funcs:count(distinct Column#180)->Column#154	57.1 MB	0 Bytes
└─Projection_188	2121046.92	1557792	root		time:6.55s, open:1.85µs, close:50.6µs, loops:2727, Concurrency:5	case(eq(Column#148, ?), Column#149)->Column#176, case(eq(Column#148, ?), Column#149)->Column#177, case(eq(Column#148, ?), Column#149)->Column#178, case(eq(Column#148, ?), Column#149)->Column#179, case(eq(Column#148, ?), Column#149)->Column#180	1.01 MB	N/A
  └─Union_78	2121046.92	1557792	root		time:7.34s, open:922ns, close:29.3µs, loops:2727		N/A	N/A
    ├─TableReader_89	2121041.42	1557792	root		time:7.25s, open:5.57ms, close:7.79µs, loops:2727, cop_task: {num: 1492, max: 0s, min: 0s, avg: 0s, p95: 0s, copr_cache: disabled}, fetch_resp_duration: 7.24s	MppVersion: 3, data:ExchangeSender_88	63.9 KB	N/A
    │ └─ExchangeSender_88	2121041.42	1557792	mpp[tiflash]		tiflash_task:{proc max:7.36s, min:6.97s, avg: 7.15s, p80:7.36s, p95:7.36s, iters:10072, tasks:3, threads:189}, tiflash_network: {inner_zone_send_bytes: 74542567}	ExchangeType: PassThrough	N/A	N/A
    │   └─Projection_81	2121041.42	1557792	mpp[tiflash]		tiflash_task:{proc max:7.36s, min:6.97s, avg: 7.15s, p80:7.36s, p95:7.36s, iters:10072, tasks:3, threads:189}	intuit_risk.group_b_180d_daily_distinct.template_id->Column#148, cast(intuit_risk.group_b_180d_daily_distinct.distinct_value, varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	N/A	N/A
    │     └─Selection_87	2121041.42	1557792	mpp[tiflash]		tiflash_task:{proc max:7.36s, min:6.97s, avg: 7.15s, p80:7.36s, p95:7.36s, iters:10072, tasks:3, threads:189}	eq(intuit_risk.group_b_180d_daily_distinct.bundle_id, ?), eq(intuit_risk.group_b_180d_daily_distinct.key2, ?), gt(intuit_risk.group_b_180d_daily_distinct.event_day, ?), in(intuit_risk.group_b_180d_daily_distinct.template_id, ?, ?, ?, ?, ?)	N/A	N/A
    │       └─TableFullScan_86	2993380.00	3090779	mpp[tiflash]	table:x	tiflash_task:{proc max:7.36s, min:6.97s, avg: 7.15s, p80:7.36s, p95:7.36s, iters:19042, tasks:3, threads:189}, tiflash_wait: {pipeline_queue_wait: 20ms}, tiflash_network: {inner_zone_send_bytes: 417513, inter_zone_send_bytes: 209441, inner_zone_receive_bytes: 417513, inter_zone_receive_bytes: 209441}, tiflash_scan:{mvcc_input_rows:188416, mvcc_input_bytes:3203072, mvcc_output_rows:188416, local_regions:0, remote_regions:4083, tot_learner_read:0ms, region_balance:{instance_num: 3, max/min: 1391/1333=1.043511}, delta_rows:0, delta_bytes:0, segments:5058, stale_read_regions:0, tot_build_snapshot:0ms, tot_build_bitmap:42028ms, tot_build_inputstream:1074271ms, min_local_stream:0ms, max_local_stream:0ms, min_remote_stream:6689ms, max_remote_stream:7100ms, dtfile:{data_scanned_rows:1083618834, data_skipped_rows:2839413467, mvcc_scanned_rows:663552, mvcc_skipped_rows:526569064, lm_filter_scanned_rows:2543895225, lm_filter_skipped_rows:944897819, tot_rs_index_check:1800ms, tot_read:9226212ms, disagg_cache_hit_bytes: 61404271876, disagg_cache_miss_bytes: 94866033996}}	pushed down filter:eq(intuit_risk.group_b_180d_daily_distinct.key1, ?), keep order:false	N/A	N/A
    ├─Projection_90	1.10	0	root		time:20.2ms, open:1.16ms, close:10.8µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.exact_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_92	1.10	0	root		time:20.2ms, open:1.15ms, close:8.49µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.exact_id))	3.11 KB	N/A
    │   └─CTEFullScan_94	1.37	0	root	CTE:raw_boundary	time:20.2ms, open:1.15ms, close:7.37µs, loops:1	data:CTE_0	0 Bytes	0 Bytes
    ├─Projection_98	1.10	0	root		time:20.3ms, open:20.2ms, close:2.76µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.smart_id, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	8.61 KB	N/A
    │ └─Selection_100	1.10	0	root		time:20.2ms, open:20.2ms, close:1.21µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.smart_id))	3.11 KB	N/A
    │   └─CTEFullScan_102	1.37	0	root	CTE:raw_boundary	time:20.2ms, open:20.2ms, close:210ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_106	1.10	0	root		time:20.3ms, open:20.2ms, close:2.25µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.input_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.98 KB	N/A
    │ └─Selection_108	1.10	0	root		time:20.2ms, open:20.2ms, close:931ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.input_ip))	3.11 KB	N/A
    │   └─CTEFullScan_110	1.37	0	root	CTE:raw_boundary	time:20.2ms, open:20.2ms, close:173ns, loops:1	data:CTE_0	N/A	N/A
    ├─Projection_114	1.10	0	root		time:20.2ms, open:20.2ms, close:1.96µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.proxy_ip, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	3.11 KB	N/A
    │ └─Selection_116	1.10	0	root		time:20.2ms, open:20.2ms, close:769ns, loops:1	not(isnull(intuit_risk.deviceprofile_fact.proxy_ip))	3.11 KB	N/A
    │   └─CTEFullScan_118	1.37	0	root	CTE:raw_boundary	time:20.2ms, open:20.2ms, close:164ns, loops:1	data:CTE_0	N/A	N/A
    └─Projection_122	1.10	0	root		time:30.7µs, open:26.1µs, close:2.08µs, loops:1, Concurrency:OFF	?->Column#148, cast(cast(intuit_risk.deviceprofile_fact.agent_type, var_string(256)), varchar(257) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)->Column#149	62.0 KB	N/A
      └─Selection_124	1.10	0	root		time:25.3µs, open:22.4µs, close:1.16µs, loops:1	not(isnull(intuit_risk.deviceprofile_fact.agent_type))	3.11 KB	N/A
        └─CTEFullScan_126	1.37	0	root	CTE:raw_boundary	time:1.5µs, open:544ns, close:112ns, loops:1	data:CTE_0	N/A	N/A
CTE_0	1.37	0	root		time:20.2ms, open:1.15ms, close:7.37µs, loops:1	Non-Recursive CTE	0 Bytes	0 Bytes
└─Projection_58(Seed Part)	1.37	0	root		time:20.2ms, open:1.15ms, close:5.89µs, loops:1, Concurrency:OFF	intuit_risk.deviceprofile_fact.exact_id, intuit_risk.deviceprofile_fact.smart_id, intuit_risk.deviceprofile_fact.input_ip, intuit_risk.deviceprofile_fact.proxy_ip, intuit_risk.deviceprofile_fact.agent_type	4.10 KB	N/A
  └─TableReader_64	1.37	0	root	partition:p20251101	time:20.2ms, open:1.14ms, close:4.54µs, loops:1, cop_task: {num: 1, max: 0s, proc_keys: 0, copr_cache: disabled}, fetch_resp_duration: 19ms	MppVersion: 3, data:ExchangeSender_63	1004 Bytes	N/A
    └─ExchangeSender_63	1.37	0	mpp[tiflash]		tiflash_task:{time:16.4ms, loops:0, threads:63}	ExchangeType: PassThrough	N/A	N/A
      └─Selection_62	1.37	0	mpp[tiflash]		tiflash_task:{time:16.4ms, loops:0, threads:63}	eq(intuit_risk.deviceprofile_fact.true_ip, ?), or(or(not(isnull(intuit_risk.deviceprofile_fact.exact_id)), not(isnull(intuit_risk.deviceprofile_fact.smart_id))), or(not(isnull(intuit_risk.deviceprofile_fact.input_ip)), or(not(isnull(intuit_risk.deviceprofile_fact.proxy_ip)), not(isnull(intuit_risk.deviceprofile_fact.agent_type)))))	N/A	N/A
        └─TableFullScan_61	25.91	0	mpp[tiflash]	table:d	tiflash_task:{time:16.4ms, loops:0, threads:63}, tiflash_scan:{mvcc_input_rows:0, mvcc_input_bytes:0, mvcc_output_rows:0, local_regions:0, remote_regions:1, tot_learner_read:0ms, region_balance:{instance_num: 1, max/min: 1/1=1.000000}, delta_rows:0, delta_bytes:0, segments:0, stale_read_regions:0, tot_build_snapshot:0ms, tot_build_bitmap:0ms, tot_build_inputstream:0ms, min_local_stream:0ms, max_local_stream:0ms, dtfile:{data_scanned_rows:0, data_skipped_rows:0, mvcc_scanned_rows:0, mvcc_skipped_rows:0, lm_filter_scanned_rows:0, lm_filter_skipped_rows:0, tot_rs_index_check:0ms, tot_read:0ms}}	pushed down filter:ge(intuit_risk.deviceprofile_fact.jms_timestamp, ?), lt(intuit_risk.deviceprofile_fact.jms_timestamp, ?), not(isnull(intuit_risk.deviceprofile_fact.jms_timestamp)), keep order:false, PartitionTableScan:true	N/A	N/A
```
