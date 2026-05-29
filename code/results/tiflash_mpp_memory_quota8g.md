# TiKV vs TiFlash Candidate A/B

- Generated: `2026-05-29T22:29:16`
- Mixed JSON: `/home/ec2-user/tidb_intuit_perf_support_bundle_lean/code/results/mixed_traffic_1780091850.json`
- Pre-agg layout: `prod180`
- Pre-agg bundle count: `0`
- Session knobs: distinct_pushdown=`True`, force_inline_cte=`0`, hashagg_final=`16`, hashagg_partial=`8`

## Summary

| bundle | group | event | variant | engines | elapsed | storage tasks | result |
|---|---:|---|---|---|---:|---|---|
| group_b_bundle_012 | B | hot_true_ip:true_ip | tiflash_hint | tikv,tiflash,tidb | 1819.4 ms |  | (1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code |
| group_a_bundle_006 | A | hot_check_bank_routing_number:check_bank_routing_number | tiflash_hint | tikv,tiflash,tidb | 83.5 ms |  | (1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code |

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

### tiflash_hint

- Engines: `tikv,tiflash,tidb`
- Elapsed: `1819.4 ms`
- Error: `(1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code: 0, e.displayText() = DB::Exception: Memory limit exceeded caused by 'RSS(Resident Set Size) much larger than limit' : process memory size would be 4.81 GiB for (attempt to allocate chunk of 2097152 bytes), limit of memory for data computing : 3.76 GiB. Memory Usage of Storage: non-query: peak=0.00 B, amount=0.00 B; kvstore: peak=0.00 B, amount=0.00 B; query-storage-task: peak=977.21 MiB, amount=249.05 MiB; fetch-pages: peak=1.48 MiB, amount=0.00 B; shared-column-data: peak=977.21 MiB, amount=251.05 MiB.: (while reading from DTFile: s3://s455/data/ks_1_t_113/dmf_1830, column: 20:exchange_receiver_1), e.what() = DB::Exception,, e.what() = DB::Exception,")`

## 2. group_a_bundle_006

- Group/window/filter: `A` / `7d` / `p.check_bank_routing_number = %s`
- Preagg applied: `False`
- Event: invoice=`INV0038298794` kind=`hot_check_bank_routing_number` hot_field=`check_bank_routing_number` hot_count=`671993` ref=`2026-04-10T23:53:15.502000`

### Params

```json
[
  "322271627"
]
```

### tiflash_hint

- Engines: `tikv,tiflash,tidb`
- Elapsed: `83.5 ms`
- Error: `(1105, "other error for mpp stream: Code: 0, e.displayText() = DB::Exception: Receiver state: ERROR, error message: Code: 0, e.displayText() = DB::Exception: Memory limit (total) exceeded caused by 'RSS(Resident Set Size) much larger than limit' : process memory size would be 4.83 GiB for (attempt to allocate chunk of 1065561 bytes), limit of memory for data computing : 3.76 GiB. Memory Usage of Storage: non-query: peak=0.00 B, amount=0.00 B; kvstore: peak=0.00 B, amount=0.00 B; query-storage-task: peak=977.21 MiB, amount=52.43 MiB; fetch-pages: peak=1.48 MiB, amount=0.00 B; shared-column-data: peak=977.21 MiB, amount=52.43 MiB., e.what() = DB::Exception,, e.what() = DB::Exception,")`
