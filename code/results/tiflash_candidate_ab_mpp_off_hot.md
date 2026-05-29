# TiKV vs TiFlash Candidate A/B

- Generated: `2026-05-29T22:26:48`
- Mixed JSON: `/home/ec2-user/tidb_intuit_perf_support_bundle_lean/code/results/mixed_traffic_1780091850.json`
- Pre-agg layout: `prod180`
- Pre-agg bundle count: `0`
- Session knobs: distinct_pushdown=`True`, force_inline_cte=`0`, hashagg_final=`16`, hashagg_partial=`8`

## Summary

| bundle | group | event | variant | engines | elapsed | storage tasks | result |
|---|---:|---|---|---|---:|---|---|
| group_b_bundle_012 | B | hot_true_ip:true_ip | tiflash_hint_mpp_off | tikv,tiflash,tidb | 3.8 ms |  | (1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated t |
| group_b_bundle_018 | B | hot_input_ip:input_ip | tiflash_hint_mpp_off | tikv,tiflash,tidb | 2.0 ms |  | (1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated t |
| group_b_bundle_020 | B | hot_true_ip:true_ip | tiflash_hint_mpp_off | tikv,tiflash,tidb | 2.0 ms |  | (1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated t |
| group_c_bundle_018 | C | hot_true_ip:true_ip | tiflash_hint_mpp_off | tikv,tiflash,tidb | 2.7 ms |  | (1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated t |
| group_a_bundle_006 | A | hot_check_bank_routing_number:check_bank_routing_number | tiflash_hint_mpp_off | tikv,tiflash,tidb | 13.8 ms |  | (1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated t |
| group_a_bundle_010 | A | hot_check_bank_routing_number:check_bank_routing_number | tiflash_hint_mpp_off | tikv,tiflash,tidb | 12.3 ms |  | (1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated t |

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

### tiflash_hint_mpp_off

- Engines: `tikv,tiflash,tidb`
- Elapsed: `3.8 ms`
- Error: `(1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch")`

## 2. group_b_bundle_018

- Group/window/filter: `B` / `180d` / `d.input_ip = %s`
- Preagg applied: `False`
- Event: invoice=`INV0019249439` kind=`hot_input_ip` hot_field=`input_ip` hot_count=`719377` ref=`2026-04-10T22:19:54.592000`

### Params

```json
[
  "74.179.68.52"
]
```

### tiflash_hint_mpp_off

- Engines: `tikv,tiflash,tidb`
- Elapsed: `2.0 ms`
- Error: `(1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch")`

## 3. group_b_bundle_020

- Group/window/filter: `B` / `180d` / `d.true_ip = %s`
- Preagg applied: `False`
- Event: invoice=`INV0007589128` kind=`hot_true_ip` hot_field=`true_ip` hot_count=`738824` ref=`2026-04-10T23:06:57.563000`

### Params

```json
[
  "72.153.231.69"
]
```

### tiflash_hint_mpp_off

- Engines: `tikv,tiflash,tidb`
- Elapsed: `2.0 ms`
- Error: `(1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch")`

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

### tiflash_hint_mpp_off

- Engines: `tikv,tiflash,tidb`
- Elapsed: `2.7 ms`
- Error: `(1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch")`

## 5. group_a_bundle_006

- Group/window/filter: `A` / `7d` / `p.check_bank_routing_number = %s`
- Preagg applied: `False`
- Event: invoice=`INV0038298794` kind=`hot_check_bank_routing_number` hot_field=`check_bank_routing_number` hot_count=`671993` ref=`2026-04-10T23:53:15.502000`

### Params

```json
[
  "322271627"
]
```

### tiflash_hint_mpp_off

- Engines: `tikv,tiflash,tidb`
- Elapsed: `13.8 ms`
- Error: `(1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch")`

## 6. group_a_bundle_010

- Group/window/filter: `A` / `30d` / `p.check_bank_routing_number = %s`
- Preagg applied: `False`
- Event: invoice=`INV0038298794` kind=`hot_check_bank_routing_number` hot_field=`check_bank_routing_number` hot_count=`671993` ref=`2026-04-10T23:53:15.502000`

### Params

```json
[
  "322271627"
]
```

### tiflash_hint_mpp_off

- Engines: `tikv,tiflash,tidb`
- Elapsed: `12.3 ms`
- Error: `(1815, "Internal : Can't find a proper physical plan for this query: cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch")`
