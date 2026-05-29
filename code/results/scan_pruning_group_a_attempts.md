# Scan/CASE-Pruning Rewrite Attempts

- Generated: `2026-05-29T13:26:00`
- Mixed JSON: `/Users/dylanliu/Downloads/tidb_intuit_perf_support_bundle_lean/code/results/mixed_traffic_1780029697.json`
- Slow CSV: `/Users/dylanliu/Downloads/tidb_intuit_perf_support_bundle_lean/code/results/slow_bundles_post_index_3eps_60s.csv`
- Session baseline for all tests: TiKV/TiDB only, CTE force-inline off, distinct pushdown off unless candidate timing variant enables it.

## group_a_bundle_010

- Group/window/filter: `A` / `30d` / `p.check_bank_routing_number = %s`
- Chosen event: `INV0046519149` kind=`hot_check_bank_routing_number` bundle_ms=`-1.0` event_ms=`695.0`
- Bundle stats: n=`181` >350=`0` >500=`0` p95=`206.9ms` max=`287.9ms`

### Current Optimized

- SELECT time: `447.4ms` result=`ok`
- EXPLAIN ANALYZE: `420.9ms`, scan_sum=`119821`, scan_max=`119821`

### Candidate: group_a_dimension_rollup

- Result check: `same`; SELECT time: `123.0ms`
- Best EXPLAIN ANALYZE: `115.1ms` variant=`optimized_hashagg_16_8` scan_sum=`119821` scan_max=`119821` accepted=`True`

| Variant | Time | Scan Sum | Scan Max | Result |
| --- | ---: | ---: | ---: | --- |
| `candidate_default` | 115.7 ms | 119821 | 119821 | ok |
| `optimized_hashagg_16_8` | 115.1 ms | 119821 | 119821 | ok |
| `optimized_hashagg_32_8` | 116.7 ms | 119821 | 119821 | ok |
| `optimized_distinct_pushdown` | 115.7 ms | 119821 | 119821 | ok |
| `optimized_distinct_pushdown_hashagg_16_8` | 115.2 ms | 119821 | 119821 | ok |

#### Candidate SQL

```sql
SELECT
  SUM(b.row_count) AS `metric__a_0041`,
  SUM(b.amount_sum) AS `metric__a_0042`,
  MIN(b.amount_min) AS `metric__a_0043`,
  MAX(b.amount_max) AS `metric__a_0044`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `metric__a_1001`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1001`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.amount_sum END) AS `metric__a_1002`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1002`,
  MIN(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.amount_min END) AS `metric__a_1003`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1003`,
  MAX(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.amount_max END) AS `metric__a_1004`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1004`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `metric__a_1013`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1013`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.amount_sum END) AS `metric__a_1014`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1014`,
  MIN(CASE WHEN b.mt_gateway = 'QBMS' THEN b.amount_min END) AS `metric__a_1015`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1015`,
  MAX(CASE WHEN b.mt_gateway = 'QBMS' THEN b.amount_max END) AS `metric__a_1016`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1016`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `metric__a_1025`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1025`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.amount_sum END) AS `metric__a_1026`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1026`,
  MIN(CASE WHEN b.mt_gateway = 'Direct' THEN b.amount_min END) AS `metric__a_1027`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1027`,
  MAX(CASE WHEN b.mt_gateway = 'Direct' THEN b.amount_max END) AS `metric__a_1028`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1028`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `metric__a_1037`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1037`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.amount_sum END) AS `metric__a_1038`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1038`,
  MIN(CASE WHEN b.mt_gateway = 'PayPal' THEN b.amount_min END) AS `metric__a_1039`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1039`,
  MAX(CASE WHEN b.mt_gateway = 'PayPal' THEN b.amount_max END) AS `metric__a_1040`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1040`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `metric__a_1049`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1049`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.amount_sum END) AS `metric__a_1050`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1050`,
  MIN(CASE WHEN b.mt_gateway = 'Stripe' THEN b.amount_min END) AS `metric__a_1051`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1051`,
  MAX(CASE WHEN b.mt_gateway = 'Stripe' THEN b.amount_max END) AS `metric__a_1052`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1052`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `metric__a_1061`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1061`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.amount_sum END) AS `metric__a_1062`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1062`,
  MIN(CASE WHEN b.mt_gateway = 'Square' THEN b.amount_min END) AS `metric__a_1063`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1063`,
  MAX(CASE WHEN b.mt_gateway = 'Square' THEN b.amount_max END) AS `metric__a_1064`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1064`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `metric__a_1073`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1073`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.amount_sum END) AS `metric__a_1074`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1074`,
  MIN(CASE WHEN b.mt_gateway = 'Braintree' THEN b.amount_min END) AS `metric__a_1075`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1075`,
  MAX(CASE WHEN b.mt_gateway = 'Braintree' THEN b.amount_max END) AS `metric__a_1076`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1076`,
  SUM(CASE WHEN b.transaction_type = 'Sale' THEN b.row_count ELSE 0 END) AS `metric__a_1089`,
  SUM(CASE WHEN b.transaction_type = 'Sale' THEN b.row_count ELSE 0 END) AS `present__a_1089`,
  SUM(CASE WHEN b.transaction_type = 'Sale' THEN b.amount_sum END) AS `metric__a_1090`,
  SUM(CASE WHEN b.transaction_type = 'Sale' THEN b.row_count ELSE 0 END) AS `present__a_1090`,
  MIN(CASE WHEN b.transaction_type = 'Sale' THEN b.amount_min END) AS `metric__a_1091`,
  SUM(CASE WHEN b.transaction_type = 'Sale' THEN b.row_count ELSE 0 END) AS `present__a_1091`,
  MAX(CASE WHEN b.transaction_type = 'Sale' THEN b.amount_max END) AS `metric__a_1092`,
  SUM(CASE WHEN b.transaction_type = 'Sale' THEN b.row_count ELSE 0 END) AS `present__a_1092`,
  SUM(CASE WHEN b.transaction_type = 'Void' THEN b.row_count ELSE 0 END) AS `metric__a_1101`,
  SUM(CASE WHEN b.transaction_type = 'Void' THEN b.row_count ELSE 0 END) AS `present__a_1101`,
  SUM(CASE WHEN b.transaction_type = 'Void' THEN b.amount_sum END) AS `metric__a_1102`,
  SUM(CASE WHEN b.transaction_type = 'Void' THEN b.row_count ELSE 0 END) AS `present__a_1102`,
  MIN(CASE WHEN b.transaction_type = 'Void' THEN b.amount_min END) AS `metric__a_1103`,
  SUM(CASE WHEN b.transaction_type = 'Void' THEN b.row_count ELSE 0 END) AS `present__a_1103`,
  MAX(CASE WHEN b.transaction_type = 'Void' THEN b.amount_max END) AS `metric__a_1104`,
  SUM(CASE WHEN b.transaction_type = 'Void' THEN b.row_count ELSE 0 END) AS `present__a_1104`,
  SUM(CASE WHEN b.transaction_type = 'Refund' THEN b.row_count ELSE 0 END) AS `metric__a_1113`,
  SUM(CASE WHEN b.transaction_type = 'Refund' THEN b.row_count ELSE 0 END) AS `present__a_1113`,
  SUM(CASE WHEN b.transaction_type = 'Refund' THEN b.amount_sum END) AS `metric__a_1114`,
  SUM(CASE WHEN b.transaction_type = 'Refund' THEN b.row_count ELSE 0 END) AS `present__a_1114`,
  MIN(CASE WHEN b.transaction_type = 'Refund' THEN b.amount_min END) AS `metric__a_1115`,
  SUM(CASE WHEN b.transaction_type = 'Refund' THEN b.row_count ELSE 0 END) AS `present__a_1115`,
  MAX(CASE WHEN b.transaction_type = 'Refund' THEN b.amount_max END) AS `metric__a_1116`,
  SUM(CASE WHEN b.transaction_type = 'Refund' THEN b.row_count ELSE 0 END) AS `present__a_1116`,
  SUM(CASE WHEN b.transaction_type = 'Auth' THEN b.row_count ELSE 0 END) AS `metric__a_1125`,
  SUM(CASE WHEN b.transaction_type = 'Auth' THEN b.row_count ELSE 0 END) AS `present__a_1125`,
  SUM(CASE WHEN b.transaction_type = 'Auth' THEN b.amount_sum END) AS `metric__a_1126`,
  SUM(CASE WHEN b.transaction_type = 'Auth' THEN b.row_count ELSE 0 END) AS `present__a_1126`,
  MIN(CASE WHEN b.transaction_type = 'Auth' THEN b.amount_min END) AS `metric__a_1127`,
  SUM(CASE WHEN b.transaction_type = 'Auth' THEN b.row_count ELSE 0 END) AS `present__a_1127`,
  MAX(CASE WHEN b.transaction_type = 'Auth' THEN b.amount_max END) AS `metric__a_1128`,
  SUM(CASE WHEN b.transaction_type = 'Auth' THEN b.row_count ELSE 0 END) AS `present__a_1128`,
  SUM(CASE WHEN b.transaction_type = 'Capture' THEN b.row_count ELSE 0 END) AS `metric__a_1137`,
  SUM(CASE WHEN b.transaction_type = 'Capture' THEN b.row_count ELSE 0 END) AS `present__a_1137`,
  SUM(CASE WHEN b.transaction_type = 'Capture' THEN b.amount_sum END) AS `metric__a_1138`,
  SUM(CASE WHEN b.transaction_type = 'Capture' THEN b.row_count ELSE 0 END) AS `present__a_1138`,
  MIN(CASE WHEN b.transaction_type = 'Capture' THEN b.amount_min END) AS `metric__a_1139`,
  SUM(CASE WHEN b.transaction_type = 'Capture' THEN b.row_count ELSE 0 END) AS `present__a_1139`,
  MAX(CASE WHEN b.transaction_type = 'Capture' THEN b.amount_max END) AS `metric__a_1140`,
  SUM(CASE WHEN b.transaction_type = 'Capture' THEN b.row_count ELSE 0 END) AS `present__a_1140`,
  SUM(CASE WHEN b.transaction_type = 'Reversal' THEN b.row_count ELSE 0 END) AS `metric__a_1149`,
  SUM(CASE WHEN b.transaction_type = 'Reversal' THEN b.row_count ELSE 0 END) AS `present__a_1149`,
  SUM(CASE WHEN b.transaction_type = 'Reversal' THEN b.amount_sum END) AS `metric__a_1150`,
  SUM(CASE WHEN b.transaction_type = 'Reversal' THEN b.row_count ELSE 0 END) AS `present__a_1150`,
  MIN(CASE WHEN b.transaction_type = 'Reversal' THEN b.amount_min END) AS `metric__a_1151`,
  SUM(CASE WHEN b.transaction_type = 'Reversal' THEN b.row_count ELSE 0 END) AS `present__a_1151`,
  MAX(CASE WHEN b.transaction_type = 'Reversal' THEN b.amount_max END) AS `metric__a_1152`,
  SUM(CASE WHEN b.transaction_type = 'Reversal' THEN b.row_count ELSE 0 END) AS `present__a_1152`,
  SUM(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.row_count ELSE 0 END) AS `metric__a_1161`,
  SUM(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.row_count ELSE 0 END) AS `present__a_1161`,
  SUM(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.amount_sum END) AS `metric__a_1162`,
  SUM(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.row_count ELSE 0 END) AS `present__a_1162`,
  MIN(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.amount_min END) AS `metric__a_1163`,
  SUM(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.row_count ELSE 0 END) AS `present__a_1163`,
  MAX(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.amount_max END) AS `metric__a_1164`,
  SUM(CASE WHEN b.transaction_type = 'CAPTURE_ORDER' THEN b.row_count ELSE 0 END) AS `present__a_1164`,
  SUM(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.row_count ELSE 0 END) AS `metric__a_1173`,
  SUM(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.row_count ELSE 0 END) AS `present__a_1173`,
  SUM(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.amount_sum END) AS `metric__a_1174`,
  SUM(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.row_count ELSE 0 END) AS `present__a_1174`,
  MIN(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.amount_min END) AS `metric__a_1175`,
  SUM(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.row_count ELSE 0 END) AS `present__a_1175`,
  MAX(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.amount_max END) AS `metric__a_1176`,
  SUM(CASE WHEN b.transaction_type = 'EMV_Advice' THEN b.row_count ELSE 0 END) AS `present__a_1176`,
  SUM(CASE WHEN b.transaction_type = 'Adjustment' THEN b.row_count ELSE 0 END) AS `metric__a_1185`,
  SUM(CASE WHEN b.transaction_type = 'Adjustment' THEN b.row_count ELSE 0 END) AS `present__a_1185`,
  SUM(CASE WHEN b.transaction_type = 'Adjustment' THEN b.amount_sum END) AS `metric__a_1186`,
  SUM(CASE WHEN b.transaction_type = 'Adjustment' THEN b.row_count ELSE 0 END) AS `present__a_1186`,
  MIN(CASE WHEN b.transaction_type = 'Adjustment' THEN b.amount_min END) AS `metric__a_1187`,
  SUM(CASE WHEN b.transaction_type = 'Adjustment' THEN b.row_count ELSE 0 END) AS `present__a_1187`,
  MAX(CASE WHEN b.transaction_type = 'Adjustment' THEN b.amount_max END) AS `metric__a_1188`,
  SUM(CASE WHEN b.transaction_type = 'Adjustment' THEN b.row_count ELSE 0 END) AS `present__a_1188`,
  SUM(CASE WHEN b.transaction_type = 'Credit' THEN b.row_count ELSE 0 END) AS `metric__a_1197`,
  SUM(CASE WHEN b.transaction_type = 'Credit' THEN b.row_count ELSE 0 END) AS `present__a_1197`,
  SUM(CASE WHEN b.transaction_type = 'Credit' THEN b.amount_sum END) AS `metric__a_1198`,
  SUM(CASE WHEN b.transaction_type = 'Credit' THEN b.row_count ELSE 0 END) AS `present__a_1198`,
  MIN(CASE WHEN b.transaction_type = 'Credit' THEN b.amount_min END) AS `metric__a_1199`,
  SUM(CASE WHEN b.transaction_type = 'Credit' THEN b.row_count ELSE 0 END) AS `present__a_1199`,
  MAX(CASE WHEN b.transaction_type = 'Credit' THEN b.amount_max END) AS `metric__a_1200`,
  SUM(CASE WHEN b.transaction_type = 'Credit' THEN b.row_count ELSE 0 END) AS `present__a_1200`,
  SUM(CASE WHEN b.transaction_type = 'Debit' THEN b.row_count ELSE 0 END) AS `metric__a_1209`,
  SUM(CASE WHEN b.transaction_type = 'Debit' THEN b.row_count ELSE 0 END) AS `present__a_1209`,
  SUM(CASE WHEN b.transaction_type = 'Debit' THEN b.amount_sum END) AS `metric__a_1210`,
  SUM(CASE WHEN b.transaction_type = 'Debit' THEN b.row_count ELSE 0 END) AS `present__a_1210`,
  MIN(CASE WHEN b.transaction_type = 'Debit' THEN b.amount_min END) AS `metric__a_1211`,
  SUM(CASE WHEN b.transaction_type = 'Debit' THEN b.row_count ELSE 0 END) AS `present__a_1211`,
  MAX(CASE WHEN b.transaction_type = 'Debit' THEN b.amount_max END) AS `metric__a_1212`,
  SUM(CASE WHEN b.transaction_type = 'Debit' THEN b.row_count ELSE 0 END) AS `present__a_1212`,
  SUM(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.row_count ELSE 0 END) AS `metric__a_1221`,
  SUM(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.row_count ELSE 0 END) AS `present__a_1221`,
  SUM(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.amount_sum END) AS `metric__a_1222`,
  SUM(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.row_count ELSE 0 END) AS `present__a_1222`,
  MIN(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.amount_min END) AS `metric__a_1223`,
  SUM(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.row_count ELSE 0 END) AS `present__a_1223`,
  MAX(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.amount_max END) AS `metric__a_1224`,
  SUM(CASE WHEN b.transaction_type = 'AuthOnly' THEN b.row_count ELSE 0 END) AS `present__a_1224`,
  SUM(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.row_count ELSE 0 END) AS `metric__a_1233`,
  SUM(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.row_count ELSE 0 END) AS `present__a_1233`,
  SUM(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.amount_sum END) AS `metric__a_1234`,
  SUM(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.row_count ELSE 0 END) AS `present__a_1234`,
  MIN(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.amount_min END) AS `metric__a_1235`,
  SUM(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.row_count ELSE 0 END) AS `present__a_1235`,
  MAX(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.amount_max END) AS `metric__a_1236`,
  SUM(CASE WHEN b.transaction_type = 'CaptureOnly' THEN b.row_count ELSE 0 END) AS `present__a_1236`,
  SUM(CASE WHEN b.transaction_type = 'PostAuth' THEN b.row_count ELSE 0 END) AS `metric__a_1245`,
  SUM(CASE WHEN b.transaction_type = 'PostAuth' THEN b.row_count ELSE 0 END) AS `present__a_1245`,
  SUM(CASE WHEN b.transaction_type = 'PostAuth' THEN b.amount_sum END) AS `metric__a_1246`,
  SUM(CASE WHEN b.transaction_type = 'PostAuth' THEN b.row_count ELSE 0 END) AS `present__a_1246`,
  MIN(CASE WHEN b.transaction_type = 'PostAuth' THEN b.amount_min END) AS `metric__a_1247`,
  SUM(CASE WHEN b.transaction_type = 'PostAuth' THEN b.row_count ELSE 0 END) AS `present__a_1247`,
  MAX(CASE WHEN b.transaction_type = 'PostAuth' THEN b.amount_max END) AS `metric__a_1248`,
  SUM(CASE WHEN b.transaction_type = 'PostAuth' THEN b.row_count ELSE 0 END) AS `present__a_1248`,
  SUM(CASE WHEN b.transaction_type = 'PreAuth' THEN b.row_count ELSE 0 END) AS `metric__a_1257`,
  SUM(CASE WHEN b.transaction_type = 'PreAuth' THEN b.row_count ELSE 0 END) AS `present__a_1257`,
  SUM(CASE WHEN b.transaction_type = 'PreAuth' THEN b.amount_sum END) AS `metric__a_1258`,
  SUM(CASE WHEN b.transaction_type = 'PreAuth' THEN b.row_count ELSE 0 END) AS `present__a_1258`,
  MIN(CASE WHEN b.transaction_type = 'PreAuth' THEN b.amount_min END) AS `metric__a_1259`,
  SUM(CASE WHEN b.transaction_type = 'PreAuth' THEN b.row_count ELSE 0 END) AS `present__a_1259`,
  MAX(CASE WHEN b.transaction_type = 'PreAuth' THEN b.amount_max END) AS `metric__a_1260`,
  SUM(CASE WHEN b.transaction_type = 'PreAuth' THEN b.row_count ELSE 0 END) AS `present__a_1260`,
  SUM(CASE WHEN b.transaction_type = 'Return' THEN b.row_count ELSE 0 END) AS `metric__a_1269`,
  SUM(CASE WHEN b.transaction_type = 'Return' THEN b.row_count ELSE 0 END) AS `present__a_1269`,
  SUM(CASE WHEN b.transaction_type = 'Return' THEN b.amount_sum END) AS `metric__a_1270`,
  SUM(CASE WHEN b.transaction_type = 'Return' THEN b.row_count ELSE 0 END) AS `present__a_1270`,
  MIN(CASE WHEN b.transaction_type = 'Return' THEN b.amount_min END) AS `metric__a_1271`,
  SUM(CASE WHEN b.transaction_type = 'Return' THEN b.row_count ELSE 0 END) AS `present__a_1271`,
  MAX(CASE WHEN b.transaction_type = 'Return' THEN b.amount_max END) AS `metric__a_1272`,
  SUM(CASE WHEN b.transaction_type = 'Return' THEN b.row_count ELSE 0 END) AS `present__a_1272`,
  SUM(CASE WHEN b.transaction_type = 'Chargeback' THEN b.row_count ELSE 0 END) AS `metric__a_1281`,
  SUM(CASE WHEN b.transaction_type = 'Chargeback' THEN b.row_count ELSE 0 END) AS `present__a_1281`,
  SUM(CASE WHEN b.transaction_type = 'Chargeback' THEN b.amount_sum END) AS `metric__a_1282`,
  SUM(CASE WHEN b.transaction_type = 'Chargeback' THEN b.row_count ELSE 0 END) AS `present__a_1282`,
  MIN(CASE WHEN b.transaction_type = 'Chargeback' THEN b.amount_min END) AS `metric__a_1283`,
  SUM(CASE WHEN b.transaction_type = 'Chargeback' THEN b.row_count ELSE 0 END) AS `present__a_1283`,
  MAX(CASE WHEN b.transaction_type = 'Chargeback' THEN b.amount_max END) AS `metric__a_1284`,
  SUM(CASE WHEN b.transaction_type = 'Chargeback' THEN b.row_count ELSE 0 END) AS `present__a_1284`,
  SUM(CASE WHEN b.transaction_type = 'Dispute' THEN b.row_count ELSE 0 END) AS `metric__a_1293`,
  SUM(CASE WHEN b.transaction_type = 'Dispute' THEN b.row_count ELSE 0 END) AS `present__a_1293`,
  SUM(CASE WHEN b.transaction_type = 'Dispute' THEN b.amount_sum END) AS `metric__a_1294`,
  SUM(CASE WHEN b.transaction_type = 'Dispute' THEN b.row_count ELSE 0 END) AS `present__a_1294`,
  MIN(CASE WHEN b.transaction_type = 'Dispute' THEN b.amount_min END) AS `metric__a_1295`,
  SUM(CASE WHEN b.transaction_type = 'Dispute' THEN b.row_count ELSE 0 END) AS `present__a_1295`,
  MAX(CASE WHEN b.transaction_type = 'Dispute' THEN b.amount_max END) AS `metric__a_1296`,
  SUM(CASE WHEN b.transaction_type = 'Dispute' THEN b.row_count ELSE 0 END) AS `present__a_1296`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.row_count ELSE 0 END) AS `metric__a_1305`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.row_count ELSE 0 END) AS `present__a_1305`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.amount_sum END) AS `metric__a_1306`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.row_count ELSE 0 END) AS `present__a_1306`,
  MIN(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.amount_min END) AS `metric__a_1307`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.row_count ELSE 0 END) AS `present__a_1307`,
  MAX(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.amount_max END) AS `metric__a_1308`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_NSF' THEN b.row_count ELSE 0 END) AS `present__a_1308`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.row_count ELSE 0 END) AS `metric__a_1317`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.row_count ELSE 0 END) AS `present__a_1317`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.amount_sum END) AS `metric__a_1318`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.row_count ELSE 0 END) AS `present__a_1318`,
  MIN(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.amount_min END) AS `metric__a_1319`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.row_count ELSE 0 END) AS `present__a_1319`,
  MAX(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.amount_max END) AS `metric__a_1320`,
  SUM(CASE WHEN b.transaction_type = 'Reversal_Timeout' THEN b.row_count ELSE 0 END) AS `present__a_1320`,
  SUM(CASE WHEN b.card_type = 'VISA' THEN b.row_count ELSE 0 END) AS `metric__a_1329`,
  SUM(CASE WHEN b.card_type = 'VISA' THEN b.row_count ELSE 0 END) AS `present__a_1329`,
  SUM(CASE WHEN b.card_type = 'VISA' THEN b.amount_sum END) AS `metric__a_1330`,
  SUM(CASE WHEN b.card_type = 'VISA' THEN b.row_count ELSE 0 END) AS `present__a_1330`,
  MIN(CASE WHEN b.card_type = 'VISA' THEN b.amount_min END) AS `metric__a_1331`,
  SUM(CASE WHEN b.card_type = 'VISA' THEN b.row_count ELSE 0 END) AS `present__a_1331`,
  MAX(CASE WHEN b.card_type = 'VISA' THEN b.amount_max END) AS `metric__a_1332`,
  SUM(CASE WHEN b.card_type = 'VISA' THEN b.row_count ELSE 0 END) AS `present__a_1332`,
  SUM(CASE WHEN b.card_type = 'MASTERCARD' THEN b.row_count ELSE 0 END) AS `metric__a_1341`,
  SUM(CASE WHEN b.card_type = 'MASTERCARD' THEN b.row_count ELSE 0 END) AS `present__a_1341`,
  SUM(CASE WHEN b.card_type = 'MASTERCARD' THEN b.amount_sum END) AS `metric__a_1342`,
  SUM(CASE WHEN b.card_type = 'MASTERCARD' THEN b.row_count ELSE 0 END) AS `present__a_1342`,
  MIN(CASE WHEN b.card_type = 'MASTERCARD' THEN b.amount_min END) AS `metric__a_1343`,
  SUM(CASE WHEN b.card_type = 'MASTERCARD' THEN b.row_count ELSE 0 END) AS `present__a_1343`,
  MAX(CASE WHEN b.card_type = 'MASTERCARD' THEN b.amount_max END) AS `metric__a_1344`,
  SUM(CASE WHEN b.card_type = 'MASTERCARD' THEN b.row_count ELSE 0 END) AS `present__a_1344`,
  SUM(CASE WHEN b.card_type = 'CHECK' THEN b.row_count ELSE 0 END) AS `metric__a_1353`,
  SUM(CASE WHEN b.card_type = 'CHECK' THEN b.row_count ELSE 0 END) AS `present__a_1353`,
  SUM(CASE WHEN b.card_type = 'CHECK' THEN b.amount_sum END) AS `metric__a_1354`,
  SUM(CASE WHEN b.card_type = 'CHECK' THEN b.row_count ELSE 0 END) AS `present__a_1354`,
  MIN(CASE WHEN b.card_type = 'CHECK' THEN b.amount_min END) AS `metric__a_1355`,
  SUM(CASE WHEN b.card_type = 'CHECK' THEN b.row_count ELSE 0 END) AS `present__a_1355`,
  MAX(CASE WHEN b.card_type = 'CHECK' THEN b.amount_max END) AS `metric__a_1356`,
  SUM(CASE WHEN b.card_type = 'CHECK' THEN b.row_count ELSE 0 END) AS `present__a_1356`,
  SUM(CASE WHEN b.card_type = 'AMEX' THEN b.row_count ELSE 0 END) AS `metric__a_1365`,
  SUM(CASE WHEN b.card_type = 'AMEX' THEN b.row_count ELSE 0 END) AS `present__a_1365`,
  SUM(CASE WHEN b.card_type = 'AMEX' THEN b.amount_sum END) AS `metric__a_1366`,
  SUM(CASE WHEN b.card_type = 'AMEX' THEN b.row_count ELSE 0 END) AS `present__a_1366`,
  MIN(CASE WHEN b.card_type = 'AMEX' THEN b.amount_min END) AS `metric__a_1367`,
  SUM(CASE WHEN b.card_type = 'AMEX' THEN b.row_count ELSE 0 END) AS `present__a_1367`,
  MAX(CASE WHEN b.card_type = 'AMEX' THEN b.amount_max END) AS `metric__a_1368`,
  SUM(CASE WHEN b.card_type = 'AMEX' THEN b.row_count ELSE 0 END) AS `present__a_1368`,
  SUM(CASE WHEN b.card_type = 'DISCOVER' THEN b.row_count ELSE 0 END) AS `metric__a_1377`,
  SUM(CASE WHEN b.card_type = 'DISCOVER' THEN b.row_count ELSE 0 END) AS `present__a_1377`,
  SUM(CASE WHEN b.card_type = 'DISCOVER' THEN b.amount_sum END) AS `metric__a_1378`,
  SUM(CASE WHEN b.card_type = 'DISCOVER' THEN b.row_count ELSE 0 END) AS `present__a_1378`,
  MIN(CASE WHEN b.card_type = 'DISCOVER' THEN b.amount_min END) AS `metric__a_1379`,
  SUM(CASE WHEN b.card_type = 'DISCOVER' THEN b.row_count ELSE 0 END) AS `present__a_1379`,
  MAX(CASE WHEN b.card_type = 'DISCOVER' THEN b.amount_max END) AS `metric__a_1380`,
  SUM(CASE WHEN b.card_type = 'DISCOVER' THEN b.row_count ELSE 0 END) AS `present__a_1380`,
  SUM(CASE WHEN b.card_type = 'DINERS' THEN b.row_count ELSE 0 END) AS `metric__a_1389`,
  SUM(CASE WHEN b.card_type = 'DINERS' THEN b.row_count ELSE 0 END) AS `present__a_1389`,
  SUM(CASE WHEN b.card_type = 'DINERS' THEN b.amount_sum END) AS `metric__a_1390`,
  SUM(CASE WHEN b.card_type = 'DINERS' THEN b.row_count ELSE 0 END) AS `present__a_1390`,
  MIN(CASE WHEN b.card_type = 'DINERS' THEN b.amount_min END) AS `metric__a_1391`,
  SUM(CASE WHEN b.card_type = 'DINERS' THEN b.row_count ELSE 0 END) AS `present__a_1391`,
  MAX(CASE WHEN b.card_type = 'DINERS' THEN b.amount_max END) AS `metric__a_1392`,
  SUM(CASE WHEN b.card_type = 'DINERS' THEN b.row_count ELSE 0 END) AS `present__a_1392`,
  SUM(CASE WHEN b.card_type = 'JCB' THEN b.row_count ELSE 0 END) AS `metric__a_1401`,
  SUM(CASE WHEN b.card_type = 'JCB' THEN b.row_count ELSE 0 END) AS `present__a_1401`,
  SUM(CASE WHEN b.card_type = 'JCB' THEN b.amount_sum END) AS `metric__a_1402`,
  SUM(CASE WHEN b.card_type = 'JCB' THEN b.row_count ELSE 0 END) AS `present__a_1402`,
  MIN(CASE WHEN b.card_type = 'JCB' THEN b.amount_min END) AS `metric__a_1403`,
  SUM(CASE WHEN b.card_type = 'JCB' THEN b.row_count ELSE 0 END) AS `present__a_1403`,
  MAX(CASE WHEN b.card_type = 'JCB' THEN b.amount_max END) AS `metric__a_1404`,
  SUM(CASE WHEN b.card_type = 'JCB' THEN b.row_count ELSE 0 END) AS `present__a_1404`,
  SUM(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.row_count ELSE 0 END) AS `metric__a_1413`,
  SUM(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.row_count ELSE 0 END) AS `present__a_1413`,
  SUM(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.amount_sum END) AS `metric__a_1414`,
  SUM(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.row_count ELSE 0 END) AS `present__a_1414`,
  MIN(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.amount_min END) AS `metric__a_1415`,
  SUM(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.row_count ELSE 0 END) AS `present__a_1415`,
  MAX(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.amount_max END) AS `metric__a_1416`,
  SUM(CASE WHEN b.card_type = 'CARTE_BLANCHE' THEN b.row_count ELSE 0 END) AS `present__a_1416`,
  SUM(CASE WHEN b.card_type = 'UNKNOWN' THEN b.row_count ELSE 0 END) AS `metric__a_1425`,
  SUM(CASE WHEN b.card_type = 'UNKNOWN' THEN b.row_count ELSE 0 END) AS `present__a_1425`,
  SUM(CASE WHEN b.card_type = 'UNKNOWN' THEN b.amount_sum END) AS `metric__a_1426`,
  SUM(CASE WHEN b.card_type = 'UNKNOWN' THEN b.row_count ELSE 0 END) AS `present__a_1426`,
  MIN(CASE WHEN b.card_type = 'UNKNOWN' THEN b.amount_min END) AS `metric__a_1427`,
  SUM(CASE WHEN b.card_type = 'UNKNOWN' THEN b.row_count ELSE 0 END) AS `present__a_1427`,
  MAX(CASE WHEN b.card_type = 'UNKNOWN' THEN b.amount_max END) AS `metric__a_1428`,
  SUM(CASE WHEN b.card_type = 'UNKNOWN' THEN b.row_count ELSE 0 END) AS `present__a_1428`,
  SUM(CASE WHEN b.card_type = 'DEBIT' THEN b.row_count ELSE 0 END) AS `metric__a_1437`,
  SUM(CASE WHEN b.card_type = 'DEBIT' THEN b.row_count ELSE 0 END) AS `present__a_1437`,
  SUM(CASE WHEN b.card_type = 'DEBIT' THEN b.amount_sum END) AS `metric__a_1438`,
  SUM(CASE WHEN b.card_type = 'DEBIT' THEN b.row_count ELSE 0 END) AS `present__a_1438`,
  MIN(CASE WHEN b.card_type = 'DEBIT' THEN b.amount_min END) AS `metric__a_1439`,
  SUM(CASE WHEN b.card_type = 'DEBIT' THEN b.row_count ELSE 0 END) AS `present__a_1439`,
  MAX(CASE WHEN b.card_type = 'DEBIT' THEN b.amount_max END) AS `metric__a_1440`,
  SUM(CASE WHEN b.card_type = 'DEBIT' THEN b.row_count ELSE 0 END) AS `present__a_1440`,
  SUM(CASE WHEN b.card_type = 'CREDIT' THEN b.row_count ELSE 0 END) AS `metric__a_1449`,
  SUM(CASE WHEN b.card_type = 'CREDIT' THEN b.row_count ELSE 0 END) AS `present__a_1449`,
  SUM(CASE WHEN b.card_type = 'CREDIT' THEN b.amount_sum END) AS `metric__a_1450`,
  SUM(CASE WHEN b.card_type = 'CREDIT' THEN b.row_count ELSE 0 END) AS `present__a_1450`,
  MIN(CASE WHEN b.card_type = 'CREDIT' THEN b.amount_min END) AS `metric__a_1451`,
  SUM(CASE WHEN b.card_type = 'CREDIT' THEN b.row_count ELSE 0 END) AS `present__a_1451`,
  MAX(CASE WHEN b.card_type = 'CREDIT' THEN b.amount_max END) AS `metric__a_1452`,
  SUM(CASE WHEN b.card_type = 'CREDIT' THEN b.row_count ELSE 0 END) AS `present__a_1452`,
  SUM(CASE WHEN b.card_type = 'PREPAID' THEN b.row_count ELSE 0 END) AS `metric__a_1461`,
  SUM(CASE WHEN b.card_type = 'PREPAID' THEN b.row_count ELSE 0 END) AS `present__a_1461`,
  SUM(CASE WHEN b.card_type = 'PREPAID' THEN b.amount_sum END) AS `metric__a_1462`,
  SUM(CASE WHEN b.card_type = 'PREPAID' THEN b.row_count ELSE 0 END) AS `present__a_1462`,
  MIN(CASE WHEN b.card_type = 'PREPAID' THEN b.amount_min END) AS `metric__a_1463`,
  SUM(CASE WHEN b.card_type = 'PREPAID' THEN b.row_count ELSE 0 END) AS `present__a_1463`,
  MAX(CASE WHEN b.card_type = 'PREPAID' THEN b.amount_max END) AS `metric__a_1464`,
  SUM(CASE WHEN b.card_type = 'PREPAID' THEN b.row_count ELSE 0 END) AS `present__a_1464`,
  SUM(CASE WHEN b.card_type = 'GIFT' THEN b.row_count ELSE 0 END) AS `metric__a_1473`,
  SUM(CASE WHEN b.card_type = 'GIFT' THEN b.row_count ELSE 0 END) AS `present__a_1473`,
  SUM(CASE WHEN b.card_type = 'GIFT' THEN b.amount_sum END) AS `metric__a_1474`,
  SUM(CASE WHEN b.card_type = 'GIFT' THEN b.row_count ELSE 0 END) AS `present__a_1474`,
  MIN(CASE WHEN b.card_type = 'GIFT' THEN b.amount_min END) AS `metric__a_1475`,
  SUM(CASE WHEN b.card_type = 'GIFT' THEN b.row_count ELSE 0 END) AS `present__a_1475`,
  MAX(CASE WHEN b.card_type = 'GIFT' THEN b.amount_max END) AS `metric__a_1476`,
  SUM(CASE WHEN b.card_type = 'GIFT' THEN b.row_count ELSE 0 END) AS `present__a_1476`,
  COUNT(DISTINCT b.card_type) AS `metric__a_1891`,
  COUNT(DISTINCT b.entry_method) AS `metric__a_1892`,
  COUNT(DISTINCT b.mt_gateway) AS `metric__a_1893`
FROM (
  SELECT p.mt_gateway, p.transaction_type, p.card_type, p.entry_method, COUNT(*) AS row_count, SUM(p.amount) AS amount_sum, MIN(p.amount) AS amount_min, MAX(p.amount) AS amount_max
  FROM pmt_txn_fact p
  WHERE p.check_bank_routing_number = %s AND p.event_date >= 1773275781546
  GROUP BY p.mt_gateway, p.transaction_type, p.card_type, p.entry_method
) b
HAVING SUM(b.row_count) > 0;
```

#### Best Candidate EXPLAIN ANALYZE

```text
-- variant=optimized_hashagg_16_8
-- explain_analyze_elapsed_ms=115.1
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_9	0.80	1	root		time:71ms, open:541µs, close:13.9µs, loops:2, RU:366.24, Concurrency:OFF	Column#51->Column#218, Column#52->Column#219, Column#53->Column#220, Column#54->Column#221, Column#55->Column#222, Column#55->Column#223, Column#56->Column#224, Column#55->Column#225, Column#57->Column#226, Column#55->Column#227, Column#58->Column#228, Column#55->Column#229, Column#59->Column#230, Column#59->Column#231, Column#60->Column#232, Column#59->Column#233, Column#61->Column#234, Column#59->Column#235, Column#62->Column#236, Column#59->Column#237, Column#63->Column#238, Column#63->Column#239, Column#64->Column#240, Column#63->Column#241, Column#65->Column#242, Column#63->Column#243, Column#66->Column#244, Column#63->Column#245, Column#67->Column#246, Column#67->Column#247, Column#68->Column#248, Column#67->Column#249, Column#69->Column#250, Column#67->Column#251, Column#70->Column#252, Column#67->Column#253, Column#71->Column#254, Column#71->Column#255, Column#72->Column#256, Column#71->Column#257, Column#73->Column#258, Column#71->Column#259, Column#74->Column#260, Column#71->Column#261, Column#75->Column#262, Column#75->Column#263, Column#76->Column#264, Column#75->Column#265, Column#77->Column#266, Column#75->Column#267, Column#78->Column#268, Column#75->Column#269, Column#79->Column#270, Column#79->Column#271, Column#80->Column#272, Column#79->Column#273, Column#81->Column#274, Column#79->Column#275, Column#82->Column#276, Column#79->Column#277, Column#83->Column#278, Column#83->Column#279, Column#84->Column#280, Column#83->Column#281, Column#85->Column#282, Column#83->Column#283, Column#86->Column#284, Column#83->Column#285, Column#87->Column#286, Column#87->Column#287, Column#88->Column#288, Column#87->Column#289, Column#89->Column#290, Column#87->Column#291, Column#90->Column#292, Column#87->Column#293, Column#91->Column#294, Column#91->Column#295, Column#92->Column#296, Column#91->Column#297, Column#93->Column#298, Column#91->Column#299, Column#94->Column#300, Column#91->Column#301, Column#95->Column#302, Column#95->Column#303, Column#96->Column#304, Column#95->Column#305, Column#97->Column#306, Column#95->Column#307, Column#98->Column#308, Column#95->Column#309, Column#99->Column#310, Column#99->Column#311, Column#100->Column#312, Column#99->Column#313, Column#101->Column#314, Column#99->Column#315, Column#102->Column#316, Column#99->Column#317, Column#103->Column#318, Column#103->Column#319, Column#104->Column#320, Column#103->Column#321, Column#105->Column#322, Column#103->Column#323, Column#106->Column#324, Column#103->Column#325, Column#107->Column#326, Column#107->Column#327, Column#108->Column#328, Column#107->Column#329, Column#109->Column#330, Column#107->Column#331, Column#110->Column#332, Column#107->Column#333, Column#111->Column#334, Column#111->Column#335, Column#112->Column#336, Column#111->Column#337, Column#113->Column#338, Column#111->Column#339, Column#114->Column#340, Column#111->Column#341, Column#115->Column#342, Column#115->Column#343, Column#116->Column#344, Column#115->Column#345, Column#117->Column#346, Column#115->Column#347, Column#118->Column#348, Column#115->Column#349, Column#119->Column#350, Column#119->Column#351, Column#120->Column#352, Column#119->Column#353, Column#121->Column#354, Column#119->Column#355, Column#122->Column#356, Column#119->Column#357, Column#123->Column#358, Column#123->Column#359, Column#124->Column#360, Column#123->Column#361, Column#125->Column#362, Column#123->Column#363, Column#126->Column#364, Column#123->Column#365, Column#127->Column#366, Column#127->Column#367, Column#128->Column#368, Column#127->Column#369, Column#129->Column#370, Column#127->Column#371, Column#130->Column#372, Column#127->Column#373, Column#131->Column#374, Column#131->Column#375, Column#132->Column#376, Column#131->Column#377, Column#133->Column#378, Column#131->Column#379, Column#134->Column#380, Column#131->Column#381, Column#135->Column#382, Column#135->Column#383, Column#136->Column#384, Column#135->Column#385, Column#137->Column#386, Column#135->Column#387, Column#138->Column#388, Column#135->Column#389, Column#139->Column#390, Column#139->Column#391, Column#140->Column#392, Column#139->Column#393, Column#141->Column#394, Column#139->Column#395, Column#142->Column#396, Column#139->Column#397, Column#143->Column#398, Column#143->Column#399, Column#144->Column#400, Column#143->Column#401, Column#145->Column#402, Column#143->Column#403, Column#146->Column#404, Column#143->Column#405, Column#147->Column#406, Column#147->Column#407, Column#148->Column#408, Column#147->Column#409, Column#149->Column#410, Column#147->Column#411, Column#150->Column#412, Column#147->Column#413, Column#151->Column#414, Column#151->Column#415, Column#152->Column#416, Column#151->Column#417, Column#153->Column#418, Column#151->Column#419, Column#154->Column#420, Column#151->Column#421, Column#155->Column#422, Column#155->Column#423, Column#156->Column#424, Column#155->Column#425, Column#157->Column#426, Column#155->Column#427, Column#158->Column#428, Column#155->Column#429, Column#159->Column#430, Column#159->Column#431, Column#160->Column#432, Column#159->Column#433, Column#161->Column#434, Column#159->Column#435, Column#162->Column#436, Column#159->Column#437, Column#163->Column#438, Column#163->Column#439, Column#164->Column#440, Column#163->Column#441, Column#165->Column#442, Column#163->Column#443, Column#166->Column#444, Column#163->Column#445, Column#167->Column#446, Column#167->Column#447, Column#168->Column#448, Column#167->Column#449, Column#169->Column#450, Column#167->Column#451, Column#170->Column#452, Column#167->Column#453, Column#171->Column#454, Column#171->Column#455, Column#172->Column#456, Column#171->Column#457, Column#173->Column#458, Column#171->Column#459, Column#174->Column#460, Column#171->Column#461, Column#175->Column#462, Column#175->Column#463, Column#176->Column#464, Column#175->Column#465, Column#177->Column#466, Column#175->Column#467, Column#178->Column#468, Column#175->Column#469, Column#179->Column#470, Column#179->Column#471, Column#180->Column#472, Column#179->Column#473, Column#181->Column#474, Column#179->Column#475, Column#182->Column#476, Column#179->Column#477, Column#183->Column#478, Column#183->Column#479, Column#184->Column#480, Column#183->Column#481, Column#185->Column#482, Column#183->Column#483, Column#186->Column#484, Column#183->Column#485, Column#187->Column#486, Column#187->Column#487, Column#188->Column#488, Column#187->Column#489, Column#189->Column#490, Column#187->Column#491, Column#190->Column#492, Column#187->Column#493, Column#191->Column#494, Column#191->Column#495, Column#192->Column#496, Column#191->Column#497, Column#193->Column#498, Column#191->Column#499, Column#194->Column#500, Column#191->Column#501, Column#195->Column#502, Column#195->Column#503, Column#196->Column#504, Column#195->Column#505, Column#197->Column#506, Column#195->Column#507, Column#198->Column#508, Column#195->Column#509, Column#199->Column#510, Column#199->Column#511, Column#200->Column#512, Column#199->Column#513, Column#201->Column#514, Column#199->Column#515, Column#202->Column#516, Column#199->Column#517, Column#203->Column#518, Column#203->Column#519, Column#204->Column#520, Column#203->Column#521, Column#205->Column#522, Column#203->Column#523, Column#206->Column#524, Column#203->Column#525, Column#207->Column#526, Column#207->Column#527, Column#208->Column#528, Column#207->Column#529, Column#209->Column#530, Column#207->Column#531, Column#210->Column#532, Column#207->Column#533, Column#211->Column#534, Column#211->Column#535, Column#212->Column#536, Column#211->Column#537, Column#213->Column#538, Column#211->Column#539, Column#214->Column#540, Column#211->Column#541, Column#215->Column#542, Column#216->Column#543, Column#217->Column#544	231.1 KB	N/A
└─Selection_11	0.80	1	root		time:70.6ms, open:438.9µs, close:12.3µs, loops:2	gt(Column#51, ?)	231.1 KB	N/A
  └─HashAgg_15	1.00	1	root		time:70.5ms, open:319.3µs, close:11.6µs, loops:3	funcs:sum(Column#553)->Column#51, funcs:sum(Column#554)->Column#52, funcs:min(Column#555)->Column#53, funcs:max(Column#556)->Column#54, funcs:sum(Column#557)->Column#55, funcs:sum(Column#558)->Column#56, funcs:min(Column#559)->Column#57, funcs:max(Column#560)->Column#58, funcs:sum(Column#561)->Column#59, funcs:sum(Column#562)->Column#60, funcs:min(Column#563)->Column#61, funcs:max(Column#564)->Column#62, funcs:sum(Column#565)->Column#63, funcs:sum(Column#566)->Column#64, funcs:min(Column#567)->Column#65, funcs:max(Column#568)->Column#66, funcs:sum(Column#569)->Column#67, funcs:sum(Column#570)->Column#68, funcs:min(Column#571)->Column#69, funcs:max(Column#572)->Column#70, funcs:sum(Column#573)->Column#71, funcs:sum(Column#574)->Column#72, funcs:min(Column#575)->Column#73, funcs:max(Column#576)->Column#74, funcs:sum(Column#577)->Column#75, funcs:sum(Column#578)->Column#76, funcs:min(Column#579)->Column#77, funcs:max(Column#580)->Column#78, funcs:sum(Column#581)->Column#79, funcs:sum(Column#582)->Column#80, funcs:min(Column#583)->Column#81, funcs:max(Column#584)->Column#82, funcs:sum(Column#585)->Column#83, funcs:sum(Column#586)->Column#84, funcs:min(Column#587)->Column#85, funcs:max(Column#588)->Column#86, funcs:sum(Column#589)->Column#87, funcs:sum(Column#590)->Column#88, funcs:min(Column#591)->Column#89, funcs:max(Column#592)->Column#90, funcs:sum(Column#593)->Column#91, funcs:sum(Column#594)->Column#92, funcs:min(Column#595)->Column#93, funcs:max(Column#596)->Column#94, funcs:sum(Column#597)->Column#95, funcs:sum(Column#598)->Column#96, funcs:min(Column#599)->Column#97, funcs:max(Column#600)->Column#98, funcs:sum(Column#601)->Column#99, funcs:sum(Column#602)->Column#100, funcs:min(Column#603)->Column#101, funcs:max(Column#604)->Column#102, funcs:sum(Column#605)->Column#103, funcs:sum(Column#606)->Column#104, funcs:min(Column#607)->Column#105, funcs:max(Column#608)->Column#106, funcs:sum(Column#609)->Column#107, funcs:sum(Column#610)->Column#108, funcs:min(Column#611)->Column#109, funcs:max(Column#612)->Column#110, funcs:sum(Column#613)->Column#111, funcs:sum(Column#614)->Column#112, funcs:min(Column#615)->Column#113, funcs:max(Column#616)->Column#114, funcs:sum(Column#617)->Column#115, funcs:sum(Column#618)->Column#116, funcs:min(Column#619)->Column#117, funcs:max(Column#620)->Column#118, funcs:sum(Column#621)->Column#119, funcs:sum(Column#622)->Column#120, funcs:min(Column#623)->Column#121, funcs:max(Column#624)->Column#122, funcs:sum(Column#625)->Column#123, funcs:sum(Column#626)->Column#124, funcs:min(Column#627)->Column#125, funcs:max(Column#628)->Column#126, funcs:sum(Column#629)->Column#127, funcs:sum(Column#630)->Column#128, funcs:min(Column#631)->Column#129, funcs:max(Column#632)->Column#130, funcs:sum(Column#633)->Column#131, funcs:sum(Column#634)->Column#132, funcs:min(Column#635)->Column#133, funcs:max(Column#636)->Column#134, funcs:sum(Column#637)->Column#135, funcs:sum(Column#638)->Column#136, funcs:min(Column#639)->Column#137, funcs:max(Column#640)->Column#138, funcs:sum(Column#641)->Column#139, funcs:sum(Column#642)->Column#140, funcs:min(Column#643)->Column#141, funcs:max(Column#644)->Column#142, funcs:sum(Column#645)->Column#143, funcs:sum(Column#646)->Column#144, funcs:min(Column#647)->Column#145, funcs:max(Column#648)->Column#146, funcs:sum(Column#649)->Column#147, funcs:sum(Column#650)->Column#148, funcs:min(Column#651)->Column#149, funcs:max(Column#652)->Column#150, funcs:sum(Column#653)->Column#151, funcs:sum(Column#654)->Column#152, funcs:min(Column#655)->Column#153, funcs:max(Column#656)->Column#154, funcs:sum(Column#657)->Column#155, funcs:sum(Column#658)->Column#156, funcs:min(Column#659)->Column#157, funcs:max(Column#660)->Column#158, funcs:sum(Column#661)->Column#159, funcs:sum(Column#662)->Column#160, funcs:min(Column#663)->Column#161, funcs:max(Column#664)->Column#162, funcs:sum(Column#665)->Column#163, funcs:sum(Column#666)->Column#164, funcs:min(Column#667)->Column#165, funcs:max(Column#668)->Column#166, funcs:sum(Column#669)->Column#167, funcs:sum(Column#670)->Column#168, funcs:min(Column#671)->Column#169, funcs:max(Column#672)->Column#170, funcs:sum(Column#673)->Column#171, funcs:sum(Column#674)->Column#172, funcs:min(Column#675)->Column#173, funcs:max(Column#676)->Column#174, funcs:sum(Column#677)->Column#175, funcs:sum(Column#678)->Column#176, funcs:min(Column#679)->Column#177, funcs:max(Column#680)->Column#178, funcs:sum(Column#681)->Column#179, funcs:sum(Column#682)->Column#180, funcs:min(Column#683)->Column#181, funcs:max(Column#684)->Column#182, funcs:sum(Column#685)->Column#183, funcs:sum(Column#686)->Column#184, funcs:min(Column#687)->Column#185, funcs:max(Column#688)->Column#186, funcs:sum(Column#689)->Column#187, funcs:sum(Column#690)->Column#188, funcs:min(Column#691)->Column#189, funcs:max(Column#692)->Column#190, funcs:sum(Column#693)->Column#191, funcs:sum(Column#694)->Column#192, funcs:min(Column#695)->Column#193, funcs:max(Column#696)->Column#194, funcs:sum(Column#697)->Column#195, funcs:sum(Column#698)->Column#196, funcs:min(Column#699)->Column#197, funcs:max(Column#700)->Column#198, funcs:sum(Column#701)->Column#199, funcs:sum(Column#702)->Column#200, funcs:min(Column#703)->Column#201, funcs:max(Column#704)->Column#202, funcs:sum(Column#705)->Column#203, funcs:sum(Column#706)->Column#204, funcs:min(Column#707)->Column#205, funcs:max(Column#708)->Column#206, funcs:sum(Column#709)->Column#207, funcs:sum(Column#710)->Column#208, funcs:min(Column#711)->Column#209, funcs:max(Column#712)->Column#210, funcs:sum(Column#713)->Column#211, funcs:sum(Column#714)->Column#212, funcs:min(Column#715)->Column#213, funcs:max(Column#716)->Column#214, funcs:count(distinct Column#717)->Column#215, funcs:count(distinct Column#718)->Column#216, funcs:count(distinct Column#719)->Column#217	239.7 KB	0 Bytes
    └─Projection_34	1.00	149	root		time:69.8ms, open:148.6µs, close:11µs, loops:17, Concurrency:OFF	cast(Column#47, decimal(20,0) BINARY)->Column#553, Column#48->Column#554, Column#49->Column#555, Column#50->Column#556, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#557, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#558, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#559, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#560, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#561, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#562, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#563, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#564, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#565, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#566, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#567, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#568, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#569, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#570, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#571, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#572, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#573, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#574, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#575, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#576, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#577, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#578, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#579, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#580, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#581, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#582, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#583, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#584, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#585, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#586, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#587, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#588, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#589, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#590, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#591, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#592, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#593, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#594, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#595, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#596, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#597, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#598, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#599, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#600, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#601, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#602, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#603, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#604, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#605, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#606, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#607, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#608, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#609, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#610, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#611, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#612, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#613, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#614, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#615, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#616, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#617, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#618, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#619, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#620, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#621, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#622, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#623, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#624, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#625, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#626, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#627, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#628, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#629, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#630, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#631, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#632, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#633, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#634, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#635, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#636, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#637, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#638, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#639, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#640, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#641, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#642, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#643, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#644, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#645, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#646, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#647, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#648, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#649, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#650, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#651, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#652, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#653, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#654, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#655, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#656, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#657, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#658, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#659, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#660, cast(case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#661, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#48)->Column#662, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#49)->Column#663, case(eq(intuit_risk.pmt_txn_fact.transaction_type, ?), Column#50)->Column#664, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#665, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#666, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#667, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#668, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#669, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#670, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#671, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#672, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#673, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#674, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#675, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#676, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#677, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#678, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#679, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#680, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#681, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#682, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#683, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#684, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#685, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#686, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#687, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#688, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#689, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#690, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#691, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#692, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#693, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#694, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#695, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#696, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#697, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#698, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#699, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#700, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#701, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#702, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#703, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#704, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#705, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#706, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#707, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#708, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#709, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#710, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#711, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#712, cast(case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#47, ?), decimal(20,0) BINARY)->Column#713, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#48)->Column#714, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#49)->Column#715, case(eq(intuit_risk.pmt_txn_fact.card_type, ?), Column#50)->Column#716, intuit_risk.pmt_txn_fact.card_type->Column#717, intuit_risk.pmt_txn_fact.entry_method->Column#718, intuit_risk.pmt_txn_fact.mt_gateway->Column#719	7.06 KB	N/A
      └─HashAgg_26	1.00	149	root		time:66.5ms, open:146.4µs, close:10.4µs, loops:17, partial_worker:{wall_time:66.295363ms, concurrency:8, task_num:1, tot_wait:66.072547ms, tot_exec:175.395µs, tot_time:530.110176ms, max:66.267967ms, p95:66.267967ms}, final_worker:{wall_time:66.38484ms, concurrency:16, task_num:16, tot_wait:20.293µs, tot_exec:618ns, tot_time:1.061486753s, max:66.363949ms, p95:66.363949ms}	group by:intuit_risk.pmt_txn_fact.card_type, intuit_risk.pmt_txn_fact.entry_method, intuit_risk.pmt_txn_fact.mt_gateway, intuit_risk.pmt_txn_fact.transaction_type, funcs:count(Column#545)->Column#47, funcs:sum(Column#546)->Column#48, funcs:min(Column#547)->Column#49, funcs:max(Column#548)->Column#50, funcs:firstrow(intuit_risk.pmt_txn_fact.transaction_type)->intuit_risk.pmt_txn_fact.transaction_type, funcs:firstrow(intuit_risk.pmt_txn_fact.mt_gateway)->intuit_risk.pmt_txn_fact.mt_gateway, funcs:firstrow(intuit_risk.pmt_txn_fact.entry_method)->intuit_risk.pmt_txn_fact.entry_method, funcs:firstrow(intuit_risk.pmt_txn_fact.card_type)->intuit_risk.pmt_txn_fact.card_type	248.1 KB	0 Bytes
        └─IndexReader_27	1.00	326	root	partition:p20260401,p20260501,p20260601,pmax	time:66.2ms, open:73.4µs, close:6.72µs, loops:2, cop_task: {num: 5, max: 56.6ms, min: 542.6µs, avg: 20.6ms, p95: 56.6ms, max_proc_keys: 68608, p95_proc_keys: 68608, tot_proc: 97.9ms, tot_wait: 218µs, copr_cache: disabled, build_task_duration: 18µs, max_distsql_concurrency: 4}, fetch_resp_duration: 66ms, rpc_info:{Cop:{num_rpc:5, total_time:102.7ms}}	index:HashAgg_19	43.4 KB	N/A
          └─HashAgg_19	1.00	326	cop[tikv]		tikv_task:{proc max:60ms, min:0s, avg: 22ms, p80:60ms, p95:60ms, iters:120, tasks:5}, scan_detail: {total_process_keys: 119821, total_process_keys_size: 21706797, total_keys: 119826, get_snapshot_time: 100.3µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 97.9ms, total_suspend_time: 220µs, total_wait_time: 218µs, total_kv_read_wall_time: 70ms}	group by:intuit_risk.pmt_txn_fact.card_type, intuit_risk.pmt_txn_fact.entry_method, intuit_risk.pmt_txn_fact.mt_gateway, intuit_risk.pmt_txn_fact.transaction_type, funcs:count(?)->Column#545, funcs:sum(intuit_risk.pmt_txn_fact.amount)->Column#546, funcs:min(intuit_risk.pmt_txn_fact.amount)->Column#547, funcs:max(intuit_risk.pmt_txn_fact.amount)->Column#548	N/A	N/A
            └─IndexRangeScan_25	285842.12	119821	cop[tikv]	table:p, index:idx_pmt_routing_runtime_cov(check_bank_routing_number, event_date, amount, mt_gateway, card_type, entry_method, transaction_type)	tikv_task:{proc max:50ms, min:0s, avg: 14ms, p80:50ms, p95:50ms, iters:120, tasks:5}	range:[? ?,? +inf], keep order:false	N/A	N/A
```

## group_a_bundle_014

- Group/window/filter: `A` / `90d` / `p.check_bank_routing_number = %s`
- Chosen event: `INV0019958147` kind=`hot_check_bank_routing_number` bundle_ms=`393.6` event_ms=`631.8`
- Bundle stats: n=`181` >350=`1` >500=`0` p95=`256.4ms` max=`393.6ms`

### Current Optimized

- SELECT time: `368.0ms` result=`ok`
- EXPLAIN ANALYZE: `307.7ms`, scan_sum=`340239`, scan_max=`340239`

### Candidate: group_a_dimension_rollup

- Result check: `same`; SELECT time: `105.9ms`
- Best EXPLAIN ANALYZE: `104.7ms` variant=`optimized_hashagg_16_8` scan_sum=`340239` scan_max=`340239` accepted=`True`

| Variant | Time | Scan Sum | Scan Max | Result |
| --- | ---: | ---: | ---: | --- |
| `candidate_default` | 106.1 ms | 340239 | 340239 | ok |
| `optimized_hashagg_16_8` | 104.7 ms | 340239 | 340239 | ok |
| `optimized_hashagg_32_8` | 112.2 ms | 340239 | 340239 | ok |
| `optimized_distinct_pushdown` | 110.9 ms | 340239 | 340239 | ok |
| `optimized_distinct_pushdown_hashagg_16_8` | 105.9 ms | 340239 | 340239 | ok |

#### Candidate SQL

```sql
SELECT
  SUM(b.row_count) AS `metric__a_0045`,
  SUM(b.amount_sum) AS `metric__a_0046`,
  MIN(b.amount_min) AS `metric__a_0047`,
  MAX(b.amount_max) AS `metric__a_0048`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `metric__a_1005`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1005`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.amount_sum END) AS `metric__a_1006`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1006`,
  MIN(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.amount_min END) AS `metric__a_1007`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1007`,
  MAX(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.amount_max END) AS `metric__a_1008`,
  SUM(CASE WHEN b.mt_gateway = 'MM-MerchantLink' THEN b.row_count ELSE 0 END) AS `present__a_1008`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `metric__a_1017`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1017`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.amount_sum END) AS `metric__a_1018`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1018`,
  MIN(CASE WHEN b.mt_gateway = 'QBMS' THEN b.amount_min END) AS `metric__a_1019`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1019`,
  MAX(CASE WHEN b.mt_gateway = 'QBMS' THEN b.amount_max END) AS `metric__a_1020`,
  SUM(CASE WHEN b.mt_gateway = 'QBMS' THEN b.row_count ELSE 0 END) AS `present__a_1020`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `metric__a_1029`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1029`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.amount_sum END) AS `metric__a_1030`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1030`,
  MIN(CASE WHEN b.mt_gateway = 'Direct' THEN b.amount_min END) AS `metric__a_1031`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1031`,
  MAX(CASE WHEN b.mt_gateway = 'Direct' THEN b.amount_max END) AS `metric__a_1032`,
  SUM(CASE WHEN b.mt_gateway = 'Direct' THEN b.row_count ELSE 0 END) AS `present__a_1032`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `metric__a_1041`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1041`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.amount_sum END) AS `metric__a_1042`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1042`,
  MIN(CASE WHEN b.mt_gateway = 'PayPal' THEN b.amount_min END) AS `metric__a_1043`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1043`,
  MAX(CASE WHEN b.mt_gateway = 'PayPal' THEN b.amount_max END) AS `metric__a_1044`,
  SUM(CASE WHEN b.mt_gateway = 'PayPal' THEN b.row_count ELSE 0 END) AS `present__a_1044`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `metric__a_1053`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1053`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.amount_sum END) AS `metric__a_1054`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1054`,
  MIN(CASE WHEN b.mt_gateway = 'Stripe' THEN b.amount_min END) AS `metric__a_1055`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1055`,
  MAX(CASE WHEN b.mt_gateway = 'Stripe' THEN b.amount_max END) AS `metric__a_1056`,
  SUM(CASE WHEN b.mt_gateway = 'Stripe' THEN b.row_count ELSE 0 END) AS `present__a_1056`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `metric__a_1065`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1065`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.amount_sum END) AS `metric__a_1066`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1066`,
  MIN(CASE WHEN b.mt_gateway = 'Square' THEN b.amount_min END) AS `metric__a_1067`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1067`,
  MAX(CASE WHEN b.mt_gateway = 'Square' THEN b.amount_max END) AS `metric__a_1068`,
  SUM(CASE WHEN b.mt_gateway = 'Square' THEN b.row_count ELSE 0 END) AS `present__a_1068`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `metric__a_1077`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1077`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.amount_sum END) AS `metric__a_1078`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1078`,
  MIN(CASE WHEN b.mt_gateway = 'Braintree' THEN b.amount_min END) AS `metric__a_1079`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1079`,
  MAX(CASE WHEN b.mt_gateway = 'Braintree' THEN b.amount_max END) AS `metric__a_1080`,
  SUM(CASE WHEN b.mt_gateway = 'Braintree' THEN b.row_count ELSE 0 END) AS `present__a_1080`
FROM (
  SELECT p.mt_gateway, COUNT(*) AS row_count, SUM(p.amount) AS amount_sum, MIN(p.amount) AS amount_min, MAX(p.amount) AS amount_max
  FROM pmt_txn_fact p
  WHERE p.check_bank_routing_number = %s AND p.event_date >= 1768101078317
  GROUP BY p.mt_gateway
) b
HAVING SUM(b.row_count) > 0;
```

#### Best Candidate EXPLAIN ANALYZE

```text
-- variant=optimized_hashagg_16_8
-- explain_analyze_elapsed_ms=104.7
id	estRows	actRows	task	access object	execution info	operator info	memory	disk
Projection_9	0.80	1	root		time:74.3ms, open:138.8µs, close:12.9µs, loops:2, RU:845.76, Concurrency:OFF	Column#51->Column#83, Column#52->Column#84, Column#53->Column#85, Column#54->Column#86, Column#55->Column#87, Column#55->Column#88, Column#56->Column#89, Column#55->Column#90, Column#57->Column#91, Column#55->Column#92, Column#58->Column#93, Column#55->Column#94, Column#59->Column#95, Column#59->Column#96, Column#60->Column#97, Column#59->Column#98, Column#61->Column#99, Column#59->Column#100, Column#62->Column#101, Column#59->Column#102, Column#63->Column#103, Column#63->Column#104, Column#64->Column#105, Column#63->Column#106, Column#65->Column#107, Column#63->Column#108, Column#66->Column#109, Column#63->Column#110, Column#67->Column#111, Column#67->Column#112, Column#68->Column#113, Column#67->Column#114, Column#69->Column#115, Column#67->Column#116, Column#70->Column#117, Column#67->Column#118, Column#71->Column#119, Column#71->Column#120, Column#72->Column#121, Column#71->Column#122, Column#73->Column#123, Column#71->Column#124, Column#74->Column#125, Column#71->Column#126, Column#75->Column#127, Column#75->Column#128, Column#76->Column#129, Column#75->Column#130, Column#77->Column#131, Column#75->Column#132, Column#78->Column#133, Column#75->Column#134, Column#79->Column#135, Column#79->Column#136, Column#80->Column#137, Column#79->Column#138, Column#81->Column#139, Column#79->Column#140, Column#82->Column#141, Column#79->Column#142	59.9 KB	N/A
└─Selection_11	0.80	1	root		time:74.3ms, open:135.9µs, close:11.5µs, loops:2	gt(Column#51, ?)	59.9 KB	N/A
  └─StreamAgg_18	1.00	1	root		time:74.3ms, open:133.4µs, close:11µs, loops:3	funcs:sum(Column#154)->Column#51, funcs:sum(Column#155)->Column#52, funcs:min(Column#156)->Column#53, funcs:max(Column#157)->Column#54, funcs:sum(Column#158)->Column#55, funcs:sum(Column#159)->Column#56, funcs:min(Column#160)->Column#57, funcs:max(Column#161)->Column#58, funcs:sum(Column#162)->Column#59, funcs:sum(Column#163)->Column#60, funcs:min(Column#164)->Column#61, funcs:max(Column#165)->Column#62, funcs:sum(Column#166)->Column#63, funcs:sum(Column#167)->Column#64, funcs:min(Column#168)->Column#65, funcs:max(Column#169)->Column#66, funcs:sum(Column#170)->Column#67, funcs:sum(Column#171)->Column#68, funcs:min(Column#172)->Column#69, funcs:max(Column#173)->Column#70, funcs:sum(Column#174)->Column#71, funcs:sum(Column#175)->Column#72, funcs:min(Column#176)->Column#73, funcs:max(Column#177)->Column#74, funcs:sum(Column#178)->Column#75, funcs:sum(Column#179)->Column#76, funcs:min(Column#180)->Column#77, funcs:max(Column#181)->Column#78, funcs:sum(Column#182)->Column#79, funcs:sum(Column#183)->Column#80, funcs:min(Column#184)->Column#81, funcs:max(Column#185)->Column#82	61.6 KB	N/A
    └─Projection_37	1.00	10	root		time:74.3ms, open:128.7µs, close:10.6µs, loops:7, Concurrency:OFF	cast(Column#47, decimal(20,0) BINARY)->Column#154, Column#48->Column#155, Column#49->Column#156, Column#50->Column#157, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#158, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#159, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#160, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#161, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#162, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#163, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#164, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#165, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#166, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#167, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#168, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#169, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#170, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#171, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#172, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#173, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#174, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#175, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#176, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#177, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#178, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#179, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#180, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#181, cast(case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#47, ?), decimal(20,0) BINARY)->Column#182, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#48)->Column#183, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#49)->Column#184, case(eq(intuit_risk.pmt_txn_fact.mt_gateway, ?), Column#50)->Column#185	7.86 KB	N/A
      └─HashAgg_27	1.00	10	root		time:74ms, open:127.2µs, close:10µs, loops:7, partial_worker:{wall_time:73.813085ms, concurrency:8, task_num:1, tot_wait:73.727365ms, tot_exec:33.328µs, tot_time:590.189967ms, max:73.777628ms, p95:73.777628ms}, final_worker:{wall_time:73.836147ms, concurrency:16, task_num:16, tot_wait:31.036µs, tot_exec:589ns, tot_time:1.18075717s, max:73.810343ms, p95:73.810343ms}	group by:intuit_risk.pmt_txn_fact.mt_gateway, funcs:count(Column#146)->Column#47, funcs:sum(Column#147)->Column#48, funcs:min(Column#148)->Column#49, funcs:max(Column#149)->Column#50, funcs:firstrow(intuit_risk.pmt_txn_fact.mt_gateway)->intuit_risk.pmt_txn_fact.mt_gateway	89.7 KB	0 Bytes
        └─IndexReader_28	1.00	40	root	partition:p20260201,p20260301,p20260401,p20260501,p20260601,pmax	time:73.8ms, open:58.1µs, close:6.75µs, loops:2, cop_task: {num: 6, max: 73.7ms, min: 474µs, avg: 34.4ms, p95: 73.7ms, max_proc_keys: 122612, p95_proc_keys: 122612, tot_proc: 201.1ms, tot_wait: 255.9µs, copr_cache: disabled, build_task_duration: 17.3µs, max_distsql_concurrency: 6}, fetch_resp_duration: 73.7ms, rpc_info:{Cop:{num_rpc:6, total_time:206.2ms}}	index:HashAgg_19	3.35 KB	N/A
          └─HashAgg_19	1.00	40	cop[tikv]		tikv_task:{proc max:70ms, min:0s, avg: 31.7ms, p80:60ms, p95:70ms, iters:336, tasks:6}, scan_detail: {total_process_keys: 340239, total_process_keys_size: 50848432, total_keys: 340245, get_snapshot_time: 115.7µs, rocksdb: {block: {}}}, time_detail: {total_process_time: 201.1ms, total_suspend_time: 410.1µs, total_wait_time: 255.9µs, total_kv_read_wall_time: 150ms}	group by:intuit_risk.pmt_txn_fact.mt_gateway, funcs:count(?)->Column#146, funcs:sum(intuit_risk.pmt_txn_fact.amount)->Column#147, funcs:min(intuit_risk.pmt_txn_fact.amount)->Column#148, funcs:max(intuit_risk.pmt_txn_fact.amount)->Column#149	N/A	N/A
            └─IndexRangeScan_25	479923.47	340239	cop[tikv]	table:p, index:idx_test(check_bank_routing_number, event_date, mt_gateway, amount)	tikv_task:{proc max:70ms, min:0s, avg: 25ms, p80:40ms, p95:70ms, iters:336, tasks:6}	range:[? ?,? +inf], keep order:false	N/A	N/A
```
