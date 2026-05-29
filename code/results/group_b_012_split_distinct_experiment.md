# group_b_bundle_012 split distinct experiment

event=INV0039365135 kind=hot_true_ip

## current

same=True best=509.5ms variant=optimized_hashagg_16_8 scan_sum=172835

| variant | ms | ok | scan_sum |
| --- | ---: | --- | ---: |
| `default` | 527.5 | True | 172835 |
| `optimized_hashagg_16_8` | 509.5 | True | 172835 |
| `optimized_hashagg_32_8` | 511.8 | True | 172835 |
| `optimized_distinct_pushdown` | 2392.1 | True | 172835 |
| `optimized_distinct_pushdown_hashagg_16_8` | 2384.8 | True | 172835 |

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
WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000'
HAVING COUNT(*) > 0;
```

## split_distinct

same=True best=1658.7ms variant=optimized_distinct_pushdown_hashagg_16_8 scan_sum=1037010

| variant | ms | ok | scan_sum |
| --- | ---: | --- | ---: |
| `default` | 1826.6 | True | 1037010 |
| `optimized_hashagg_16_8` | 1727.2 | True | 1037010 |
| `optimized_hashagg_32_8` | 1722.8 | True | 1037010 |
| `optimized_distinct_pushdown` | 1674.9 | True | 1037010 |
| `optimized_distinct_pushdown_hashagg_16_8` | 1658.7 | True | 1037010 |

```sql
SELECT
  base.`metric__b_0011`,
  base.`metric__b_0057`,
  base.`present__b_0057`,
  base.`metric__b_0060`,
  base.`present__b_0060`,
  base.`metric__b_0063`,
  base.`present__b_0063`,
  base.`metric__b_0066`,
  base.`present__b_0066`,
  base.`metric__b_0069`,
  base.`present__b_0069`,
  base.`metric__b_0072`,
  base.`present__b_0072`,
  (SELECT COUNT(DISTINCT d.exact_id) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.exact_id IS NOT NULL) AS `metric__b_0161`,
  (SELECT COUNT(DISTINCT d.smart_id) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.smart_id IS NOT NULL) AS `metric__b_0165`,
  (SELECT COUNT(DISTINCT d.input_ip) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.input_ip IS NOT NULL) AS `metric__b_0169`,
  (SELECT COUNT(DISTINCT d.proxy_ip) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.proxy_ip IS NOT NULL) AS `metric__b_0173`,
  (SELECT COUNT(DISTINCT d.agent_type) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.agent_type IS NOT NULL) AS `metric__b_0177`,
  base.`metric__b_0275`,
  base.`present__b_0275`,
  base.`metric__b_0276`,
  base.`present__b_0276`,
  base.`metric__b_0277`,
  base.`present__b_0277`,
  base.`metric__b_0284`,
  base.`present__b_0284`,
  base.`metric__b_0285`,
  base.`present__b_0285`,
  base.`metric__b_0286`,
  base.`present__b_0286`,
  base.`metric__b_0293`,
  base.`present__b_0293`,
  base.`metric__b_0294`,
  base.`present__b_0294`,
  base.`metric__b_0295`,
  base.`present__b_0295`,
  base.`metric__b_0302`,
  base.`present__b_0302`,
  base.`metric__b_0303`,
  base.`present__b_0303`,
  base.`metric__b_0304`,
  base.`present__b_0304`,
  base.`metric__b_0311`,
  base.`present__b_0311`,
  base.`metric__b_0312`,
  base.`present__b_0312`,
  base.`metric__b_0313`,
  base.`present__b_0313`
FROM (
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
  FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000'
  HAVING COUNT(*) > 0
) base;
```

## split_distinct_numeric

same=True best=1761.7ms variant=optimized_distinct_pushdown_hashagg_16_8 scan_sum=1037010

| variant | ms | ok | scan_sum |
| --- | ---: | --- | ---: |
| `default` | 1817.9 | True | 1037010 |
| `optimized_hashagg_16_8` | 1816.6 | True | 1037010 |
| `optimized_hashagg_32_8` | 1821.1 | True | 1037010 |
| `optimized_distinct_pushdown` | 1770.4 | True | 1037010 |
| `optimized_distinct_pushdown_hashagg_16_8` | 1761.7 | True | 1037010 |

```sql
SELECT
  base.`metric__b_0011`,
  base.`metric__b_0057`,
  base.`present__b_0057`,
  base.`metric__b_0060`,
  base.`present__b_0060`,
  base.`metric__b_0063`,
  base.`present__b_0063`,
  base.`metric__b_0066`,
  base.`present__b_0066`,
  base.`metric__b_0069`,
  base.`present__b_0069`,
  base.`metric__b_0072`,
  base.`present__b_0072`,
  (SELECT COUNT(DISTINCT d.exact_id) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.exact_id IS NOT NULL) AS `metric__b_0161`,
  (SELECT COUNT(DISTINCT d.smart_id) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.smart_id IS NOT NULL) AS `metric__b_0165`,
  (SELECT COUNT(DISTINCT d.input_ip) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.input_ip IS NOT NULL) AS `metric__b_0169`,
  (SELECT COUNT(DISTINCT d.proxy_ip) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.proxy_ip IS NOT NULL) AS `metric__b_0173`,
  (SELECT COUNT(DISTINCT d.agent_type) FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000' AND d.agent_type IS NOT NULL) AS `metric__b_0177`,
  base.`metric__b_0275`,
  base.`present__b_0275`,
  base.`metric__b_0276`,
  base.`present__b_0276`,
  base.`metric__b_0277`,
  base.`present__b_0277`,
  base.`metric__b_0284`,
  base.`present__b_0284`,
  base.`metric__b_0285`,
  base.`present__b_0285`,
  base.`metric__b_0286`,
  base.`present__b_0286`,
  base.`metric__b_0293`,
  base.`present__b_0293`,
  base.`metric__b_0294`,
  base.`present__b_0294`,
  base.`metric__b_0295`,
  base.`present__b_0295`,
  base.`metric__b_0302`,
  base.`present__b_0302`,
  base.`metric__b_0303`,
  base.`present__b_0303`,
  base.`metric__b_0304`,
  base.`present__b_0304`,
  base.`metric__b_0311`,
  base.`present__b_0311`,
  base.`metric__b_0312`,
  base.`present__b_0312`,
  base.`metric__b_0313`,
  base.`present__b_0313`
FROM (
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
  FROM (SELECT d.agent_type, CASE WHEN d.device_score IS NOT NULL AND d.device_score != '' THEN CAST(d.device_score AS DECIMAL(10,2)) END AS `device_score__num`, CASE WHEN d.device_fingerprint_score IS NOT NULL AND d.device_fingerprint_score != '' THEN CAST(d.device_fingerprint_score AS DECIMAL(10,2)) END AS `device_fingerprint_score__num`, CASE WHEN d.device_worst_score IS NOT NULL AND d.device_worst_score != '' THEN CAST(d.device_worst_score AS DECIMAL(10,2)) END AS `device_worst_score__num`, CASE WHEN d.true_ip_score IS NOT NULL AND d.true_ip_score != '' THEN CAST(d.true_ip_score AS DECIMAL(10,2)) END AS `true_ip_score__num`, CASE WHEN d.input_ip_score IS NOT NULL AND d.input_ip_score != '' THEN CAST(d.input_ip_score AS DECIMAL(10,2)) END AS `input_ip_score__num` FROM deviceprofile_fact d WHERE d.true_ip = %s AND d.jms_timestamp >= '2026-03-11 19:16:29.762000') d
  HAVING COUNT(*) > 0
) base;
```
