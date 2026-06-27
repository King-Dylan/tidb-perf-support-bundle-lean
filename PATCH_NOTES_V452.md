# v4.5.2 patch notes — hot-event rate grounded in cluster data

Single change on top of v4.5.1. No code logic changed; only one default value, plus docs.

## What changed

- **`--hot-event-pct` default 0.05 → 0.07** in `build_reuse_events_from_stats.py` and
  `generate_go_workload.py` (and the legacy `mixed_traffic_test.py` aligned 0.10 → 0.07 so there is
  one consistent number). A forgotten flag now produces the measured rate, not the old guess.
- `FINAL_RUN_CHECKLIST.md` example commands updated to `0.07`, with a note documenting the derivation
  and the known field-mix gap.

## Why 0.07 (the measurement)

Measured live on 2026-06-26 against the Premium cluster (`tidb-cjysta2c78rd`, TiDB X v8.5.4-nextgen)
from precomputed stats only (`SHOW STATS_META` + `SHOW STATS_TOPN`), so it cost ~nothing on the
scaled-in cluster. "Hot" = a field value with **>10,000 occurrences** (the harness's own normal/hot
threshold, `--max-payment-rows` / `--max-device-rows` = 10000).

Base-table sizes: `pmt_txn_fact` 83.5M rows, `deviceprofile_fact` 365.8M rows.

Per-field share of real traffic sitting on a hot value (all exact — every >10k value is captured in TopN):

| Field | hot values (>10k) | % of traffic |
|---|---|---|
| check_bank_routing_number | 10 | 4.29% |
| input_ip | 9 | 1.12% |
| true_ip | 9 | 1.09% |
| smart_id | 9 | 0.81% |
| merchant_account_number | 1 | 0.10% |
| exact_id | 9 | 0.09% |
| card_holder_number_sha512 | 0 | 0.00% |
| check_bank_account_number_sha512 | 0 | 0.00% |

Combined event-hot rate ≈ **5–7%** (routing alone ~4.3%, up to ~7% treating fields as independent).
0.07 is the rounded-up, conservative end. The two SHA512 fields are 0.00% hot, which confirms the
existing `--no-hot-field` exclusion.

## Known gap carried forward (not fixed in 4.5.2)

The generator still spreads hot events **evenly** across the 6 real hot fields. The measurement says
real hot traffic is **routing-dominated**: ~57% check_bank_routing_number, ~15% input_ip, ~14% true_ip,
~11% smart_id, ~1% merchant, ~1% exact_id. If routing lookups are the slow tail, the even split
under-tests them. Weighting the hot-field selection to the measured shares is a follow-up.

## Caveats

- Stats-based estimate from the two base tables; the exact event-level rate depends on the
  payment↔device join cardinality (not run, to avoid a credit-burning join scan on the scaled-in cluster).
- Reflects the all-time average in the tables; a real peak burst could be spikier. Confirm with Intuit
  whether their peak is more concentrated, and whether routing-dominance matches their view.
