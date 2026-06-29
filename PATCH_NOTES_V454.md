# v4.5.4 patch notes — empty-serving guard implemented in main.go

Single addition on top of v4.5.3: the Go harness now classifies an empty serving
point-lookup as a distinct outcome instead of a fast success. This closes, in the
harness itself, the "~10% hollow instant-successes" inflation that v4.5.3 only
caught at the pool level (via `serving_coverage_check.py`).

## What changed (`go-loadgen/main.go`, conn-fanout path)

- **`fetchRowsCount(rows) (int, error)`** — new; returns the row count (the existing
  `fetchRows` is unchanged for the worker-pool / event-fanout paths).
- **`servingTemplates map[int]bool`** — populated at startup: a template is "serving"
  if its SQL hits `risk_feature_serving*`. This reliably tags exactly the 12 served
  180d bundles and never a runtime bundle (runtime SQL hits `pmt_txn_fact` /
  `deviceprofile_fact`).
- **Guard in `runOneEventConnFanout`** — when a serving bundle returns zero rows:
  `success = false; empty = true`. Empties are then:
  - NOT counted toward successes (so they can't pad 60/65 or 65/65),
  - NOT counted as errors (so they can't trip DEGRADED),
  - tallied separately and emitted as `empty_serving` (run-level) in the result JSON.
- Scope: **conn-fanout only** (the customer-run mode). worker-pool / event-fanout keep
  the prior `success = (qerr == nil)` logic. Runtime bundles are unaffected — a
  legitimate zero-row runtime aggregate is never misflagged.

## Verification

- `go build` and `go vet` are **clean** on EC2 (go 1.26.3).
- Behavior (empty count, miss accounting) should be confirmed in the validation run;
  the v4.5.3 `serving_coverage_check.py` preflight remains the primary, pool-level gate.

## Still remaining for the run (unchanged from v4.5.3)

- Scale the cluster up (a scaled-in footprint OOMs TiFlash on the RAND()-join sampling).
- Refresh the wide serving table for the run pool's date range (Jinlong; cluster).
- Run the concurrency ladder (100/200/500/1000).
- Confirm with Intuit: SLA number/boundary, Group C join predicate, 180d backfill depth.
