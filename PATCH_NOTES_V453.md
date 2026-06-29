# v4.5.3 patch notes — serving-coverage gate, 180d correctness gate, ladder, disclosures

Built on v4.5.2 after a full review of the Intuit use-case spec PDF + 3 months of call
transcripts against the bundle. The benchmark logic and the served 180d values are correct;
this release adds the missing **gates** so a stale serving table or a scope gap cannot silently
produce a wrong/over-claimed customer number, and folds in the spec's concurrency-ladder ask.

## What changed

- **NEW `serving_coverage_check.py`** — pre-run guard. Replays the 12 180d serving lookups for a
  generated pool and fails loud if wide-table coverage is below `--min-coverage` (default 99%).
  Closes the "empty serving lookup counts as a fast success" inflation at the pool level.
- **NEW `spotcheck_wide_serving.py`** — correctness gate for the BENCHMARKED path. Asserts
  `wide point-lookup == prod180 rollup == raw runtime 180d aggregate` for the 12 served bundles.
  Verified live 2026-06-29: **12/12 PASS** at as_of `2026-04-10`. (The shipped
  spotcheck_prod180_correctness.py only checked raw-vs-rollup, not the wide table the fleet serves.)
- **`setup_schema.py`** — `INTUIT_PARTITION_GRAIN` default flipped `weekly` -> `monthly` (the
  blessed best-performing grain; the canonical build already forced monthly).
- **`FINAL_RUN_CHECKLIST.md`**:
  - New **Step 3.5** — serving-table coverage + 180d correctness gate (both must pass before the run),
    plus the wide-table-rebuild-for-the-pool dependency and the scale-up note.
  - **Concurrency ladder** (Intuit Additional Requirement #5) added to Step 5: run the fleet at
    100/200/500/1000 EPS and report P50/P95/P99 + TPS at each.
  - **Disclosures** section: 180d transactions / ~160d device; wall-clock + ~10ms app-hop vs the
    300/350ms SLA; production read pattern only (not both strategies); confirm serving coverage >=99%.
- **`GO_HANDOFF_JINLONG.md`** — elevated the zero-row/empty-serving guard to **run-blocking** with
  the measured coverage finding, and added the wide-table refresh dependency.

## Key findings behind these changes (verified on the cluster)

- **Served 180d values are correct.** 12/12 wide == raw == prod180. The big base tables
  (pmt_txn_fact ~83.5M, deviceprofile_fact ~365.8M) match the spec's monthly volumes exactly.
- **Serving coverage is the real pre-run item.** Wide table covers through `2026-04-10`; base
  data drifted to `2026-05-28` (0.08% junk inserts with null keys, excluded by sampling). A fresh
  RAND pool gets ~89% serving coverage; ~10% of events would report hollow instant-successes
  unless the wide table is refreshed for the pool and/or main.go guards empty responses.
- **Graph / UDF / STDDEV are NOT required** by the spec (only a single-hop Group C join; standard
  SQL aggregates). The 65 bundles render 2,738 aggregates (> the spec's ~1,200), faithful to the
  Group A/B/C catalog.
- **Scope to confirm with Intuit:** canonical metric count (~1000 vs ~1200), SLA number/boundary
  (300 vs 350, DB-only vs end-to-end), Group C join predicate, 180d backfill depth.

## Still owned by Jinlong / POC (not in this bundle)

- Refresh the wide serving table for the run pool + the `main.go` empty-serving guard (run-blocking).
- Additional Requirements deferred to POC: HTAP concurrent read+write, MV backfill metrics,
  failure/resilience, cluster sizing & cost. Full-180 device window = a data backfill.
