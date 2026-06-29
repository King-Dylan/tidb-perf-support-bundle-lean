# Go load-generator items for Jinlong (go-loadgen/main.go)

These came out of an adversarial review. They are **harness-internal semantics in your Go code**, so they
need your call + a real `go build` (there was no Go toolchain where the rest was fixed). Everything else in
the bundle — the report, workload generator, fleet runner, sampler — is already fixed (see PATCH_NOTES_V45.md).

**Important context: in the blessed customer run these are near-zero impact.** The event pool is built with
complete 8-field bindings (so almost nothing is skipped), the serving table is populated for the run window
(so almost nothing returns zero rows), and we run **conn-fanout** (which avoids the event-fanout timing
artifacts). These are hardening, not live bugs for the actual run. The report now **discloses** the skipped
case loudly, which is the mitigation we chose on the Python side.

Also: **main.go was not compiled where these edits were staged** (no Go toolchain). Please rebuild it on the
EC2 host via `validate_event_report_bundle.sh` (checklist step 1) before the run — that catches any compile error.

## NEW (2026-06-29) — serving coverage makes the empty-row guard run-blocking

Measured on the Premium cluster: the wide serving table (`risk_feature_serving_wide`) covers through as_of `2026-04-10`, but base data now runs to `2026-05-28`, and a fresh `ORDER BY RAND()` pool references ~11% of days the table does not have. Each 180d serving bundle is a point lookup keyed by (bundle, key, as_of = the event's own date); an uncovered (bundle, key, date) returns **zero rows**, which today counts as a fast success (item 2 below) → ~10% of events report 12 hollow "instant successes" and the 65/65 headline is inflated. The served **values are correct where present** (wide == raw == prod180, 12/12 verified) — this is a coverage problem, not a correctness one.

Two things for the run:
1. **Rebuild/refresh the wide serving table for the run pool's date range** before the run (it is an MV that must be refreshed to the run window). Ravish ships a preflight, `serving_coverage_check.py`, that fails loud below 99% coverage — run it after the pool build. **(Still your side — needs the cluster + your build process.)**
2. **The empty-serving guard is now IMPLEMENTED in `main.go` (v4.5.4).** A serving bundle (its SQL hits `risk_feature_serving*`, detected at startup into `servingTemplates`) that returns zero rows is now classified as a distinct `empty` outcome in **conn-fanout**: it is NOT counted as a success (so it can't pad 65/65) and NOT as an error (so it can't trip DEGRADED). Run-level count is emitted as `empty_serving` in the result JSON. **Verified: `go build` + `go vet` clean.** Please rebuild via `validate_event_report_bundle.sh` and **confirm the `empty_serving` count behaves as expected in your validation run** (it is scoped to conn-fanout only; worker-pool/event-fanout still use the old success=qerr==nil logic). Implementation: `fetchRowsCount()`, the `servingTemplates` map, and the guard in `runOneEventConnFanout`.

Also: scale the cluster up (TiKV/TiDB) for throughput before the run. The benchmark and pool build pin `tidb_isolation_read_engines='tikv'`, so they are TiKV-only and do not use TiFlash (it can be scaled down to save RU during the run; keep the replica for the analytics story). A manual query without that pin can get pushed to TiFlash MPP and OOM on a scaled-in footprint — pin tikv when probing.

## Worth deciding before the customer run
1. **Skipped (null-binding) bundles count as successes** toward 60/65 and 65/65, and toward
   `bundles_by_300/350/500ms` (they're marked `Success=true` with ~0ms completion).
   - event-fanout ~586-588, 615-618, 650-668; conn-fanout ~959-962, 992-995, 1026-1044; worker-pool ~426-428, 462-465, 479-482.
   - Options: (a) add `outcome.Skipped` and base the SLA + cutoff counts on **executed** bundles only, or
     (b) leave as-is (the report now discloses how many of the 65 were skipped). We did (b). Your call on (a).
2. **Zero-row query responses are counted as fast successes.** `fetchRows()` (~302-319) discards values and
   returns nil regardless of row count, and success is decided purely on `qerr==nil` (~600-604, 976-980). A
   serving point-lookup that misses (key absent for the run's reference window) then looks like a fast hit.
   - Suggest: have `fetchRows` return a row count; treat zero-row **serving** responses as a distinct
     `empty` outcome and surface an `empty_serving_responses` counter so a serving miss can't masquerade as a hit.
3. **Emit a `deadline_cut` count** (queries aborted by the `max_execution_time` cap / context deadline —
   MySQL err 3024 "maximum statement execution time exceeded", or context deadline/canceled), separate from
   `total_errors`. With the 500ms cap now on by default, deadline cuts are EXPECTED SLA misses, not failures.
   The report already separates them and only flags DEGRADED on genuine errors — but it does so by
   **pattern-matching the sampled `first_query_errors` messages**, which can miss a rare genuine error that
   doesn't land in the 5-message sample. A per-result `deadline_cut` integer (incremented when the query error
   matches the deadline signatures) lets the report split exactly instead of heuristically. Low effort, removes
   the only soft spot in the otherwise-correct error classification.

## Lower priority (mostly mitigated by conn-fanout)
3. event-fanout folds connection-acquisition wait into the metric labeled "SQL-only" (~590-611); `queueMS`
   measures goroutine-launch delay, not connection wait (~581). conn-fanout separates these cleanly — use it.
4. event-fanout has no backpressure by default (`--max-pending-events 0`) and shares one pool — the prior
   connection-starvation shape. We default the customer run to conn-fanout; consider making conn-fanout the
   default mode and/or right-sizing `--connections-total` for 65k bundle-SQL/sec.
5. SQL-only `score60` order statistic is deflated because skipped bundles inject 0.0ms into the sorted array (~670-683).
6. Global prepare mutex is locked on every bundle in event-fanout (~736-749), adding client-side serialization.
7. Go pool silently wraps the event list modulo its length (~245-253) — add a warning/abort if the workload
   has fewer unique events than the run needs (defense-in-depth against the replay anti-pattern).
8. Redundant blank import of `go-sql-driver/mysql` (lines 19-20) — harmless (it compiles), but tidy it up.
