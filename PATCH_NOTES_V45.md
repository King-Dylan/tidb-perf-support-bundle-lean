# v4.5 patch notes — report & harness honesty fixes

v4.5 fixes 43 issues found in an adversarial code review of v4.4. None were in the TiDB cluster or the
workload design; all were in the **reporting and load-harness plumbing** that could make the
customer-facing numbers wrong, self-contradictory, or misleadingly clean. Every fix below was
verified by rendering the report against a synthetic run that included failed events, query errors,
and an elapsed-time overrun.

## Blockers
- **Fleet EPS / connections were shown at 1/N.** The fleet splits target EPS and connections across
  worker processes; the report read the per-process value, so a 1000 EPS / 1300-conn run was titled
  "125 EPS" with "163 connections." Fixed: the fleet now writes `<prefix>_summary.json` with a
  `run_shape` block of **fleet totals**, and the report reads it via `--fleet-summary` (or sums across
  per-host result files). Title, target load, SQL/sec, connections, and Test Shape now show fleet totals.
- **"Achieved throughput" always equaled target by construction** (it divided by the configured
  duration). Fixed: achieved EPS is now `completed_events / actual_elapsed` (max elapsed across hosts),
  the report prints both configured duration and actual elapsed, and flags a **Sustained-rate miss**
  when elapsed exceeds the window by >10%.

## High
- **Headline latency hid non-completing events.** Percentiles dropped the `-1` sentinels, so "max"
  excluded events that missed the SLA. Fixed: every latency line now states its population
  ("over N of M events that returned all 65"); a tier with zero completions renders
  "no events completed", not "0.0ms".
- **Self-contradictory latency rows.** Within a row, `n`/percentiles excluded failed events but the
  `>cutoff` columns counted them ("max 279ms" next to ">500ms=200"). Fixed: `>cutoff` counts only
  completed events over the cutoff; non-completions live in a separate **did_not_complete** column.
- **Serving bundles defaulted to the KV table, not the wide table** the checklist claims drives the
  ~100-240ms numbers, with nothing recording which table was hit. Fixed: `generate_go_workload.py`
  adds `--serving-layout {kv,wide}` defaulting to **wide**, threads it through, validates
  `--serving-bundle` ids (errors on a typo), and records `serving_layout` + `serving_table` in the
  workload JSON and the report.
- **Query errors were never surfaced; failed/missing fleet hosts were silently dropped.** Fixed: new
  **Errors & Completion** section (total errors, events that didn't reach 65/65, skipped bundles,
  connections established vs configured, sample errors), plus a warning when fewer hosts reported than
  the fleet summary expected. Any errors / connection shortfall / overrun marks the run **degraded**.
- **`--runtime-window-params` defaulted to False**, freezing the first event's time window into every
  runtime bundle. Fixed: default is now **True**; the report loudly warns if a non-smoke run was
  generated with it off.
- **SQL-only "score60/full65" were per-event order statistics, mislabeled for direct comparison with
  wall-clock.** Fixed: relabeled "60th-fastest / Slowest bundle query runtime" with a caption stating
  they are a best-case lower bound for diagnosis only.
- **event-fanout connection starvation** (the prior 11s failure mode): the runner now warns when
  event-fanout runs with no backpressure; **conn-fanout is recommended** for customer runs.

## Go load generator (go-loadgen/main.go)
- Per-event completion timestamp is now captured immediately after `wg.Wait()` and **before** the
  metrics mutex, so full-event latency is no longer inflated by lock contention.
- Emits a per-event `skipped` count (null-binding bundles) so the report can disclose it.
- Worker-pool remaining-counter and unknown-bundle-id handling fixed so a bad id can't hang the run.
- **Note:** no Go toolchain was available where these edits were made, so main.go was verified by
  inspection (brace/paren balanced) only. It MUST be rebuilt on the EC2 host via
  `validate_event_report_bundle.sh` (checklist step 1) before the customer run — that step will catch
  any compile error.

## Sampler / build-reuse
- Reuse path no longer silently truncates the normal pool to `--normal-events` (default 80); it uses
  the full reused pool and warns on explicit truncation.
- `sample_mixed_events` no longer injects hot keys on the 2 SHA512 fields.
- Normal-event candidate selection no longer uses `LIMIT` without ordering.
- `build_reuse` validates `--no-hot-field` names and applies a minimum-frequency guard on fallback hot keys.

## New run mechanics (see FINAL_RUN_CHECKLIST.md)
- The fleet writes `<output-prefix>_summary.json`; pass it to the report with `--fleet-summary`.
- generate defaults: `--serving-layout wide`, `--runtime-window-params` on.
- Recommended execution mode for customer runs: `conn-fanout`.
