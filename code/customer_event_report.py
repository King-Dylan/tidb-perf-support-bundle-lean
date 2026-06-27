#!/usr/bin/env python3
"""Generate a customer-readable event-level report from Go loadgen results.

Matches the v10 customer report layout (Test Shape, Binding Reuse / Test
Realism, Event Mix, Runtime vs Pre-Agg / Serving Bundle Counts, Hot-Key Values
Used, Event Latency, 60/65 + 65/65 SLA, Tail / Miss Drivers) and adds:

- Benchmark-harness event wall-clock latency (timer starts before the 65 bundle
  queries fan out, stops when the 60th / 65th result is received).
- SQL-only (DB-side) event latency side by side, when the Go output provides it.
- 300ms / 350ms / 500ms cutoffs for the SLA and average-bundles views.
- All / Normal / Hot-key breakdown for the SLA and latency views.
- Return-time histogram and per-field realism stats.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


KEY_FIELDS = [
    "merchant_account_number",
    "card_holder_number_sha512",
    "check_bank_routing_number",
    "check_bank_account_number_sha512",
    "exact_id",
    "smart_id",
    "input_ip",
    "true_ip",
]

HOT_EVENT_KINDS = [
    "normal",
    "hot_merchant_account_number",
    "hot_card_holder_number_sha512",
    "hot_check_bank_routing_number",
    "hot_check_bank_account_number_sha512",
    "hot_exact_id",
    "hot_smart_id",
    "hot_input_ip",
    "hot_true_ip",
]

CUTOFFS = [300.0, 350.0, 500.0]
HIST_BUCKETS = [
    ("0-50ms", 0.0, 50.0),
    ("50-100ms", 50.0, 100.0),
    ("100-150ms", 100.0, 150.0),
    ("150-200ms", 150.0, 200.0),
    ("200-300ms", 200.0, 300.0),
    ("300-350ms", 300.0, 350.0),
    ("350-500ms", 350.0, 500.0),
]


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int((len(ordered) * p + 99) // 100) - 1
    idx = max(0, min(idx, len(ordered) - 1))
    return ordered[idx]


def summarize(values: list[float]) -> dict[str, float]:
    # A latency value < 0 is the Go loadgen's "did not complete this tier"
    # sentinel (full65_ms=-1 when <65 bundles returned; score60_ms=-1 when <60).
    # n / percentiles / max are computed ONLY over completed events (value >= 0).
    # "total" is the full event population and "not_completed" the sentinel count,
    # so callers can render miss columns and a separate did-not-complete column
    # over a single, consistent denominator.
    total = len(values)
    clean = [v for v in values if v >= 0]
    not_completed = total - len(clean)
    if not clean:
        return {
            "n": 0,
            "total": total,
            "not_completed": not_completed,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
        }
    return {
        "n": len(clean),
        "total": total,
        "not_completed": not_completed,
        "p50": percentile(clean, 50),
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": max(clean),
    }


def count_over(values: list[float], cutoff: float) -> int:
    """Count ONLY completed events (value >= 0) whose latency exceeds the cutoff.

    The <0 "did not complete" sentinels are NOT folded in here -- they are
    surfaced separately as a did-not-complete count so a row's max can never be
    smaller than its >cutoff count implies.
    """
    return sum(1 for v in values if v >= 0 and v > cutoff)


def fmt_ms(value: float | None) -> str:
    # None means no completed events for this tier -- never render as 0.0ms,
    # which would read as instant success rather than "no data".
    if value is None:
        return "n/a"
    if value >= 1000:
        return f"{value / 1000:.2f}s"
    return f"{value:.1f}ms"


def fmt_stat(summary: dict[str, Any], key: str) -> str:
    """Render a percentile/max cell, surfacing the n==0 (no completions) case."""
    if summary["n"] == 0:
        return f"no events completed (0/{summary['total']:,})"
    return fmt_ms(summary[key])


def fmt_duration(seconds: float) -> str:
    seconds = float(seconds or 0)
    if seconds <= 0:
        return "unknown"
    if seconds % 60 == 0:
        return f"{int(seconds // 60)}m ({int(seconds)}s)"
    return f"{seconds:.0f}s"


def hist(values: list[float]) -> dict[str, int]:
    # Keep completed-but-slow (>500ms) distinct from did-not-complete sentinels
    # (value < 0), consistent with the latency tables: a sentinel is not a 500ms+
    # measurement.
    counts = {name: 0 for name, _, _ in HIST_BUCKETS}
    counts[">500ms"] = 0
    counts["did_not_complete"] = 0
    for value in values:
        if value < 0:
            counts["did_not_complete"] += 1
            continue
        if value > 500:
            counts[">500ms"] += 1
            continue
        placed = False
        for name, lo, hi in HIST_BUCKETS:
            if lo <= value <= hi:
                counts[name] += 1
                placed = True
                break
        if not placed:
            counts[">500ms"] += 1
    return counts


def source_index(event_idx: int, offset: int, stride: int, event_count: int) -> int:
    if stride <= 0:
        stride = 1
    return (offset + event_idx * stride) % event_count


def full_binding_key(event: dict[str, Any]) -> tuple[Any, ...]:
    bindings = event.get("bindings") or {}
    return tuple(bindings.get(field) for field in KEY_FIELDS)


def short_value(value: Any, limit: int = 36) -> str:
    if value is None:
        return ""
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def latency_table_row(label: str, values: list[float]) -> list[str]:
    """Build one latency table row with a consistent population.

    n / p50 / p95 / p99 / max cover only completed events (value >= 0); the
    >cutoff columns count only completed events over the cutoff; non-completions
    live in a separate did-not-complete column. A row can never claim a max
    smaller than its >500ms count implies.
    """
    summary = summarize(values)
    return [
        label,
        f"{summary['n']:,}",
        fmt_stat(summary, "p50"),
        fmt_stat(summary, "p95"),
        fmt_stat(summary, "p99"),
        fmt_stat(summary, "max"),
        f"{count_over(values, 300):,}",
        f"{count_over(values, 350):,}",
        f"{count_over(values, 500):,}",
        f"{summary['not_completed']:,}",
    ]


def sla_cells(rows: list[dict[str, Any]], threshold: int) -> list[str]:
    """For each cutoff, count events whose bundles_by_<cutoff>ms meets the threshold."""
    total = len(rows)
    cells = []
    for cutoff in CUTOFFS:
        count = sum(1 for row in rows if row[f"bundles_by_{int(cutoff)}ms"] >= threshold)
        pct = count / total * 100 if total else 0.0
        cells.append(f"{count:,}/{total:,} ({pct:.2f}%)")
    return cells


def cutoff_count(rows: list[dict[str, Any]], threshold: int, cutoff_ms: int) -> int:
    return sum(1 for row in rows if row.get(f"bundles_by_{cutoff_ms}ms", 0) >= threshold)


def count_pct(count: int, total: int) -> str:
    pct = count / total * 100 if total else 0.0
    return f"{count:,}/{total:,} ({pct:.2f}%)"


def yes_no(value: bool) -> str:
    return "yes" if value else "no"


def text_table(headers: list[str], rows: list[list[Any]], right_align: set[int] | None = None) -> list[str]:
    """Render a raw-Markdown-friendly fixed-width table.

    Markdown pipe tables render nicely in some tools, but are difficult to read
    in Slack, terminal previews, and raw .md views. A fenced text table keeps
    alignment stable everywhere.
    """
    right_align = right_align or set()
    str_rows = [[str(cell) for cell in row] for row in rows]
    widths = [
        max(len(str(headers[idx])), *(len(row[idx]) for row in str_rows)) if str_rows else len(str(headers[idx]))
        for idx in range(len(headers))
    ]

    def fmt_row(row: list[Any]) -> str:
        cells = []
        for idx, cell in enumerate(row):
            text = str(cell)
            cells.append(text.rjust(widths[idx]) if idx in right_align else text.ljust(widths[idx]))
        return "  ".join(cells).rstrip()

    lines = ["```text", fmt_row(headers), fmt_row(["-" * width for width in widths])]
    lines.extend(fmt_row(row) for row in str_rows)
    lines.append("```")
    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", required=True)
    ap.add_argument("--results", nargs="+", required=True)
    ap.add_argument("--output-md", required=True)
    ap.add_argument("--output-json", default=None)
    ap.add_argument(
        "--fleet-summary",
        default=None,
        help="Path to the fleet '<prefix>_summary.json' written by run_go_loadgen_fleet.py. "
        "When provided, the fleet-total run shape (target EPS, total connections, app processes, "
        "duration, execution mode) is taken from it instead of being derived from per-host results.",
    )
    args = ap.parse_args()

    workload_path = Path(args.workload)
    workload = load_json(workload_path)
    workload_events = workload.get("events", [])
    if not workload_events:
        raise ValueError("workload has no events")

    # Exclude the fleet summary file from the per-host merge. The checklist's
    # glob (<prefix>_*.json) also matches <prefix>_summary.json; counting it as a
    # host would inflate app_processes and (if sorted first) feed fleet-total
    # fields into the per-host base. fleet_summary==true marks it definitively.
    all_result_paths = [Path(path) for path in args.results]
    result_paths: list[Path] = []
    excluded_summary_paths: list[Path] = []
    for path in all_result_paths:
        if path.name.endswith("_summary.json"):
            excluded_summary_paths.append(path)
            continue
        result_paths.append(path)
    results = []
    for path in result_paths:
        data = load_json(path)
        if isinstance(data, dict) and data.get("fleet_summary"):
            # Defensive: a fleet summary that did not match the name filter.
            excluded_summary_paths.append(path)
            continue
        results.append(data)
    if not results:
        raise ValueError(
            "no per-host result files found (after excluding fleet '_summary.json'). "
            "Pass the per-host result JSONs via --results."
        )

    # Per-result-file cutoff availability. bundles_by_*ms are recorded only by
    # fanout modes; worker-pool leaves them at zero. Deciding this per file (not
    # from a single host's execution_mode) prevents counting worker-pool events
    # (bundles_by_*=0) as hard SLA misses when a fleet mixes host modes.
    CUTOFF_MODES = {"event-fanout", "conn-fanout"}

    def _result_has_cutoffs(result: dict[str, Any]) -> bool:
        return str(result.get("execution_mode") or "") in CUTOFF_MODES

    event_records: list[dict[str, Any]] = []
    executed_source_indexes: list[int] = []
    mapping_warnings: list[str] = []
    for result_idx, result in enumerate(results):
        # Do NOT guess offset/stride from the file position: a wrong guess
        # silently maps events to the wrong workload rows, fabricating bindings /
        # hot-keys / realism stats. The Go loadgen always emits event_offset /
        # event_stride; if a result lacks them, fall back to the single-process
        # defaults (0, 1) and warn loudly in the report rather than inventing a
        # multi-file stride.
        if "event_offset" in result and "event_stride" in result:
            offset = int(result["event_offset"])
            stride = int(result["event_stride"])
        else:
            offset = 0
            stride = 1
            mapping_warnings.append(
                f"result #{result_idx} missing event_offset/event_stride; "
                "assumed single-process mapping (offset=0, stride=1) -- "
                "binding/realism stats for this host may be misattributed."
            )
        result_has_cutoffs = _result_has_cutoffs(result)
        for event_result in result.get("event_results", []):
            src_idx = source_index(int(event_result["event_idx"]), offset, stride, len(workload_events))
            executed_source_indexes.append(src_idx)
            source = workload_events[src_idx]
            event_records.append(
                {
                    "source_index": src_idx,
                    "has_cutoffs": result_has_cutoffs,
                    "source_event": source.get("event"),
                    "kind": source.get("kind") or "normal",
                    "hot_field": source.get("hot_field"),
                    "bindings": source.get("bindings") or {},
                    "score60_ms": float(event_result.get("score60_ms", -1)),
                    "full65_ms": float(event_result.get("full65_ms", -1)),
                    "sql_score60_ms": float(event_result.get("sql_score60_ms", -1)),
                    "sql_full65_ms": float(event_result.get("sql_full65_ms", -1)),
                    "bundles_by_300ms": int(event_result.get("bundles_by_300ms", 0)),
                    "bundles_by_350ms": int(event_result.get("bundles_by_350ms", 0)),
                    "bundles_by_500ms": int(event_result.get("bundles_by_500ms", 0)),
                    "successes": int(event_result.get("successes", 0)),
                    "errors": int(event_result.get("errors", 0)),
                    "skipped": int(event_result.get("skipped", 0)),
                }
            )

    if not event_records:
        raise ValueError(
            "no event_results found in the result files. Re-run go-loadgen with "
            "--omit-event-results=false so per-event records are retained."
        )

    completed_events = len(event_records)
    normal_rows = [r for r in event_records if r["kind"] == "normal"]
    hot_rows = [r for r in event_records if r["kind"] != "normal"]

    # Require a positive SQL latency: worker-pool mode can emit 0 (unset) without omitempty,
    # and a real DB round-trip is always > 0, so this avoids a fake all-zero SQL-only section.
    have_sql = any(r["sql_full65_ms"] > 0 for r in event_records)
    event_mix = Counter(row["kind"] for row in event_records)

    generated_source_events = [event.get("event") for event in workload_events]
    generated_binding_keys = [full_binding_key(event) for event in workload_events]
    executed_source_events = [row["source_event"] for row in event_records]
    executed_binding_keys = [tuple(row["bindings"].get(field) for field in KEY_FIELDS) for row in event_records]

    field_stats = {}
    for field in KEY_FIELDS:
        values = [row["bindings"].get(field) for row in event_records]
        counts = Counter(values)
        field_stats[field] = {
            "distinct": len(counts),
            "max_repeat": max(counts.values(), default=0),
        }

    # Average bundles is meaningful only for cutoff-bearing hosts (fanout modes);
    # worker-pool events carry bundles_by_*=0 and would drag the mean toward zero.
    # Restrict to the same cutoff-bearing population used by the SLA tables.
    _avg_bundles_pop = [row for row in event_records if row.get("has_cutoffs")]
    avg_bundles = {
        300.0: mean(row["bundles_by_300ms"] for row in _avg_bundles_pop) if _avg_bundles_pop else 0,
        350.0: mean(row["bundles_by_350ms"] for row in _avg_bundles_pop) if _avg_bundles_pop else 0,
        500.0: mean(row["bundles_by_500ms"] for row in _avg_bundles_pop) if _avg_bundles_pop else 0,
    }

    bundle_rollup: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"n": 0, "p95": 0, "p99": 0, "max": 0, "over_300": 0, "over_350": 0, "over_500": 0}
    )
    for result in results:
        for bundle_id, summary in (result.get("bundle_summaries") or {}).items():
            row = bundle_rollup[bundle_id]
            row["n"] += int(summary.get("n", 0))
            row["p95"] = max(row["p95"], float(summary.get("p95", 0)))
            row["p99"] = max(row["p99"], float(summary.get("p99", 0)))
            row["max"] = max(row["max"], float(summary.get("max", 0)))
            row["over_300"] += int(summary.get("over_300", 0))
            row["over_350"] += int(summary.get("over_350", 0))
            row["over_500"] += int(summary.get("over_500", 0))
    tail_rows = sorted(
        bundle_rollup.items(),
        key=lambda item: (item[1]["over_500"], item[1]["over_300"], item[1]["p99"]),
        reverse=True,
    )[:20]

    generated_workload_stats = workload.get("workload_stats") or {}
    hot_key_profile = workload.get("hot_key_profile") or {}

    base = results[0] if results else {}

    def _num(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # --- Fleet-wide run shape ---
    # The fleet runner divides the fleet target across worker processes and
    # passes the PER-PROCESS share (target_event_eps / connections) to each Go
    # process, which writes that per-process value verbatim into its result JSON.
    # Reading results[0] verbatim therefore understates the fleet target/conns by
    # the process count. The authoritative source is the fleet summary's
    # run_shape (fleet totals); absent that, reconstruct fleet totals by SUMMing
    # the per-host rates/connections (they ran concurrently) and MAXing elapsed.
    fleet_summary: dict[str, Any] | None = None
    if args.fleet_summary:
        fs_path = Path(args.fleet_summary)
        try:
            loaded = load_json(fs_path)
            if isinstance(loaded, dict) and loaded.get("run_shape"):
                fleet_summary = loaded
        except (OSError, ValueError):
            fleet_summary = None

    # Actual wall-clock elapsed is the MAX across concurrently-running hosts.
    elapsed_values = [v for v in (_num(r.get("elapsed_seconds")) for r in results) if v and v > 0]
    actual_elapsed = max(elapsed_values) if elapsed_values else None
    # fmt_duration() renders elapsed rounded to whole seconds, but achieved
    # throughput must divide by the SAME value the report shows or the two will
    # not reconcile (e.g. 606s displayed vs 605.7s used). Round once here and use
    # this single value for BOTH display and the throughput denominator. (Overrun
    # detection keeps the raw value -- a >10% window check is insensitive to
    # sub-second rounding.)
    elapsed_display_seconds = round(actual_elapsed) if actual_elapsed else None

    if fleet_summary is not None:
        fs_shape = fleet_summary.get("run_shape") or {}
        run_shape = {
            "target_event_eps": fs_shape.get("target_event_eps"),
            "duration_seconds": fs_shape.get("duration_seconds"),
            "total_connections": fs_shape.get("total_connections"),
            "app_processes": fs_shape.get("app_processes") or len(results),
            "execution_mode": fs_shape.get("execution_mode") or base.get("execution_mode"),
            "max_pending_events_total": fs_shape.get("max_pending_events_total"),
            "prepare_all": base.get("prepare_all"),
            "max_execution_time_ms": base.get("max_execution_time_ms"),
            "run_shape_source": "fleet_summary",
        }
    else:
        target_sum = sum(
            v for v in (_num(r.get("target_event_eps")) for r in results) if v is not None
        )
        conn_sum = sum(
            int(r.get("connections") or r.get("total_connections") or 0) for r in results
        )
        duration_vals = [
            v for v in (_num(r.get("duration_seconds")) for r in results) if v is not None
        ]
        run_shape = {
            # SUM concurrent per-host rates/connections to recover fleet totals.
            "target_event_eps": target_sum if target_sum else None,
            "duration_seconds": max(duration_vals) if duration_vals else None,
            "total_connections": conn_sum or None,
            "app_processes": len(results),
            "prepare_all": base.get("prepare_all"),
            "max_execution_time_ms": base.get("max_execution_time_ms"),
            "execution_mode": base.get("execution_mode"),
            "run_shape_source": "derived_from_per_host_results",
        }

    # Connections that actually established across the fleet (vs configured).
    ready_workers_total = sum(int(r.get("ready_workers") or 0) for r in results)
    configured_connections = run_shape.get("total_connections")

    # Per-bundle cutoff counts (bundles_by_*ms) are recorded in event-fanout /
    # conn-fanout. worker-pool leaves them at zero, which would render a fake
    # all-zero SLA table. Use execution_mode instead of "any count > 0" so a bad
    # fanout run with legitimately zero bundles by 500ms still reports honestly.
    execution_mode = str(run_shape.get("execution_mode") or results[0].get("execution_mode") or "")
    # Decide cutoff-availability PER RESULT FILE, not from one host's mode. A fleet
    # mixing fanout hosts (bundles_by_*>0) with worker-pool hosts (bundles_by_*=0)
    # must not count the worker-pool events as hard SLA misses. The SLA / avg-bundles
    # tables are therefore computed ONLY over events from cutoff-bearing hosts.
    cutoff_event_records = [r for r in event_records if r.get("has_cutoffs")]
    n_cutoff_results = sum(1 for r in results if _result_has_cutoffs(r))
    n_noncutoff_results = len(results) - n_cutoff_results
    have_cutoffs = bool(cutoff_event_records)
    # Hosts mix cutoff-bearing and non-cutoff modes: the SLA denominator is
    # restricted to cutoff-bearing hosts and we warn loudly so the share is never
    # read as fleet-wide.
    cutoff_hosts_mixed = have_cutoffs and n_noncutoff_results > 0
    cutoff_excluded_events = len(event_records) - len(cutoff_event_records)

    templates = workload.get("templates") or []
    mode_counts = Counter((template.get("mode") or "unknown") for template in templates)
    group_mode_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for template in templates:
        group_mode_counts[str(template.get("group") or "unknown")][str(template.get("mode") or "unknown")] += 1

    sample_reused = "yes" if completed_events > len(set(executed_source_events)) else "no"
    workload_cycled = "yes" if completed_events > len(workload_events) else "no"

    target_eps = run_shape.get("target_event_eps")
    duration_seconds = run_shape.get("duration_seconds")
    # Achieved throughput MUST use actual wall-clock elapsed, not the configured
    # duration. In steady mode the collector waits for ALL events, so dividing by
    # the configured window always yields ~target by construction even when the
    # run overran. Fall back to configured duration only if no host reported
    # elapsed_seconds.
    throughput_denominator = elapsed_display_seconds if elapsed_display_seconds else (
        float(duration_seconds) if duration_seconds else None
    )
    achieved_eps = completed_events / throughput_denominator if throughput_denominator else None

    # Overrun: actual elapsed materially exceeds the configured window => the
    # cluster did not actually sustain the target rate, even if all events drained.
    elapsed_overrun = bool(
        actual_elapsed and duration_seconds and actual_elapsed > float(duration_seconds) * 1.10
    )

    def _fmt_eps(value: Any) -> str:
        v = _num(value)
        if v is None:
            return "unknown"
        return f"{round(v):,}"

    title = "Event-Level Benchmark Report"
    if target_eps:
        title = f"{_fmt_eps(target_eps)} EPS Event-Level Benchmark Report"

    # Expected events uses the configured duration (the intended count to drain).
    expected_events = (
        int(round(float(target_eps) * float(duration_seconds)))
        if target_eps and duration_seconds
        else 0
    )

    # --- Errors & completion accounting ---
    total_event_errors = sum(int(r.get("errors") or 0) for r in event_records)
    total_top_errors = sum(int(res.get("total_errors") or 0) for res in results)
    total_errors = max(total_event_errors, total_top_errors)
    total_skipped = sum(int(r.get("skipped") or 0) for r in event_records)
    events_with_skips = sum(1 for r in event_records if int(r.get("skipped") or 0) > 0)
    avg_skipped_per_event = (total_skipped / completed_events) if completed_events else 0.0
    incomplete_events = sum(1 for r in event_records if r["successes"] < 65)
    first_query_errors: list[str] = []
    for res in results:
        for msg in (res.get("first_query_errors") or []):
            if msg not in first_query_errors:
                first_query_errors.append(str(msg))
    setup_errors: list[str] = []
    for res in results:
        for msg in (res.get("setup_errors") or []):
            if msg not in setup_errors:
                setup_errors.append(str(msg))

    # Distinguish deadline-cut bundles (aborted by the configured query cap -- an
    # EXPECTED SLA miss, exactly how a hard-deadline service like Intuit's behaves)
    # from genuine failures (connection drops, OOM, lock timeouts). A deadline cut is
    # a miss, not a failure, and must NOT flag the run DEGRADED.
    cap_ms = _num(run_shape.get("max_execution_time_ms")) or 0
    _qto = str(base.get("query_timeout") or "0s").strip()
    cap_on = bool(cap_ms and cap_ms > 0) or (_qto not in ("0s", "0", ""))
    DEADLINE_SIGNATURES = (
        "maximum statement execution time exceeded",  # TiDB max_execution_time (err 3024)
        "context deadline exceeded",
        "context canceled",
    )

    def _is_deadline_cut(msg: str) -> bool:
        m = str(msg).lower()
        return any(sig in m for sig in DEADLINE_SIGNATURES)

    nondeadline_sampled = [m for m in first_query_errors if not _is_deadline_cut(m)]
    # If the cap is on and every sampled error message is a deadline cut, attribute the
    # error bucket to deadline cuts (counted as SLA misses, not failures). If any
    # non-deadline signature appears, genuine failures are present -> keep them as
    # errors and degrade the run. A precise per-error split would require the Go loadgen
    # to emit a deadline-cut count (see GO_HANDOFF_JINLONG.md); from a sampled message
    # list we stay conservative.
    if total_errors > 0 and cap_on and first_query_errors and not nondeadline_sampled:
        deadline_cuts = total_errors
        genuine_errors = 0
    elif total_errors > 0 and cap_on and nondeadline_sampled:
        deadline_cuts = 0
        genuine_errors = total_errors
    else:
        deadline_cuts = 0
        genuine_errors = total_errors  # cap off, or zero errors

    connections_short = bool(
        configured_connections and ready_workers_total < int(configured_connections)
    )
    ERROR_THRESHOLD = 0  # any GENUINE (non-deadline-cut) query error degrades a customer benchmark
    errors_degraded = genuine_errors > ERROR_THRESHOLD

    # Lost hosts: fleet summary declares more app processes than result files merged.
    hosts_missing = False
    if fleet_summary is not None:
        _fs_procs = _num((fleet_summary.get("run_shape") or {}).get("app_processes"))
        hosts_missing = _fs_procs is not None and len(results) < int(_fs_procs)

    # A run is degraded (not a clean customer result) when it overran the target
    # window, lost connections/hosts, or had query errors -- not merely when it
    # was killed early. The "completed == expected" identity can no longer mask a
    # slow-but-completed run.
    degraded = elapsed_overrun or errors_degraded or connections_short or hosts_missing
    is_smoke = (
        completed_events < 1000
        or bool(expected_events and completed_events < expected_events * 0.9)
    )
    if is_smoke:
        run_type = "Smoke / format-validation run"
    elif degraded:
        run_type = "DEGRADED run (NOT a clean customer result)"
    else:
        run_type = "Customer benchmark run"
    # SLA counts and their denominator are restricted to cutoff-bearing hosts so
    # worker-pool events (bundles_by_*=0) are not silently counted as hard misses.
    cutoff_completed_events = len(cutoff_event_records)
    score300 = cutoff_count(cutoff_event_records, 60, 300) if have_cutoffs else 0
    score350 = cutoff_count(cutoff_event_records, 60, 350) if have_cutoffs else 0
    score500 = cutoff_count(cutoff_event_records, 60, 500) if have_cutoffs else 0
    full300 = cutoff_count(cutoff_event_records, 65, 300) if have_cutoffs else 0
    full350 = cutoff_count(cutoff_event_records, 65, 350) if have_cutoffs else 0
    full500 = cutoff_count(cutoff_event_records, 65, 500) if have_cutoffs else 0
    score_summary = summarize([r["score60_ms"] for r in event_records])
    full_summary = summarize([r["full65_ms"] for r in event_records])
    real_hot_fields = [
        field
        for field in KEY_FIELDS
        if int((hot_key_profile.get(field) or {}).get("count") or 0) > 0
    ]
    no_hot_fields = [field for field in KEY_FIELDS if field not in real_hot_fields]

    # ---- JSON output (machine-readable) ----
    report_json = {
        "run_type": run_type,
        "degraded": degraded,
        "completed_events": completed_events,
        "target_event_eps": target_eps,
        "total_connections": configured_connections,
        "connections_established": ready_workers_total,
        "app_processes": run_shape.get("app_processes"),
        "execution_mode": execution_mode or None,
        "configured_duration_seconds": duration_seconds,
        "actual_elapsed_seconds": actual_elapsed,
        "elapsed_overrun": elapsed_overrun,
        "achieved_eps": achieved_eps,
        "errors": {
            "total_errors": total_errors,
            "deadline_cuts": deadline_cuts,
            "genuine_errors": genuine_errors,
            "incomplete_events": incomplete_events,
            "skipped_bundles": total_skipped,
            "first_query_errors_sample": first_query_errors[:5],
            "setup_errors_sample": setup_errors[:5],
        },
        "wall_clock": {
            "score_ready_60_of_65": {
                "all": summarize([r["score60_ms"] for r in event_records]),
                "normal": summarize([r["score60_ms"] for r in normal_rows]),
                "hot_key": summarize([r["score60_ms"] for r in hot_rows]),
            },
            "full_65_of_65": {
                "all": summarize([r["full65_ms"] for r in event_records]),
                "normal": summarize([r["full65_ms"] for r in normal_rows]),
                "hot_key": summarize([r["full65_ms"] for r in hot_rows]),
            },
        },
        "sql_only": {
            "score_ready_60_of_65": summarize([r["sql_score60_ms"] for r in event_records]) if have_sql else None,
            "full_65_of_65": summarize([r["sql_full65_ms"] for r in event_records]) if have_sql else None,
        },
        # Gate on have_cutoffs (mirrors sql_only gating on have_sql): worker-pool
        # mode never records bundles_by_*, so emitting zeros would read as "0
        # bundles returned by every cutoff" -- a catastrophic real result -- when
        # it was simply not measured.
        "avg_bundles_returned": (
            {str(int(c)): avg_bundles[c] for c in CUTOFFS} if have_cutoffs else None
        ),
        "field_stats": field_stats,
        "event_mix": dict(event_mix),
    }
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(report_json, indent=2), encoding="utf-8")

    # ---- Markdown report ----
    lines = [
        f"# {title}",
        "",
        "## Executive Readout",
        "",
    ]
    def latency_line(label: str, summary: dict[str, Any], returned_desc: str = "returned all 65") -> str:
        pop = f" (over {summary['n']:,} of {summary['total']:,} events that {returned_desc})"
        if summary["n"] == 0:
            return f"- {label}: no events completed (0/{summary['total']:,})"
        return (
            f"- {label}: "
            f"p50 {fmt_ms(summary['p50'])}, p95 {fmt_ms(summary['p95'])}, "
            f"p99 {fmt_ms(summary['p99'])}, max {fmt_ms(summary['max'])}{pop}"
        )

    lines.append(f"- Run type: {run_type}")
    if is_smoke:
        lines.append(
            "- Important: this is a small validation run for report shape and script correctness, not the final customer performance result."
        )
    if degraded and not is_smoke:
        reasons = []
        if elapsed_overrun:
            reasons.append(
                f"actual elapsed {fmt_duration(elapsed_display_seconds)} exceeded the configured "
                f"window {fmt_duration(duration_seconds)} by >10% (target rate NOT sustained)"
            )
        if errors_degraded:
            reasons.append(f"{genuine_errors:,} query errors")
        if connections_short:
            reasons.append(
                f"only {ready_workers_total:,}/{int(configured_connections):,} connections established"
            )
        lines.append("- **DEGRADED / OVERRUN: " + "; ".join(reasons) + "**")
    if target_eps:
        lines.append(
            f"- Target load: {_fmt_eps(target_eps)} events/sec "
            f"({round(_num(target_eps) * 65):,} bundled SQL executions/sec)"
        )
    if duration_seconds:
        lines.append(f"- Configured duration: {fmt_duration(duration_seconds)}")
    if actual_elapsed:
        lines.append(f"- Actual elapsed: {fmt_duration(elapsed_display_seconds)}")
    lines.extend(
        [
            f"- Completed events: {completed_events:,}",
            f"- Achieved event throughput: {achieved_eps:.1f} events/sec (completed events / actual elapsed)" if achieved_eps else "- Achieved event throughput: unknown",
            f"- Primary event metric: all 65/65 bundles by 300ms = {count_pct(full300, cutoff_completed_events) if have_cutoffs else 'not recorded'}",
            f"- Score-ready fallback: at least 60/65 bundles by 300ms = {count_pct(score300, cutoff_completed_events) if have_cutoffs else 'not recorded'}",
            latency_line("Full-event wall-clock latency", full_summary, "returned all 65"),
            latency_line("Score-ready wall-clock latency", score_summary, "returned at least 60 (score-ready)"),
            f"- Event replay check: source sample reused = {sample_reused}; workload rows cycled = {workload_cycled}",
            (
                "- Unique executed binding sets: "
                f"{len(set(executed_binding_keys)):,}/{completed_events:,} full 8-field binding sets"
            ),
            (
                "- Query design: "
                f"{mode_counts.get('runtime', 0)} runtime bundles against base tables, "
                f"{mode_counts.get('serving', 0)} 180d serving bundles, "
                f"{mode_counts.get('preagg', 0)} daily pre-agg bundles"
            ),
        ]
    )
    if real_hot_fields:
        lines.append("- Hot-key traffic uses real hot fields: " + ", ".join(real_hot_fields))
    if no_hot_fields:
        lines.append("- No artificial hot keys injected for high-cardinality fields: " + ", ".join(no_hot_fields))

    lines.extend(
        [
            "",
            "### SLA Snapshot",
            "",
        ]
    )
    if have_cutoffs:
        lines.extend(
            text_table(
                ["Event completion view", "<=300ms", "<=350ms", "<=500ms"],
                [
                    [
                        "Score-ready >=60/65",
                        count_pct(score300, cutoff_completed_events),
                        count_pct(score350, cutoff_completed_events),
                        count_pct(score500, cutoff_completed_events),
                    ],
                    [
                        "Full event 65/65",
                        count_pct(full300, cutoff_completed_events),
                        count_pct(full350, cutoff_completed_events),
                        count_pct(full500, cutoff_completed_events),
                    ],
                ],
                right_align={1, 2, 3},
            )
        )
    else:
        lines.extend(
            text_table(
                ["Event completion view", "<=300ms", "<=350ms", "<=500ms"],
                [
                    ["Score-ready >=60/65", "not recorded", "not recorded", "not recorded"],
                    ["Full event 65/65", "not recorded", "not recorded", "not recorded"],
                ],
            )
        )
    if cutoff_hosts_mixed:
        lines.extend(
            [
                "",
                f"- **WARNING: this fleet MIXES cutoff-bearing hosts ({n_cutoff_results} fanout result file(s)) "
                f"with non-cutoff hosts ({n_noncutoff_results} worker-pool result file(s)). The SLA shares above "
                f"are computed ONLY over the {cutoff_completed_events:,} events from cutoff-bearing hosts; "
                f"{cutoff_excluded_events:,} events from worker-pool hosts are EXCLUDED (they record no per-bundle "
                "cutoffs, so including them would count them as hard SLA misses). These shares are NOT fleet-wide.**",
            ]
        )

    # ---- Errors & Completion ----
    lines.extend(
        [
            "",
            "### Errors & Completion",
            "",
        ]
    )
    _cap_label = f"{int(cap_ms)}ms" if cap_ms else "the configured deadline"
    if deadline_cuts > 0:
        lines.append(
            f"- Bundles cut at {_cap_label}: {deadline_cuts:,} (counted as SLA misses, NOT failures -- "
            "the expected, production-faithful behavior of a hard-deadline cap: the bundle exceeded the "
            "deadline and was aborted server-side, exactly like the customer's own service would)."
        )
        lines.append(f"- Genuine query errors (unexpected failures): {genuine_errors:,}")
    else:
        lines.append(f"- Total query errors: {total_errors:,}")
    lines.extend(
        [
            f"- Events that did NOT reach all 65 successes: {incomplete_events:,}/{completed_events:,}",
            f"- Skipped bundles (null-binding, not executed against TiDB): {total_skipped:,}",
        ]
    )
    if total_skipped > 0:
        lines.append(
            "- **IMPORTANT: the 60/65 and 65/65 SLA denominators COUNT skipped (null-binding, "
            f"non-executed) bundles as if they returned. This run had {total_skipped:,} skipped bundles "
            f"across {events_with_skips:,} of {completed_events:,} events (avg {avg_skipped_per_event:.2f} "
            "skipped/event). An event can therefore reach 60/65 (or even 65/65) with FEWER than 60 (or 65) "
            "real queries against TiDB. Do NOT read '65/65' as 65 executed queries.**"
        )
    lines.extend(
        [
            (
                f"- Connections established (ready_workers): {ready_workers_total:,}"
                + (
                    f" / {int(configured_connections):,} configured"
                    if configured_connections
                    else " (configured count unknown)"
                )
            ),
        ]
    )
    if connections_short:
        lines.append(
            "- **WARNING: fewer connections established than configured -- the run used reduced "
            "concurrency and the measured latencies reflect that.**"
        )
    if first_query_errors:
        lines.append("- Sample query errors:")
        for msg in first_query_errors[:5]:
            lines.append(f"  - {short_value(msg, 160)}")
    if setup_errors:
        lines.append("- Sample setup errors:")
        for msg in setup_errors[:5]:
            lines.append(f"  - {short_value(msg, 160)}")
    if genuine_errors == 0 and deadline_cuts == 0 and not connections_short and incomplete_events == 0:
        lines.append("- No query errors, no incomplete events, all configured connections established.")
    elif genuine_errors == 0 and not connections_short and not hosts_missing and not elapsed_overrun:
        lines.append(
            "- No genuine failures and no connection/host/overrun issues; the only misses are bundles "
            "cut at the deadline (expected). This is a clean run."
        )
    # Host-completeness: if the fleet summary declares more app processes than we
    # merged result files for, hosts were lost and the fleet ran short.
    if fleet_summary is not None:
        fs_procs = _num((fleet_summary.get("run_shape") or {}).get("app_processes"))
        if fs_procs is not None and len(results) < int(fs_procs):
            lines.append(
                f"- **WARNING: fleet summary expected {int(fs_procs):,} app processes but only "
                f"{len(results):,} per-host result files were merged -- hosts were lost; "
                "throughput/SLA are computed over an incomplete fleet.**"
            )
    if mapping_warnings:
        lines.append("")
        lines.append("- **Event-to-workload mapping warnings:**")
        for w in mapping_warnings:
            lines.append(f"  - {w}")

    lines.extend(
        [
            "",
            "### How To Read This",
            "",
            "- Event-level wall-clock latency is the customer-facing metric: timer starts before the 65 bundle queries fan out and stops when the 60th or 65th result returns.",
            "- SQL-only latency is included only for diagnosis; it excludes client fan-out/fan-in overhead.",
            "- Individual key values can repeat naturally because production has hot keys; the important realism check is that full 8-field binding sets are not replayed or cycled.",
            "",
            "## Run Details",
            "",
            f"- Result JSON files merged: {len(results)}",
            "- Latency scope: benchmark-harness event wall-clock time.",
            "  - Timer starts immediately before the 65 bundle queries fan out for an event.",
            "  - Score-ready latency stops when the 60th bundle result is received.",
            "  - Full-event latency stops when the 65th bundle result is received.",
        ]
    )
    if target_eps:
        lines.append(f"- Target event throughput: {_fmt_eps(target_eps)} events/sec")
    if achieved_eps:
        lines.append(
            f"- Achieved event throughput: {achieved_eps:.1f} events/sec "
            "(completed events / actual elapsed)"
        )
    if duration_seconds:
        lines.append(f"- Configured duration: {fmt_duration(duration_seconds)}")
    if actual_elapsed:
        lines.append(f"- Actual elapsed: {fmt_duration(elapsed_display_seconds)}")
        if elapsed_overrun:
            lines.append(
                "- **Sustained-rate miss: actual elapsed exceeded the configured window by >10%.**"
            )
    lines.append(f"- Run shape source: {run_shape.get('run_shape_source', 'unknown')}")
    if have_sql:
        lines.append("- SQL-only latency (DB-side query execution, excludes fan-out/fan-in) is reported alongside for comparison.")
    else:
        lines.append("- SQL-only latency not present in this run's output.")

    lines.extend(["", "## Test Shape", ""])
    if target_eps:
        lines.append(f"- Target event EPS (fleet total): {_fmt_eps(target_eps)}")
    if duration_seconds:
        lines.append(f"- Configured duration: {fmt_duration(duration_seconds)}")
    if actual_elapsed:
        lines.append(f"- Actual elapsed: {fmt_duration(elapsed_display_seconds)}")
    if configured_connections:
        lines.append(
            f"- Total connections (fleet, configured): {int(configured_connections):,}"
        )
        lines.append(
            f"- Connections established (ready_workers): {ready_workers_total:,}"
        )
    if run_shape.get("app_processes"):
        lines.append(f"- Fleet app processes: {int(run_shape['app_processes'])}")
    if run_shape.get("execution_mode"):
        lines.append(f"- Execution mode: {run_shape.get('execution_mode')}")
    if "prepare_all" in run_shape:
        lines.append(f"- Prepared statements: {run_shape.get('prepare_all')}")
    if run_shape.get("max_execution_time_ms") is not None:
        lines.append(f"- Max execution time (ms): {run_shape.get('max_execution_time_ms')}")
    lines.extend([
        f"- Bundles per event: {workload.get('bundle_count', len(templates) or 'unknown')}",
        f"- Workload mode: {workload.get('mode', 'unknown')}",
        f"- Runtime window params: {workload.get('runtime_window_params', 'unknown')}",
        f"- Serving layout: {workload.get('serving_layout', 'unknown')}",
        f"- Serving table: {workload.get('serving_table', 'unknown')}",
    ])
    # Frozen-window footgun: with runtime_window_params off, every event's
    # 1d/7d/30d/90d runtime bundles query the SAME literal time window baked from
    # the first event, inflating cache/coprocessor reuse and understating latency.
    if workload.get("runtime_window_params") is False and not is_smoke:
        lines.append(
            "- **WARNING: runtime window params are FROZEN (runtime_window_params=False). "
            "Every event's runtime bundles query the same time window baked from the first "
            "event, which inflates cache reuse and understates the live-path latency. "
            "Re-generate with --runtime-window-params for a representative customer run.**"
        )
    lines.extend([
        "",
        "## Binding Reuse / Test Realism",
        "",
        f"- Generated workload rows: {int(generated_workload_stats.get('generated_workload_rows') or len(workload_events)):,}",
        f"- Unique source events generated: {int(generated_workload_stats.get('unique_source_events') or len(set(generated_source_events))):,}",
        f"- Unique full 8-field binding sets generated: {int(generated_workload_stats.get('unique_full_binding_sets') or len(set(generated_binding_keys))):,}",
        f"- Unique source events executed: {len(set(executed_source_events)):,}",
        f"- Unique full 8-field binding sets executed: {len(set(executed_binding_keys)):,}",
        f"- Max workload row repeat in executed run: {max(Counter(executed_source_indexes).values(), default=0):,}",
        f"- Max source event repeat in executed run: {max(Counter(executed_source_events).values(), default=0):,}",
        f"- Source event sample reused: {sample_reused}",
        f"- Workload rows cycled during run: {workload_cycled}",
        "",
        "Individual field values repeat naturally (hot-key behavior); the full 8-field binding set is what should stay unique.",
        "",
    ])
    field_rows = []
    for field, stats in field_stats.items():
        field_rows.append([field, f"{stats['distinct']:,}", f"{stats['max_repeat']:,}"])
    lines.extend(text_table(["Field", "Distinct values executed", "Max repeat"], field_rows, right_align={1, 2}))

    lines.extend(["", "## Event Mix", ""])
    for key in HOT_EVENT_KINDS:
        lines.append(f"- {key}: {event_mix.get(key, 0):,}")
    for key, value in sorted(event_mix.items()):
        if key not in HOT_EVENT_KINDS:
            lines.append(f"- {key}: {value:,}")

    lines.extend([
        "",
        "## Runtime vs Pre-Agg / Serving Bundle Counts",
        "",
    ])
    mode_rows = []
    for group in ["A", "B", "C"]:
        counts = group_mode_counts.get(group, Counter())
        runtime_n = counts.get("runtime", 0)
        preagg_n = counts.get("preagg", 0)
        serving_n = counts.get("serving", 0)
        mode_rows.append([f"Group {group}", runtime_n, preagg_n, serving_n, runtime_n + preagg_n + serving_n])
    mode_rows.append([ "Total", mode_counts.get("runtime", 0), mode_counts.get("preagg", 0), mode_counts.get("serving", 0), len(templates)])
    lines.extend(
        text_table(
            ["Group", "Runtime", "Daily pre-agg", "Serving", "Total"],
            mode_rows,
            right_align={1, 2, 3, 4},
        )
    )

    lines.extend([
        "",
        "## Hot-Key Values Used",
        "",
    ])
    if hot_key_profile:
        hot_rows_table = []
        for field in KEY_FIELDS:
            profile = hot_key_profile.get(field) or {}
            hot_rows_table.append(
                [field, profile.get("source", ""), short_value(profile.get("value"), 48), f"{int(profile.get('count') or 0):,}"]
            )
        lines.extend(text_table(["Field", "Source", "Hot value", "Count"], hot_rows_table, right_align={3}))
        lines.append("")
        lines.append(
            "Count meaning depends on Source: a SHOW STATS_TOPN source is a table-wide "
            "Top-N frequency; any recent-window/fallback source is a frequency within the "
            "recent sample only, not a table-wide row count."
        )
    else:
        lines.append("- Hot-key profile not present in workload JSON.")

    # ---- Event-Level SLA: All / Normal / Hot-key, by 300/350/500 ----
    lines.extend([
        "",
        "## Event-Level SLA",
        "",
        "Share of events with at least 60/65 (score-ready) or all 65/65 bundles returned by each cutoff.",
        "",
    ])
    if total_skipped > 0:
        lines.extend(
            [
                "- **IMPORTANT: the 65-bundle denominator includes "
                f"{total_skipped:,} skipped (null-binding, non-executed) bundles across "
                f"{events_with_skips:,} of {completed_events:,} events (avg {avg_skipped_per_event:.2f} "
                "skipped/event). The /65 in these views is NOT 65 executed queries: an event can reach "
                "60/65 (or 65/65) with fewer than 60 (or 65) real queries against TiDB.**",
                "",
            ]
        )
    if not have_cutoffs:
        lines.append(
            "_Per-bundle cutoff counts were not recorded in this run (worker-pool mode). "
            "Run in event-fanout or conn-fanout mode to populate this table._"
        )
    else:
        # Restrict every scope to cutoff-bearing hosts (see SLA Snapshot): a fleet
        # mixing fanout + worker-pool hosts would otherwise count worker-pool events
        # (bundles_by_*=0) as hard misses.
        cutoff_normal_rows = [r for r in cutoff_event_records if r["kind"] == "normal"]
        cutoff_hot_rows = [r for r in cutoff_event_records if r["kind"] != "normal"]
        if cutoff_hosts_mixed:
            lines.append(
                f"_Fleet MIXES host modes: shares below cover ONLY the {cutoff_completed_events:,} events from "
                f"{n_cutoff_results} cutoff-bearing (fanout) result file(s); {cutoff_excluded_events:,} worker-pool "
                "events are EXCLUDED. These shares are NOT fleet-wide._"
            )
            lines.append("")
        lines.extend([
            "```text",
        ])
        sla_rows = []
        for view_label, threshold in [("Score-ready >=60/65", 60), ("Full 65/65", 65)]:
            for scope_label, rows in [("All", cutoff_event_records), ("Normal", cutoff_normal_rows), ("Hot-key", cutoff_hot_rows)]:
                if not rows:
                    continue
                sla_rows.append([view_label, scope_label, *sla_cells(rows, threshold)])
        lines.pop()
        lines.extend(text_table(["View", "Scope", "<=300ms", "<=350ms", "<=500ms"], sla_rows, right_align={2, 3, 4}))

    # ---- Event Latency: wall-clock (All/Normal/Hot), then SQL-only ----
    lines.extend([
        "",
        "## Event Latency (wall-clock)",
        "",
    ])
    lines.append(
        "n / p50 / p95 / p99 / max cover only events that returned that tier; "
        ">cutoff columns count only completed events over the cutoff; "
        "non-completions are in the separate did-not-complete column."
    )
    lines.append("")
    latency_rows = []
    for scope_label, rows in [("All", event_records), ("Normal", normal_rows), ("Hot-key", hot_rows)]:
        if rows:
            latency_rows.append(
                latency_table_row(f"Score-ready 60/65 ({scope_label})", [r["score60_ms"] for r in rows])
            )
    for scope_label, rows in [("All", event_records), ("Normal", normal_rows), ("Hot-key", hot_rows)]:
        if rows:
            latency_rows.append(
                latency_table_row(f"Full 65/65 ({scope_label})", [r["full65_ms"] for r in rows])
            )
    lines.extend(
        text_table(
            ["View", "n", "p50", "p95", "p99", "max", ">300ms", ">350ms", ">500ms", "did_not_complete"],
            latency_rows,
            right_align={1, 2, 3, 4, 5, 6, 7, 8, 9},
        )
    )

    if have_sql:
        lines.extend([
            "",
            "## Event Latency (SQL-only, DB-side)",
            "",
            "DB-side single-query execution time only (excludes fan-out/fan-in and client overhead). "
            "NOTE: these are per-event order statistics of individual query runtimes -- the 60th-fastest "
            "and slowest bundle query within each event -- NOT the SQL-side time-to-60th/65th-result. "
            "Because queries do not all start simultaneously (connection-wait staggering), these are a "
            "best-case lower bound on DB-side completion time and are NOT a like-for-like analogue of the "
            "wall-clock score-ready/full-event metrics above; use them for diagnosis only.",
            "",
        ])
        sql_rows = []
        for label, values in [
            ("60th-fastest bundle query runtime (All)", [r["sql_score60_ms"] for r in event_records]),
            ("Slowest bundle query runtime (All)", [r["sql_full65_ms"] for r in event_records]),
        ]:
            sql_rows.append(latency_table_row(label, values))
        lines.extend(
            text_table(
                ["View", "n", "p50", "p95", "p99", "max", ">300ms", ">350ms", ">500ms", "did_not_complete"],
                sql_rows,
                right_align={1, 2, 3, 4, 5, 6, 7, 8, 9},
            )
        )

    # ---- Histogram (wall-clock) ----
    lines.extend([
        "",
        "## Return-Time Histogram (wall-clock)",
        "",
    ])
    hist_rows = []
    for label, values in [
        ("Score-ready 60/65", [r["score60_ms"] for r in event_records]),
        ("Full 65/65", [r["full65_ms"] for r in event_records]),
    ]:
        counts = hist(values)
        hist_rows.append([
            label,
            *[f"{counts[name]:,}" for name, _, _ in HIST_BUCKETS],
            f"{counts['>500ms']:,}",
            f"{counts['did_not_complete']:,}",
        ])
    lines.extend(
        text_table(
            ["View", "0-50", "50-100", "100-150", "150-200", "200-300", "300-350", "350-500", ">500ms", "did_not_complete"],
            hist_rows,
            right_align={1, 2, 3, 4, 5, 6, 7, 8, 9},
        )
    )

    lines.extend(["", "## Average Bundles Returned", ""])
    if not have_cutoffs:
        lines.append(
            "_Not recorded in this run (worker-pool mode). Run in event-fanout or conn-fanout mode._"
        )
    else:
        lines.extend(
            text_table(
                ["Cutoff", "Average bundles returned"],
                [[f"{int(cutoff)}ms", f"{avg_bundles[cutoff]:.2f}/65"] for cutoff in CUTOFFS],
                right_align={1},
            )
        )

    multi_process = len(results) > 1
    tail_caption = (
        "Per-bundle SQL-only runtime across all events. Miss counts (>cutoff) are "
        "exact summed bundle-execution counts and are the primary ranking key."
    )
    if multi_process:
        tail_caption += (
            " NOTE: p95/p99/max are the MAX of each process's per-bundle percentile "
            "(not a true merged fleet percentile) -- treat as an approximate upper bound."
        )
    lines.extend([
        "",
        "## Tail / Miss Drivers",
        "",
        tail_caption,
        "",
    ])
    p_header_suffix = " (max-of-process)" if multi_process else ""
    tail_table_rows = []
    for bundle_id, row in tail_rows:
        tail_table_rows.append([
            bundle_id,
            f"{row['n']:,}",
            fmt_ms(row["p95"]),
            fmt_ms(row["p99"]),
            fmt_ms(row["max"]),
            f"{row['over_300']:,}",
            f"{row['over_350']:,}",
            f"{row['over_500']:,}",
        ])
    lines.extend(
        text_table(
            [
                "Bundle",
                "n",
                "p95" + p_header_suffix,
                "p99" + p_header_suffix,
                "max" + p_header_suffix,
                ">300ms",
                ">350ms",
                ">500ms",
            ],
            tail_table_rows,
            right_align={1, 2, 3, 4, 5, 6, 7},
        )
    )

    Path(args.output_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {args.output_md}")


if __name__ == "__main__":
    main()
