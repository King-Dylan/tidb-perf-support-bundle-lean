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
    clean = [v for v in values if v >= 0]
    if not clean:
        return {"n": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    return {
        "n": len(clean),
        "p50": percentile(clean, 50),
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "max": max(clean),
    }


def fmt_ms(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:.2f}s"
    return f"{value:.1f}ms"


def fmt_duration(seconds: float) -> str:
    seconds = float(seconds or 0)
    if seconds <= 0:
        return "unknown"
    if seconds % 60 == 0:
        return f"{int(seconds // 60)}m ({int(seconds)}s)"
    return f"{seconds:.0f}s"


def hist(values: list[float]) -> dict[str, int]:
    counts = {name: 0 for name, _, _ in HIST_BUCKETS}
    counts[">500ms/error"] = 0
    for value in values:
        if value < 0 or value > 500:
            counts[">500ms/error"] += 1
            continue
        placed = False
        for name, lo, hi in HIST_BUCKETS:
            if lo <= value <= hi:
                counts[name] += 1
                placed = True
                break
        if not placed:
            counts[">500ms/error"] += 1
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


def latency_row(label: str, values: list[float]) -> str:
    summary = summarize(values)
    over300 = sum(1 for v in values if v < 0 or v > 300)
    over350 = sum(1 for v in values if v < 0 or v > 350)
    over500 = sum(1 for v in values if v < 0 or v > 500)
    return (
        f"| {label} | {summary['n']:,} | {fmt_ms(summary['p50'])} | {fmt_ms(summary['p95'])} | "
        f"{fmt_ms(summary['p99'])} | {fmt_ms(summary['max'])} | {over300:,} | {over350:,} | {over500:,} |"
    )


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


def first_value(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return default


def int_value(row: dict[str, Any], *keys: str, default: int = 0) -> int:
    value = first_value(row, *keys, default=default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def float_value(row: dict[str, Any], *keys: str, default: float = -1.0) -> float:
    value = first_value(row, *keys, default=default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
    args = ap.parse_args()

    workload_path = Path(args.workload)
    workload = load_json(workload_path)
    workload_events = workload.get("events", [])
    if not workload_events:
        raise ValueError("workload has no events")

    result_paths = [Path(path) for path in args.results]
    results = [load_json(path) for path in result_paths]

    event_records: list[dict[str, Any]] = []
    executed_source_indexes: list[int] = []
    for result_idx, result in enumerate(results):
        offset = int(result.get("event_offset", result_idx))
        stride = int(result.get("event_stride", len(results)))
        for event_result in result.get("event_results", []):
            src_idx = source_index(int(event_result["event_idx"]), offset, stride, len(workload_events))
            executed_source_indexes.append(src_idx)
            source = workload_events[src_idx]
            event_records.append(
                {
                    "source_index": src_idx,
                    "source_event": source.get("event"),
                    "kind": source.get("kind") or "normal",
                    "hot_field": source.get("hot_field"),
                    "bindings": source.get("bindings") or {},
                    "score60_ms": float_value(event_result, "score60_ms"),
                    "full65_ms": float_value(event_result, "full65_ms", "ms"),
                    "sql_score60_ms": float_value(event_result, "sql_score60_ms"),
                    "sql_full65_ms": float_value(event_result, "sql_full65_ms"),
                    "bundles_by_300ms": int_value(event_result, "bundles_by_300ms", "bundles_by_300_ms"),
                    "bundles_by_350ms": int_value(event_result, "bundles_by_350ms", "bundles_by_350_ms"),
                    "bundles_by_500ms": int_value(event_result, "bundles_by_500ms", "bundles_by_500_ms"),
                    "successes": int_value(event_result, "successes"),
                    "errors": int_value(event_result, "errors"),
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

    avg_bundles = {
        300.0: mean(row["bundles_by_300ms"] for row in event_records) if event_records else 0,
        350.0: mean(row["bundles_by_350ms"] for row in event_records) if event_records else 0,
        500.0: mean(row["bundles_by_500ms"] for row in event_records) if event_records else 0,
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
    # go-loadgen emits these at the top level of each result; older/merged outputs
    # may nest them under "run_shape". Prefer nested, else build from top-level keys.
    run_shape = next((r.get("run_shape") for r in results if r.get("run_shape")), None)
    if not run_shape:
        base = results[0] if results else {}
        run_shape = {
            "target_event_eps": base.get("target_event_eps"),
            "duration_seconds": base.get("duration_seconds"),
            "total_connections": base.get("connections") or base.get("total_connections"),
            "app_processes": len(results),
            "prepare_all": base.get("prepare_all"),
            "max_execution_time_ms": base.get("max_execution_time_ms"),
            "execution_mode": base.get("execution_mode"),
        }

    # Per-bundle cutoff counts (bundles_by_*ms) are recorded in event-fanout /
    # conn-fanout. worker-pool leaves them at zero, which would render a fake
    # all-zero SLA table. Use execution_mode instead of "any count > 0" so a bad
    # fanout run with legitimately zero bundles by 500ms still reports honestly.
    execution_mode = str(run_shape.get("execution_mode") or results[0].get("execution_mode") or "")
    have_cutoffs = execution_mode in {"event-fanout", "conn-fanout"}

    templates = workload.get("templates") or []
    mode_counts = Counter((template.get("mode") or "unknown") for template in templates)
    group_mode_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for template in templates:
        group_mode_counts[str(template.get("group") or "unknown")][str(template.get("mode") or "unknown")] += 1

    sample_reused = "yes" if completed_events > len(set(executed_source_events)) else "no"
    workload_cycled = "yes" if completed_events > len(workload_events) else "no"

    target_eps = run_shape.get("target_event_eps")
    duration_seconds = run_shape.get("duration_seconds")
    achieved_eps = completed_events / float(duration_seconds) if duration_seconds else None

    title = "Event-Level Benchmark Report"
    if target_eps:
        title = f"{int(target_eps):,} EPS Event-Level Benchmark Report"

    expected_events = int(float(target_eps or 0) * float(duration_seconds or 0)) if target_eps and duration_seconds else 0
    is_smoke = completed_events < 1000 or bool(expected_events and completed_events < expected_events * 0.9)
    run_type = "Smoke / format-validation run" if is_smoke else "Customer benchmark run"
    score300 = cutoff_count(event_records, 60, 300) if have_cutoffs else 0
    score350 = cutoff_count(event_records, 60, 350) if have_cutoffs else 0
    score500 = cutoff_count(event_records, 60, 500) if have_cutoffs else 0
    full300 = cutoff_count(event_records, 65, 300) if have_cutoffs else 0
    full350 = cutoff_count(event_records, 65, 350) if have_cutoffs else 0
    full500 = cutoff_count(event_records, 65, 500) if have_cutoffs else 0
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
        "completed_events": completed_events,
        "target_event_eps": target_eps,
        "duration_seconds": duration_seconds,
        "achieved_eps": achieved_eps,
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
        "avg_bundles_returned": {str(int(c)): avg_bundles[c] for c in CUTOFFS},
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
    lines.append(f"- Run type: {run_type}")
    if is_smoke:
        lines.append(
            "- Important: this is a small validation run for report shape and script correctness, not the final customer performance result."
        )
    if target_eps:
        lines.append(f"- Target load: {int(target_eps):,} events/sec ({int(target_eps) * 65:,} bundled SQL executions/sec)")
    if duration_seconds:
        lines.append(f"- Run window: {fmt_duration(duration_seconds)}")
    lines.extend(
        [
            f"- Completed events: {completed_events:,}",
            f"- Achieved event throughput: {achieved_eps:.1f} events/sec" if achieved_eps else "- Achieved event throughput: unknown",
            f"- Primary event metric: all 65/65 bundles by 300ms = {count_pct(full300, completed_events) if have_cutoffs else 'not recorded'}",
            f"- Score-ready fallback: at least 60/65 bundles by 300ms = {count_pct(score300, completed_events) if have_cutoffs else 'not recorded'}",
            (
                "- Full-event wall-clock latency: "
                f"p50 {fmt_ms(full_summary['p50'])}, p95 {fmt_ms(full_summary['p95'])}, "
                f"p99 {fmt_ms(full_summary['p99'])}, max {fmt_ms(full_summary['max'])}"
            ),
            (
                "- Score-ready wall-clock latency: "
                f"p50 {fmt_ms(score_summary['p50'])}, p95 {fmt_ms(score_summary['p95'])}, "
                f"p99 {fmt_ms(score_summary['p99'])}, max {fmt_ms(score_summary['max'])}"
            ),
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
                        count_pct(score300, completed_events),
                        count_pct(score350, completed_events),
                        count_pct(score500, completed_events),
                    ],
                    [
                        "Full event 65/65",
                        count_pct(full300, completed_events),
                        count_pct(full350, completed_events),
                        count_pct(full500, completed_events),
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
        lines.append(f"- Target event throughput: {int(target_eps):,} events/sec")
    if achieved_eps:
        lines.append(f"- Achieved event throughput: {achieved_eps:.1f} events/sec")
    if have_sql:
        lines.append("- SQL-only latency (DB-side query execution, excludes fan-out/fan-in) is reported alongside for comparison.")
    else:
        lines.append("- SQL-only latency not present in this run's output.")

    lines.extend(["", "## Test Shape", ""])
    if target_eps:
        lines.append(f"- Target event EPS: {int(target_eps):,}")
    if duration_seconds:
        lines.append(f"- Duration: {fmt_duration(duration_seconds)}")
    if run_shape.get("total_connections"):
        lines.append(f"- Total connections: {int(run_shape['total_connections']):,}")
    if run_shape.get("app_processes"):
        lines.append(f"- Fleet app processes: {int(run_shape['app_processes'])}")
    if "prepare_all" in run_shape:
        lines.append(f"- Prepared statements: {run_shape.get('prepare_all')}")
    if run_shape.get("max_execution_time_ms") is not None:
        lines.append(f"- Max execution time (ms): {run_shape.get('max_execution_time_ms')}")
    lines.extend([
        f"- Bundles per event: {workload.get('bundle_count', len(templates) or 'unknown')}",
        f"- Workload mode: {workload.get('mode', 'unknown')}",
        f"- Runtime window params: {workload.get('runtime_window_params', 'unknown')}",
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
        lines.extend(text_table(["Field", "Source", "Hot value", "Rows"], hot_rows_table, right_align={3}))
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
    if not have_cutoffs:
        lines.append(
            "_Per-bundle cutoff counts were not recorded in this run (worker-pool mode). "
            "Run in event-fanout or conn-fanout mode to populate this table._"
        )
    else:
        lines.extend([
            "```text",
        ])
        sla_rows = []
        for view_label, threshold in [("Score-ready >=60/65", 60), ("Full 65/65", 65)]:
            for scope_label, rows in [("All", event_records), ("Normal", normal_rows), ("Hot-key", hot_rows)]:
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
    latency_rows = []
    for scope_label, rows in [("All", event_records), ("Normal", normal_rows), ("Hot-key", hot_rows)]:
        if rows:
            summary = summarize([r["score60_ms"] for r in rows])
            values = [r["score60_ms"] for r in rows]
            latency_rows.append([
                f"Score-ready 60/65 ({scope_label})",
                f"{summary['n']:,}",
                fmt_ms(summary["p50"]),
                fmt_ms(summary["p95"]),
                fmt_ms(summary["p99"]),
                fmt_ms(summary["max"]),
                f"{sum(1 for v in values if v < 0 or v > 300):,}",
                f"{sum(1 for v in values if v < 0 or v > 350):,}",
                f"{sum(1 for v in values if v < 0 or v > 500):,}",
            ])
    for scope_label, rows in [("All", event_records), ("Normal", normal_rows), ("Hot-key", hot_rows)]:
        if rows:
            summary = summarize([r["full65_ms"] for r in rows])
            values = [r["full65_ms"] for r in rows]
            latency_rows.append([
                f"Full 65/65 ({scope_label})",
                f"{summary['n']:,}",
                fmt_ms(summary["p50"]),
                fmt_ms(summary["p95"]),
                fmt_ms(summary["p99"]),
                fmt_ms(summary["max"]),
                f"{sum(1 for v in values if v < 0 or v > 300):,}",
                f"{sum(1 for v in values if v < 0 or v > 350):,}",
                f"{sum(1 for v in values if v < 0 or v > 500):,}",
            ])
    lines.extend(
        text_table(
            ["View", "n", "p50", "p95", "p99", "max", ">300ms", ">350ms", ">500ms"],
            latency_rows,
            right_align={1, 2, 3, 4, 5, 6, 7, 8},
        )
    )

    if have_sql:
        lines.extend([
            "",
            "## Event Latency (SQL-only, DB-side)",
            "",
            "Same events, counting only TiDB-facing query execution time (excludes fan-out/fan-in and client overhead). Shown for comparison with the wall-clock view above.",
            "",
        ])
        sql_rows = []
        for label, values in [
            ("Score-ready 60/65 (All)", [r["sql_score60_ms"] for r in event_records]),
            ("Full 65/65 (All)", [r["sql_full65_ms"] for r in event_records]),
        ]:
            summary = summarize(values)
            sql_rows.append([
                label,
                f"{summary['n']:,}",
                fmt_ms(summary["p50"]),
                fmt_ms(summary["p95"]),
                fmt_ms(summary["p99"]),
                fmt_ms(summary["max"]),
                f"{sum(1 for v in values if v < 0 or v > 300):,}",
                f"{sum(1 for v in values if v < 0 or v > 350):,}",
                f"{sum(1 for v in values if v < 0 or v > 500):,}",
            ])
        lines.extend(
            text_table(
                ["View", "n", "p50", "p95", "p99", "max", ">300ms", ">350ms", ">500ms"],
                sql_rows,
                right_align={1, 2, 3, 4, 5, 6, 7, 8},
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
        hist_rows.append([label, *[f"{counts[name]:,}" for name, _, _ in HIST_BUCKETS], f"{counts['>500ms/error']:,}"])
    lines.extend(
        text_table(
            ["View", "0-50", "50-100", "100-150", "150-200", "200-300", "300-350", "350-500", ">500/error"],
            hist_rows,
            right_align={1, 2, 3, 4, 5, 6, 7, 8},
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

    lines.extend([
        "",
        "## Tail / Miss Drivers",
        "",
        "Per-bundle SQL-only runtime across all events. Miss counts are bundle executions over each cutoff.",
        "",
    ])
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
            ["Bundle", "n", "p95", "p99", "max", ">300ms", ">350ms", ">500ms"],
            tail_table_rows,
            right_align={1, 2, 3, 4, 5, 6, 7},
        )
    )

    Path(args.output_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {args.output_md}")


if __name__ == "__main__":
    main()
