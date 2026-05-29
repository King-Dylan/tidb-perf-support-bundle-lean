#!/usr/bin/env python3
"""Create a team-ready summary from mixed traffic + engine comparison JSON."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


PREAGG_SCOPE = [
    ("Group A", "routing, merchant", "30d / 90d / 180d", 6),
    ("Group B", "exact_id, smart_id, input_ip, true_ip", "30d / 90d / 180d", 12),
    ("Group C", "catalog long-window joined bundles 015-025", "30d / 180d paths in catalog", 11),
]

BINDING_FIELDS = [
    "check_bank_routing_number",
    "merchant_account_number",
    "card_holder_number_sha512",
    "check_bank_account_number_sha512",
    "exact_id",
    "smart_id",
    "input_ip",
    "true_ip",
]

HISTOGRAM_CUTOFFS_MS = [50, 100, 150, 200, 250, 300, 350, 400, 450, 500]
DROPOFF_WINDOWS_MS = [
    ("0-50ms", 0, 50),
    ("50-100ms", 50, 100),
    ("100-150ms", 100, 150),
    ("150-200ms", 150, 200),
    ("200-350ms", 200, 350),
    ("350-500ms", 350, 500),
]
BUNDLES_PER_EVENT = 65
BUNDLE_COUNTS_BY_GROUP = {"Group A": 20, "Group B": 20, "Group C": 25}


def pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = (len(vals) - 1) * p / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return vals[int(k)]
    return vals[f] * (c - k) + vals[c] * (k - f)


def fmt_ms(v: float) -> str:
    return f"{v:.1f} ms"


def fmt_pct(numerator: int, denominator: int) -> str:
    return f"{(numerator / denominator * 100):.1f}%" if denominator else "0.0%"


def summarize_vals(vals: list[float]) -> dict[str, float | int]:
    return {
        "n": len(vals),
        "avg": sum(vals) / len(vals) if vals else 0.0,
        "p50": pct(vals, 50),
        "p95": pct(vals, 95),
        "p99": pct(vals, 99),
        "max": max(vals) if vals else 0.0,
        "over350": sum(v > 350 for v in vals),
        "over500": sum(v > 500 for v in vals),
    }


def summarize_events(rows: list[dict[str, Any]]) -> dict[str, Any]:
    vals = [float(r["ms"]) for r in rows]
    return summarize_vals(vals)


def completion_summary(rows: list[dict[str, Any]], field: str) -> dict[str, float | int]:
    vals = [float(r[field]) for r in rows if float(r.get(field, -1.0)) >= 0]
    return summarize_vals(vals)


def event_total_bundles(row: dict[str, Any]) -> int:
    bundles = row.get("bundle_results", [])
    if bundles:
        return len(bundles)
    return int(row.get("bundle_count", BUNDLES_PER_EVENT))


def event_bundles_by_cutoff(row: dict[str, Any], cutoff_ms: int) -> int:
    counts = row.get("bundle_counts_by_cutoff", {})
    if counts:
        if str(cutoff_ms) in counts:
            return int(counts[str(cutoff_ms)])
        lower_cutoffs = [int(k) for k in counts if int(k) <= cutoff_ms]
        return int(counts[str(max(lower_cutoffs))]) if lower_cutoffs else 0
    return sum(1 for b in row.get("bundle_results", []) if 0 <= float(b["ms"]) <= cutoff_ms)


def event_bundle_errors(row: dict[str, Any]) -> int:
    if "error_count" in row:
        return int(row.get("error_count") or 0)
    return sum(1 for b in row.get("bundle_results", []) if float(b["ms"]) < 0)


def bundle_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    coverage = []
    for r in rows:
        total = event_total_bundles(r)
        by350 = event_bundles_by_cutoff(r, 350)
        by500 = event_bundles_by_cutoff(r, 500)
        errors = event_bundle_errors(r)
        coverage.append(
            {
                "event": r["event"],
                "kind": r["kind"],
                "event_ms": float(r["ms"]),
                "by350": by350,
                "by500": by500,
                "over350": max(total - by350 - errors, 0),
                "over500": max(total - by500 - errors, 0),
                "errors": errors,
                "total": total,
            }
        )
    return {
        "rows": coverage,
        "events_with_60_by_350": sum(r["by350"] >= 60 for r in coverage),
        "events_with_60_by_500": sum(r["by500"] >= 60 for r in coverage),
        "events_all_65_by_350": sum(r["by350"] >= 65 for r in coverage),
        "events_all_65_by_500": sum(r["by500"] >= 65 for r in coverage),
        "by350": summarize_vals([r["by350"] for r in coverage]),
        "by500": summarize_vals([r["by500"] for r in coverage]),
        "errors": summarize_vals([r["errors"] for r in coverage]),
    }


def sampled_event_index(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for key in ("sampled_normal_events", "sampled_hot_events"):
        for event in data.get(key, []):
            events[event["invoice_number"]] = event
    return events


def binding_reuse_rows(data: dict[str, Any], reads: list[dict[str, Any]]) -> tuple[int, int, list[list[Any]]]:
    event_index = sampled_event_index(data)
    full_sets = []
    values_by_field: dict[str, list[str]] = {field: [] for field in BINDING_FIELDS}
    for read in reads:
        event = event_index.get(read["event"], {})
        bindings = event.get("bindings", {})
        full_sets.append(tuple(str(bindings.get(field, "")) for field in BINDING_FIELDS))
        for field in BINDING_FIELDS:
            values_by_field[field].append(str(bindings.get(field, "")))
    rows: list[list[Any]] = []
    for field in BINDING_FIELDS:
        values = values_by_field[field]
        counts = Counter(values)
        rows.append([field, len(counts), max(counts.values()) if counts else 0])
    return len(set(read["event"] for read in reads)), len(set(full_sets)), rows


def runtime_preagg_counts(rows: list[dict[str, Any]], preagg_bundle_ids: list[str] | None = None) -> tuple[list[list[Any]], int, int]:
    by_bundle: dict[str, dict[str, Any]] = {}
    runtime_execs = 0
    preagg_execs = 0
    if rows and not rows[0].get("bundle_results"):
        preagg_by_group: dict[str, int] = defaultdict(int)
        for bundle_id in preagg_bundle_ids or []:
            if bundle_id.startswith("group_a_"):
                preagg_by_group["Group A"] += 1
            elif bundle_id.startswith("group_b_"):
                preagg_by_group["Group B"] += 1
            elif bundle_id.startswith("group_c_"):
                preagg_by_group["Group C"] += 1
        rows_out = []
        for group in ["Group A", "Group B", "Group C"]:
            preagg = preagg_by_group[group] * len(rows)
            runtime = (BUNDLE_COUNTS_BY_GROUP[group] - preagg_by_group[group]) * len(rows)
            rows_out.append([group, runtime, preagg, runtime + preagg])
        rows_out.append(["Total", sum(r[1] for r in rows_out), sum(r[2] for r in rows_out), sum(r[3] for r in rows_out)])
        return rows_out, sum(r[1] for r in rows_out[:-1]), sum(r[2] for r in rows_out[:-1])

    for event in rows:
        for bundle in event.get("bundle_results", []):
            bid = bundle["bundle_id"]
            by_bundle[bid] = bundle
            if bundle.get("preagg_applied"):
                preagg_execs += 1
            else:
                runtime_execs += 1
    by_group: dict[str, Counter] = defaultdict(Counter)
    for bundle in by_bundle.values():
        mode = "preagg" if bundle.get("preagg_applied") else "runtime"
        by_group[bundle.get("group", "?")][mode] += 1
    rows_out = []
    for group in ["A", "B", "C"]:
        runtime = by_group[group]["runtime"]
        preagg = by_group[group]["preagg"]
        rows_out.append([f"Group {group}", runtime, preagg, runtime + preagg])
    rows_out.append(["Total", sum(r[1] for r in rows_out), sum(r[2] for r in rows_out), sum(r[3] for r in rows_out)])
    return rows_out, runtime_execs, preagg_execs


def bundle_return_histogram(rows: list[dict[str, Any]]) -> list[list[Any]]:
    out: list[list[Any]] = []
    for cutoff in HISTOGRAM_CUTOFFS_MS:
        per_event_counts = []
        total_returned = 0
        for event in rows:
            count = event_bundles_by_cutoff(event, cutoff)
            per_event_counts.append(count)
            total_returned += count
        out.append(
            [
                f"<={cutoff}ms",
                f"{(sum(per_event_counts) / len(per_event_counts)):.1f}/65" if per_event_counts else "0.0/65",
                f"{pct(per_event_counts, 50):.0f}" if per_event_counts else "0",
                f"{sum(v >= 60 for v in per_event_counts)}/{len(rows)}",
                f"{sum(v >= 65 for v in per_event_counts)}/{len(rows)}",
                total_returned,
            ]
        )
    timeout_or_errors = sum(max(event_total_bundles(event) - event_bundles_by_cutoff(event, 500), 0) for event in rows)
    out.append([">500ms or error", "", "", "", "", timeout_or_errors])
    return out


def bundle_return_dropoff(rows: list[dict[str, Any]]) -> list[list[Any]]:
    out: list[list[Any]] = []
    event_count = len(rows)
    for label, start_ms, end_ms in DROPOFF_WINDOWS_MS:
        total = 0
        for event in rows:
            if start_ms == 0:
                total += event_bundles_by_cutoff(event, end_ms)
            else:
                total += event_bundles_by_cutoff(event, end_ms) - event_bundles_by_cutoff(event, start_ms)
        out.append([label, f"{(total / event_count):.1f}" if event_count else "0.0", f"{total:,}"])
    timeout_or_errors = sum(max(event_total_bundles(event) - event_bundles_by_cutoff(event, 500), 0) for event in rows)
    out.append([">500/error", f"{(timeout_or_errors / event_count):.2f}" if event_count else "0.00", f"{timeout_or_errors:,}"])
    return out


def slow_bundle_table(rows: list[dict[str, Any]], limit: int = 0) -> list[dict[str, Any]]:
    by_bundle: dict[str, list[tuple[float, dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    meta: dict[str, dict[str, Any]] = {}
    for event in rows:
        for bundle in event.get("bundle_results", []):
            bid = bundle["bundle_id"]
            ms = float(bundle["ms"])
            by_bundle[bid].append((ms, event, bundle))
            meta[bid] = bundle
    out = []
    for bid, vals in by_bundle.items():
        over350 = [(ms, ev, b) for ms, ev, b in vals if ms > 350]
        over500 = [(ms, ev, b) for ms, ev, b in vals if ms > 500]
        errors = [(ms, ev, b) for ms, ev, b in vals if ms < 0]
        if not over350 and not errors:
            continue
        vals_ms = [ms for ms, _, _ in vals if ms >= 0]
        out.append(
            {
                "bundle_id": bid,
                "group": meta[bid].get("group"),
                "window_days": meta[bid].get("window_days"),
                "base_filter": meta[bid].get("base_filter"),
                "preagg_applied": meta[bid].get("preagg_applied"),
                "over350": len(over350),
                "over500": len(over500),
                "errors": len(errors),
                "p95": pct(vals_ms, 95) if vals_ms else -1,
                "max": max(vals_ms) if vals_ms else -1,
                "kinds": Counter(ev["kind"] for _, ev, _ in (over350 or errors)).most_common(4),
            }
        )
    out = sorted(out, key=lambda r: (-(r["over350"] + r["errors"]), -r["max"]))
    return out[:limit] if limit else out


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = [
        "bundle_id",
        "group",
        "window_days",
        "base_filter",
        "preagg_applied",
        "over350",
        "over500",
        "errors",
        "p95",
        "max",
        "kinds",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})


def write_table(lines: list[str], headers: list[str], rows: list[list[Any]]) -> None:
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(x) for x in row) + " |")


def write_text_table(lines: list[str], headers: list[str], rows: list[list[Any]]) -> None:
    string_rows = [[str(x) for x in row] for row in rows]
    widths = [
        max([len(str(header))] + [len(row[i]) for row in string_rows])
        for i, header in enumerate(headers)
    ]
    lines.append("```text")
    lines.append("  ".join(str(header).ljust(widths[i]) for i, header in enumerate(headers)))
    for row in string_rows:
        lines.append("  ".join(row[i].ljust(widths[i]) for i in range(len(headers))))
    lines.append("```")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mixed-json", required=True)
    ap.add_argument("--engine-json")
    ap.add_argument("--label", default="Benchmark")
    ap.add_argument("--output")
    ap.add_argument("--slow-limit", type=int, default=0, help="Limit slow bundle rows in Markdown. 0 means all.")
    ap.add_argument("--slow-csv", help="Optional CSV output for all bundles that exceeded 350ms.")
    args = ap.parse_args()

    mixed_path = ROOT / args.mixed_json if not args.mixed_json.startswith("/") else Path(args.mixed_json)
    mixed = json.loads(mixed_path.read_text())
    reads = mixed["read_results"]
    normal = [r for r in reads if r["kind"] == "normal"]
    hot = [r for r in reads if r["kind"].startswith("hot_")]
    writes = mixed.get("write_results", [])

    lines: list[str] = []
    lines.append(f"# {args.label}")
    lines.append("")
    lines.append("## Test Shape")
    lines.append("")
    lines.append(f"- Duration: {mixed['duration']} seconds")
    lines.append(f"- Read rate: {mixed['read_rate']} events/sec")
    fanout = mixed.get("fanout_capacity") or {}
    if fanout:
        lines.append(
            f"- Bundle fan-out target: {fanout.get('target_bundle_qps', 0):,.0f} bundle SQL/sec "
            f"({fanout.get('bundles_per_event', 65)} independent bundle queries per event)"
        )
    lines.append(f"- Hot-key mix target: {mixed['hot_event_pct']:.1%}")
    lines.append(f"- Scripted warmup: {mixed['warmup']} seconds; skip initial preflight: {mixed.get('skip_initial_warmup')}")
    lines.append(f"- Read max_execution_time: {mixed.get('read_max_execution_time_ms', 0)} ms")
    lines.append(f"- Pre-agg mode: {mixed.get('preagg_mode', 'hybrid')}")
    lines.append(f"- Reads completed: {len(reads)} events ({len(normal)} normal, {len(hot)} hot-key)")
    lines.append(f"- Writes completed: {len(writes)} insert attempts")
    notes = mixed.get("query_shape_notes", {})
    if notes:
        if notes.get("event_bundle_dependency"):
            lines.append(f"- Event bundle dependency model: {notes.get('event_bundle_dependency')}")
        lines.append(f"- Group C join key: {notes.get('group_c_join_key')}")
        lines.append(f"- Group C timestamp filter: {notes.get('group_c_timestamp_filter')}")
    if reads and not reads[0].get("bundle_results"):
        if reads[0].get("bundle_counts_by_cutoff"):
            lines.append("- WARNING: this JSON omits full `bundle_results`; coverage stats are available, but slow-bundle details are limited.")
        else:
            lines.append("- WARNING: this JSON does not include full `bundle_results`; slow-bundle and 60/65 timeout stats are incomplete.")
    lines.append("")

    if fanout:
        lines.append("## Fan-Out Capacity")
        lines.append("")
        lines.append(
            "The scoring app waits for the combined feature vector, so event latency is the fan-in wall-clock "
            "of the 65 independent bundle queries, not the sum of their latencies."
        )
        lines.append("")
        write_table(
            lines,
            ["Metric", "Value"],
            [
                ["Events/sec target", f"{float(fanout.get('event_qps', 0)):.1f}"],
                ["Bundles/event", fanout.get("bundles_per_event", 65)],
                ["Bundle SQL/sec target", f"{float(fanout.get('target_bundle_qps', 0)):,.0f}"],
                ["Client bundle slots", fanout.get("configured_bundle_slots", "")],
                ["Max fully fanned-out events by client slots", fanout.get("max_events_fully_fanned_out_by_client", "")],
                ["Slots needed if bundles occupy 350ms", fanout.get("required_bundle_slots_if_queries_run_350ms", "")],
                ["Slots needed if bundles occupy 500ms", fanout.get("required_bundle_slots_if_queries_run_500ms", "")],
                ["Configured / 350ms requirement", f"{float(fanout.get('configured_vs_350ms_requirement_pct', 0)):.1f}%"],
                ["Configured / 500ms requirement", f"{float(fanout.get('configured_vs_500ms_requirement_pct', 0)):.1f}%"],
            ],
        )
        lines.append("")

    unique_events, unique_full_sets, binding_rows = binding_reuse_rows(mixed, reads)
    lines.append("## Binding Reuse / Test Realism")
    lines.append("")
    lines.append(
        f"This run used {unique_events} unique event IDs, and the full 8-ID binding set was unique for "
        f"{unique_full_sets}/{len(reads)} events. We were not replaying one warmed event repeatedly."
    )
    lines.append("")
    write_table(lines, ["Field", f"Distinct values / {len(reads)} events", "Max repeat"], binding_rows)
    lines.append("")

    lines.append("## Pre-Aggregation Scope")
    lines.append("")
    write_table(lines, ["Group", "Keys / paths", "Windows", "Logical paths"], PREAGG_SCOPE)
    lines.append("")
    lines.append(f"Pre-agg bundle IDs: {', '.join(mixed.get('preagg_bundles', []))}")
    lines.append("")

    counts_rows, runtime_execs, preagg_execs = runtime_preagg_counts(reads, mixed.get("preagg_bundles", []))
    lines.append("## Runtime vs Pre-Agg Bundle Counts")
    lines.append("")
    lines.append(
        f"Across {len(reads)} events: runtime bundle executions={runtime_execs:,}; "
        f"pre-agg bundle executions={preagg_execs:,}."
    )
    lines.append("")
    write_table(lines, ["Group", "Runtime bundles", "Pre-agg bundles", "Total bundles"], counts_rows)
    lines.append("")

    lines.append("## Hot-Key Values Used")
    lines.append("")
    hot_fields = mixed.get("profile", {}).get("hot_fields", {})
    hot_rows = [[k, v.get("table"), v.get("value"), f"{v.get('count'):,}"] for k, v in hot_fields.items()]
    write_table(lines, ["Field", "Source", "Hot value", "Rows"], hot_rows)
    lines.append("")

    lines.append("## Event Latency")
    lines.append("")
    latency_rows = []
    for label, subset in [("All", reads), ("Normal", normal), ("Hot-key", hot)]:
        s = summarize_events(subset)
        latency_rows.append([label, s["n"], fmt_ms(s["p50"]), fmt_ms(s["p95"]), fmt_ms(s["p99"]), fmt_ms(s["max"]), s["over350"], s["over500"]])
    write_table(lines, ["Scope", "n", "p50", "p95", "p99", "max", ">350ms", ">500ms"], latency_rows)
    lines.append("")

    if any(float(r.get("bundle_60th_completion_ms", -1.0)) >= 0 for r in reads):
        lines.append("## Score-Ready Completion Latency")
        lines.append("")
        lines.append("This measures wall-clock time from event dispatch until enough bundle queries have completed.")
        lines.append("")
        completion_rows = []
        for label, subset in [("All", reads), ("Normal", normal), ("Hot-key", hot)]:
            s60 = completion_summary(subset, "bundle_60th_completion_ms")
            s65 = completion_summary(subset, "bundle_65th_completion_ms")
            completion_rows.append(
                [
                    label,
                    s60["n"],
                    fmt_ms(s60["p50"]),
                    fmt_ms(s60["p95"]),
                    fmt_ms(s60["p99"]),
                    s60["over350"],
                    s60["over500"],
                    s65["n"],
                    fmt_ms(s65["p95"]),
                    fmt_ms(s65["p99"]),
                    s65["over500"],
                ]
            )
        write_table(
            lines,
            ["Scope", "60/65 n", "60/65 p50", "60/65 p95", "60/65 p99", "60/65 >350", "60/65 >500", "65/65 n", "65/65 p95", "65/65 p99", "65/65 >500"],
            completion_rows,
        )
        lines.append("")

    all_cov = bundle_coverage(reads)
    lines.append("## Bundle Coverage Scorecard")
    lines.append("")
    lines.append("Henry's 60/65 rule: can the event proceed?")
    lines.append("")
    lines.append(
        f"- By 350ms: {all_cov['events_with_60_by_350']}/{len(reads)} events passed "
        f"({fmt_pct(all_cov['events_with_60_by_350'], len(reads))})"
    )
    lines.append(
        f"- By 500ms: {all_cov['events_with_60_by_500']}/{len(reads)} events passed "
        f"({fmt_pct(all_cov['events_with_60_by_500'], len(reads))})"
    )
    lines.append("")
    lines.append("Andrew's 65/65 view: did we get every feature?")
    lines.append("")
    lines.append(
        f"- By 350ms: {all_cov['events_all_65_by_350']}/{len(reads)} events were complete "
        f"({fmt_pct(all_cov['events_all_65_by_350'], len(reads))})"
    )
    lines.append(
        f"- By 500ms: {all_cov['events_all_65_by_500']}/{len(reads)} events were complete "
        f"({fmt_pct(all_cov['events_all_65_by_500'], len(reads))})"
    )
    lines.append("")
    lines.append("Average bundle coverage across all events:")
    lines.append("")
    lines.append(f"- By 350ms: {all_cov['by350']['avg']:.1f}/65 bundles back on average")
    lines.append(f"- By 500ms: {all_cov['by500']['avg']:.1f}/65 bundles back on average")
    lines.append("")

    lines.append("## Coverage Detail by Event Type")
    lines.append("")
    coverage_rows = []
    for label, subset in [("All", reads), ("Normal", normal), ("Hot-key", hot)]:
        cov = bundle_coverage(subset)
        coverage_rows.append(
            [
                label,
                len(subset),
                f"{cov['events_with_60_by_350']}/{len(subset)}",
                f"{cov['events_with_60_by_500']}/{len(subset)}",
                f"{cov['events_all_65_by_350']}/{len(subset)}",
                f"{cov['events_all_65_by_500']}/{len(subset)}",
                f"{cov['by350']['p50']:.0f}",
                f"{cov['by500']['p50']:.0f}",
                f"{cov['errors']['p50']:.0f}",
            ]
        )
    write_table(
        lines,
        ["Scope", "events", ">=60/65 by 350ms", ">=60/65 by 500ms", "65/65 by 350ms", "65/65 by 500ms", "median by 350", "median by 500", "median errors"],
        coverage_rows,
    )
    lines.append("")

    lines.append("## Bundle Return-Time Drop-Off")
    lines.append("")
    lines.append("This shows how many additional bundle results land in each time window.")
    lines.append("")
    write_text_table(
        lines,
        ["Window", "Avg bundles/event", "Total bundle executions"],
        bundle_return_dropoff(reads),
    )
    lines.append("")

    lines.append("## Bundle Return-Time CDF")
    lines.append("")
    lines.append("This shows how quickly bundle results become available across events.")
    lines.append("")
    write_text_table(
        lines,
        ["Cutoff", "Avg bundles returned", "Median returned", "Events >=60/65", "Events 65/65", "Bundle executions"],
        bundle_return_histogram(reads),
    )
    lines.append("")

    lines.append("## Slow Bundles (>350ms)")
    lines.append("")
    slow_rows = []
    all_slow_bundles = slow_bundle_table(reads, limit=0)
    lines.append(f"Bundles with at least one run over 350ms: {len(all_slow_bundles)}")
    lines.append("")
    for row in all_slow_bundles[: args.slow_limit or None]:
        slow_rows.append(
            [
                row["bundle_id"],
                row["group"],
                row["window_days"],
                row["base_filter"],
                row["preagg_applied"],
                row["over350"],
                row["over500"],
                row["errors"],
                fmt_ms(row["p95"]),
                fmt_ms(row["max"]),
                "; ".join(f"{k}:{v}" for k, v in row["kinds"]),
            ]
        )
    write_table(lines, ["Bundle", "Group", "Window", "Filter", "Preagg", ">350", ">500", "Errors", "p95", "max", "Kinds"], slow_rows)
    lines.append("")
    slow_csv = Path(args.slow_csv) if args.slow_csv else None
    if slow_csv:
        write_csv(slow_csv, all_slow_bundles)

    if writes:
        lines.append("## Write Latency")
        lines.append("")
        by_table: dict[str, list[float]] = defaultdict(list)
        for w in writes:
            if w.get("ok", True):
                by_table[w.get("table", "unknown")].append(float(w["ms"]))
        write_rows = []
        for table, vals in sorted(by_table.items()):
            s = summarize_vals(vals)
            write_rows.append([table, s["n"], fmt_ms(s["p50"]), fmt_ms(s["p95"]), fmt_ms(s["p99"]), fmt_ms(s["max"])])
        write_table(lines, ["Table", "n", "p50", "p95", "p99", "max"], write_rows)
        lines.append("")

    if args.engine_json:
        engine_path = ROOT / args.engine_json if not args.engine_json.startswith("/") else Path(args.engine_json)
        engine = json.loads(engine_path.read_text())
        matrix = engine.get("matrix", [])
        counts = Counter(m["best_by_p95"] for m in matrix)
        lines.append("## TiKV vs TiFlash A/B")
        lines.append("")
        lines.append(f"- Best by p95: TiFlash {counts.get('TIFLASH', 0)}/65, TiKV {counts.get('TIKV', 0)}/65")
        lines.append("- This A/B is for the original runtime query shapes. Daily pre-aggregation is measured separately in the mixed-traffic run.")
        still_slow = []
        for m in matrix:
            best = m["best_by_p95"].lower()
            stats = m[best]
            if stats["over_350"]:
                still_slow.append([m["bundle_id"], m["group"], m["window_days"], m["base_filter"], m["best_by_p95"], stats["over_350"], fmt_ms(stats["p95"]), fmt_ms(stats["max"])])
        lines.append("")
        write_table(lines, ["Bundle", "Group", "Window", "Filter", "Best engine", ">350", "best p95", "best max"], still_slow[:30])
        lines.append("")

    output = Path(args.output) if args.output else ROOT / "results" / f"final_benchmark_summary_{mixed_path.stem}.md"
    output.write_text("\n".join(lines), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
