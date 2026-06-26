#!/usr/bin/env python3
"""Run the Go load generator across multiple SSH clients.

This script assumes each host already has the support bundle under
--remote-dir.  It is intentionally small: orchestration should not become
another benchmark bottleneck.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import subprocess
import sys
import time
from pathlib import Path

BINDING_FIELDS = [
    "merchant_account_number",
    "card_holder_number_sha512",
    "check_bank_routing_number",
    "check_bank_account_number_sha512",
    "exact_id",
    "smart_id",
    "input_ip",
    "true_ip",
]

EVENT_MIX_KEYS = [
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

HISTOGRAM_BUCKETS = [
    "0-50ms",
    "50-100ms",
    "100-150ms",
    "150-200ms",
    "200-300ms",
    "300-350ms",
    "350-500ms",
    ">500/error",
]


def parse_hosts(raw: str) -> list[str]:
    hosts: list[str] = []
    for part in raw.replace("\n", ",").split(","):
        part = part.strip()
        if part:
            hosts.append(part)
    if not hosts:
        raise ValueError("no hosts provided")
    return hosts


def parse_duration_seconds(raw: str | None) -> float | None:
    if raw is None:
        return None
    value = raw.strip().lower()
    if not value:
        return None
    unit = value[-1]
    multiplier = 1.0
    if unit in {"s", "m", "h"}:
        value = value[:-1]
        multiplier = {"s": 1.0, "m": 60.0, "h": 3600.0}[unit]
    seconds = float(value) * multiplier
    if seconds < 0:
        raise ValueError(f"duration must be >= 0: {raw}")
    return seconds


def percentile(values: list[float], pct: float) -> float:
    clean = sorted(v for v in values if v >= 0)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    rank = (pct / 100.0) * (len(clean) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return clean[int(rank)]
    return clean[lo] + (clean[hi] - clean[lo]) * (rank - lo)


def summarize(values: list[float]) -> dict[str, float | int]:
    clean = [v for v in values if v >= 0]
    if not clean:
        return {"n": 0}
    return {
        "n": len(clean),
        "p50": percentile(clean, 50),
        "p95": percentile(clean, 95),
        "p99": percentile(clean, 99),
        "p999": percentile(clean, 99.9),
        "avg": sum(clean) / len(clean),
        "max": max(clean),
        "over_300": sum(1 for v in clean if v > 300),
        "over_350": sum(1 for v in clean if v > 350),
        "over_500": sum(1 for v in clean if v > 500),
    }


def histogram(values: list[float], total_events: int) -> dict[str, int]:
    buckets = {bucket: 0 for bucket in HISTOGRAM_BUCKETS}
    valid = 0
    for value in values:
        if value < 0:
            continue
        valid += 1
        if value <= 50:
            buckets["0-50ms"] += 1
        elif value <= 100:
            buckets["50-100ms"] += 1
        elif value <= 150:
            buckets["100-150ms"] += 1
        elif value <= 200:
            buckets["150-200ms"] += 1
        elif value <= 300:
            buckets["200-300ms"] += 1
        elif value <= 350:
            buckets["300-350ms"] += 1
        elif value <= 500:
            buckets["350-500ms"] += 1
        else:
            buckets[">500/error"] += 1
    buckets[">500/error"] += max(0, total_events - valid)
    return buckets


def print_summary_line(label: str, summary: dict[str, float | int]) -> None:
    print(
        f"{label:<28s} n={summary.get('n', 0)} "
        f"p50={summary.get('p50', 0):.1f} p95={summary.get('p95', 0):.1f} "
        f"p99={summary.get('p99', 0):.1f} p999={summary.get('p999', 0):.1f} "
        f"max={summary.get('max', 0):.1f} >300={summary.get('over_300', 0)} "
        f">350={summary.get('over_350', 0)} >500={summary.get('over_500', 0)}"
    )


def nested_dict(row: dict[str, object], path: list[str]) -> dict[str, object]:
    current = nested_value(row, path)
    return current if isinstance(current, dict) else {}


def nested_value(row: dict[str, object], path: list[str]) -> object:
    current: object = row
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def first_nested_dict(results: list[dict[str, object]], path: list[str]) -> dict[str, object]:
    for result in results:
        found = nested_dict(result, path)
        if found:
            return found
    return {}


def aggregate_result_jsons(
    paths: list[Path],
    report_window_start_s: float | None = None,
    report_window_duration_s: float | None = None,
) -> dict[str, object]:
    results = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    workload_stats = first_nested_dict(results, ["workload_stats"]) or first_nested_dict(results, ["customer_report", "test_realism"])
    workload_selection = first_nested_dict(results, ["workload_event_selection"])
    bundle_mode_counts = first_nested_dict(results, ["customer_report", "bundle_mode_counts"])
    all_event_rows: list[dict[str, object]] = []
    for result in results:
        started_at = float(result.get("started_at_unix", 0.0) or 0.0)
        for row in result.get("event_results", []):
            if not isinstance(row, dict):
                continue
            tagged = dict(row)
            tagged["_worker_started_at_unix"] = started_at
            all_event_rows.append(tagged)
    window_anchor = min((float(result.get("started_at_unix", 0.0) or 0.0) for result in results), default=0.0)
    window_end_s = None
    if report_window_start_s is not None and report_window_duration_s is not None:
        window_start_ns = int((window_anchor + report_window_start_s) * 1_000_000_000)
        window_end_ns = int((window_anchor + report_window_start_s + report_window_duration_s) * 1_000_000_000)
        event_rows = [
            row
            for row in all_event_rows
            if window_start_ns <= int(row.get("completed_at_unix_nano", 0) or 0) < window_end_ns
        ]
        window_end_s = report_window_start_s + report_window_duration_s
    else:
        event_rows = all_event_rows
    total_events = len(event_rows)

    event_score60 = [float(row.get("score60_ms", -1)) for row in event_rows]
    event_full65 = [float(row.get("full65_ms", -1)) for row in event_rows]
    sql_score60 = [float(row.get("sql_score60_ms", -1)) for row in event_rows]
    sql_full65 = [float(row.get("sql_full65_ms", -1)) for row in event_rows]
    bundles_300 = [float(row.get("bundles_by_300_ms", -1)) for row in event_rows if float(row.get("bundles_by_300_ms", -1)) >= 0]
    bundles_350 = [float(row.get("bundles_by_350_ms", -1)) for row in event_rows if float(row.get("bundles_by_350_ms", -1)) >= 0]
    bundles_500 = [float(row.get("bundles_by_500_ms", -1)) for row in event_rows if float(row.get("bundles_by_500_ms", -1)) >= 0]
    event_mix = Counter(str(row.get("kind") or "<empty>") for row in event_rows)
    hot_field_mix = Counter(str(row.get("hot_field") or "<empty>") for row in event_rows)
    source_events = {str(row.get("source_event")) for row in event_rows if row.get("source_event")}
    binding_set_hashes = {str(row.get("binding_set_hash")) for row in event_rows if row.get("binding_set_hash")}
    workload_indices = [int(row.get("workload_idx", -1)) for row in event_rows if int(row.get("workload_idx", -1)) >= 0]
    cache_states = sorted({str(result.get("cache_state") or "unknown") for result in results})
    run_shape = {
        "app_processes": len(results),
        "total_connections": sum(int(result.get("connections", 0) or 0) for result in results),
        "ready_workers": sum(int(result.get("ready_workers", 0) or 0) for result in results),
        "target_event_eps": sum(float(result.get("target_event_eps", 0.0) or 0.0) for result in results),
        "duration_seconds": max((float(result.get("duration_seconds", 0.0) or 0.0) for result in results), default=0.0),
        "max_pending_events_per_process": max((int(result.get("max_pending_events", 0) or 0) for result in results), default=0),
        "execution_modes": sorted({str(result.get("execution_mode") or "unknown") for result in results}),
        "prepare_all": all(bool(result.get("prepare_all")) for result in results) if results else False,
        "max_execution_time_ms": sorted({int(result.get("max_execution_time_ms", 0) or 0) for result in results}),
        "event_stride_values": sorted({int(result.get("event_stride", 0) or 0) for result in results}),
        "event_sampling_modes": sorted({str(result.get("event_sampling") or "stride") for result in results}),
        "event_random_seeds": sorted({int(result.get("event_random_seed", 0) or 0) for result in results}),
    }

    tail_by_bundle: dict[str, dict[str, float | int | str]] = {}
    for result in results:
        event_summaries = result.get("bundle_event_summaries", {}) if isinstance(result.get("bundle_event_summaries"), dict) else {}
        sql_summaries = result.get("bundle_summaries", {}) if isinstance(result.get("bundle_summaries"), dict) else {}
        tail_rows = nested_value(result, ["customer_report", "tail_drivers"])
        miss_by_bundle: dict[str, dict[str, int]] = {}
        if isinstance(tail_rows, list):
            for row in tail_rows:
                if isinstance(row, dict):
                    bundle_id = str(row.get("bundle_id") or "")
                    miss_by_bundle[bundle_id] = {
                        "over_300": int(row.get("over_300", 0) or 0),
                        "over_350": int(row.get("over_350", 0) or 0),
                        "over_500": int(row.get("over_500", 0) or 0),
                    }
        for bundle_id, summary in event_summaries.items():
            if not isinstance(summary, dict):
                continue
            sql_summary = sql_summaries.get(bundle_id, {}) if isinstance(sql_summaries.get(bundle_id, {}), dict) else {}
            misses = miss_by_bundle.get(bundle_id, {})
            row = tail_by_bundle.setdefault(
                bundle_id,
                {
                    "bundle_id": bundle_id,
                    "n": 0,
                    "p95": 0.0,
                    "p99": 0.0,
                    "p999": 0.0,
                    "max": 0.0,
                    "over_300": 0,
                    "over_350": 0,
                    "over_500": 0,
                    "sql_p95": 0.0,
                    "sql_p99": 0.0,
                    "sql_p999": 0.0,
                    "sql_max": 0.0,
                },
            )
            row["n"] = int(row["n"]) + int(summary.get("n", 0))
            row["p95"] = max(float(row["p95"]), float(summary.get("p95", 0.0)))
            row["p99"] = max(float(row["p99"]), float(summary.get("p99", 0.0)))
            row["p999"] = max(float(row["p999"]), float(summary.get("p999", 0.0)))
            row["max"] = max(float(row["max"]), float(summary.get("max", 0.0)))
            row["over_300"] = int(row["over_300"]) + int(misses.get("over_300", summary.get("over_300", 0)) or 0)
            row["over_350"] = int(row["over_350"]) + int(misses.get("over_350", summary.get("over_350", 0)) or 0)
            row["over_500"] = int(row["over_500"]) + int(misses.get("over_500", summary.get("over_500", 0)) or 0)
            row["sql_p95"] = max(float(row["sql_p95"]), float(sql_summary.get("p95", 0.0)))
            row["sql_p99"] = max(float(row["sql_p99"]), float(sql_summary.get("p99", 0.0)))
            row["sql_p999"] = max(float(row["sql_p999"]), float(sql_summary.get("p999", 0.0)))
            row["sql_max"] = max(float(row["sql_max"]), float(sql_summary.get("max", 0.0)))
        if not event_summaries:
            for bundle_id, summary in sql_summaries.items():
                if not isinstance(summary, dict):
                    continue
                row = tail_by_bundle.setdefault(
                    bundle_id,
                    {"bundle_id": bundle_id, "n": 0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "max": 0.0, "over_300": 0, "over_350": 0, "over_500": 0},
                )
                row["n"] = int(row["n"]) + int(summary.get("n", 0))
                row["p95"] = max(float(row["p95"]), float(summary.get("p95", 0.0)))
                row["p99"] = max(float(row["p99"]), float(summary.get("p99", 0.0)))
                row["p999"] = max(float(row["p999"]), float(summary.get("p999", 0.0)))
                row["max"] = max(float(row["max"]), float(summary.get("max", 0.0)))
                row["over_300"] = int(row["over_300"]) + int(summary.get("over_300", 0))
                row["over_350"] = int(row["over_350"]) + int(summary.get("over_350", 0))
                row["over_500"] = int(row["over_500"]) + int(summary.get("over_500", 0))
    tail_drivers = sorted(
        tail_by_bundle.values(),
        key=lambda row: (int(row["over_500"]), int(row["over_350"]), int(row["over_300"]), float(row["p99"]), float(row["max"])),
        reverse=True,
    )[:20]

    elapsed = report_window_duration_s if report_window_duration_s is not None else max((float(result.get("elapsed_seconds", 0.0)) for result in results), default=0.0)
    def count_rate(values: list[float], cutoff: float) -> dict[str, float | int]:
        count = sum(1 for value in values if 0 <= value <= cutoff)
        return {
            "events": count,
            "percent": (count / total_events * 100.0) if total_events else 0.0,
            "eps": (count / elapsed) if elapsed else 0.0,
        }

    return {
        "result_files": [str(path) for path in paths],
        "completed_events": total_events,
        "report_window": {
            "applied": report_window_start_s is not None and report_window_duration_s is not None,
            "anchor_started_at_unix": window_anchor,
            "start_seconds": report_window_start_s,
            "end_seconds": window_end_s,
            "duration_seconds": report_window_duration_s,
            "input_events_before_window": len(all_event_rows),
        },
        "elapsed_seconds": elapsed,
        "completed_eps": (total_events / elapsed) if elapsed else 0.0,
        "run_shape": run_shape,
        "latency_scope": "benchmark_harness_event_wall_clock_ms",
        "event_sla": {
            "score_ready_60_of_65": {"300ms": count_rate(event_score60, 300), "350ms": count_rate(event_score60, 350), "500ms": count_rate(event_score60, 500)},
            "full_65_of_65": {"300ms": count_rate(event_full65, 300), "350ms": count_rate(event_full65, 350), "500ms": count_rate(event_full65, 500)},
        },
        "event_latency": {
            "score_ready_60_of_65": {"summary": summarize(event_score60), "histogram": histogram(event_score60, total_events)},
            "full_65_of_65": {"summary": summarize(event_full65), "histogram": histogram(event_full65, total_events)},
        },
        "sql_only_sla": {
            "score_ready_60_of_65": {"300ms": count_rate(sql_score60, 300), "350ms": count_rate(sql_score60, 350), "500ms": count_rate(sql_score60, 500)},
            "full_65_of_65": {"300ms": count_rate(sql_full65, 300), "350ms": count_rate(sql_full65, 350), "500ms": count_rate(sql_full65, 500)},
        },
        "sql_only_latency": {
            "score_ready_60_of_65": {"summary": summarize(sql_score60), "histogram": histogram(sql_score60, total_events)},
            "full_65_of_65": {"summary": summarize(sql_full65), "histogram": histogram(sql_full65, total_events)},
        },
        "diagnostic_note": "Event SLA/latency is benchmark-harness wall-clock. SQL-only latency is retained only for database-side diagnosis.",
        "avg_bundles_by_300_ms": (sum(bundles_300) / len(bundles_300)) if bundles_300 else 0.0,
        "avg_bundles_by_350_ms": (sum(bundles_350) / len(bundles_350)) if bundles_350 else 0.0,
        "avg_bundles_by_500_ms": (sum(bundles_500) / len(bundles_500)) if bundles_500 else 0.0,
        "test_realism": {
            "generated_workload_rows": int(workload_stats.get("event_rows", workload_stats.get("generated_workload_rows", 0)) or 0),
            "unique_source_events_generated": int(workload_stats.get("unique_source_events", 0) or 0),
            "unique_binding_sets_generated": int(workload_stats.get("unique_binding_sets", 0) or 0),
            "unique_executed_source_events": len(source_events),
            "unique_executed_binding_sets": len(binding_set_hashes),
            "unique_executed_workload_rows": len(set(workload_indices)),
            "workload_row_reuse_max": max(Counter(workload_indices).values()) if workload_indices else 0,
            "event_sample_cycled_or_reused": (total_events > len(set(workload_indices))) if workload_indices else False,
            "event_mix": {key: int(event_mix.get(key, 0)) for key in EVENT_MIX_KEYS},
            "hot_field_mix": dict(sorted(hot_field_mix.items())),
            "cache_states": cache_states,
            "workload_selection": workload_selection,
            "workload_stats": workload_stats,
        },
        "bundle_mode_counts": bundle_mode_counts,
        "tail_drivers": tail_drivers,
    }


def print_fleet_customer_report(report: dict[str, object]) -> None:
    print()
    print("FLEET CUSTOMER EVENT WALL-CLOCK REPORT")
    print(f"completed_events={report['completed_events']} completed_eps={report['completed_eps']:.2f}")
    window = report.get("report_window") or {}
    if isinstance(window, dict) and window.get("applied"):
        print(
            "report_window="
            f"{window.get('start_seconds'):.1f}s-{window.get('end_seconds'):.1f}s "
            f"duration={window.get('duration_seconds'):.1f}s "
            f"input_events_before_window={window.get('input_events_before_window')}"
        )
    sla = report["event_sla"]
    for name in ("score_ready_60_of_65", "full_65_of_65"):
        for cutoff in ("300ms", "350ms", "500ms"):
            row = sla[name][cutoff]
            print(f"  {name} <= {cutoff}: events={row['events']} pct={row['percent']:.2f}% eps={row['eps']:.2f}")
    print(
        f"avg_bundles_by_300ms={report['avg_bundles_by_300_ms']:.2f} "
        f"avg_bundles_by_350ms={report['avg_bundles_by_350_ms']:.2f} "
        f"avg_bundles_by_500ms={report['avg_bundles_by_500_ms']:.2f}"
    )
    for name, detail in report["event_latency"].items():
        print_summary_line(name, detail["summary"])
        print(f"  {name}_histogram {detail['histogram']}")
    print("sql_only_latency_for_diagnosis")
    for name, detail in report["sql_only_latency"].items():
        print_summary_line("sql_only_" + name, detail["summary"])
    realism = report["test_realism"]
    print("test_realism")
    print(
        f"  generated_workload_rows={realism.get('generated_workload_rows')} "
        f"unique_source_events_generated={realism.get('unique_source_events_generated')} "
        f"unique_binding_sets_generated={realism.get('unique_binding_sets_generated')} "
        f"  unique_executed_source_events={realism['unique_executed_source_events']} "
        f"unique_executed_binding_sets={realism.get('unique_executed_binding_sets')} "
        f"unique_executed_workload_rows={realism['unique_executed_workload_rows']} "
        f"workload_row_reuse_max={realism['workload_row_reuse_max']} "
        f"event_sample_cycled_or_reused={realism.get('event_sample_cycled_or_reused')} "
        f"cache_states={realism['cache_states']}"
    )
    print(f"  event_mix={realism['event_mix']} hot_field_mix={realism['hot_field_mix']}")
    print(
        f"  event_sampling_modes={report.get('run_shape', {}).get('event_sampling_modes')} "
        f"event_random_seeds={report.get('run_shape', {}).get('event_random_seeds')}"
    )
    workload_stats = realism.get("workload_stats") or {}
    if workload_stats:
        print(
            f"  generated_workload_rows={workload_stats.get('event_rows')} "
            f"unique_source_events={workload_stats.get('unique_source_events')} "
            f"unique_binding_sets={workload_stats.get('unique_binding_sets')}"
        )
        print(f"  binding_fields={workload_stats.get('binding_fields')}")
    print("tail_drivers_by_bundle_event_wall_clock_miss")
    for index, row in enumerate(report["tail_drivers"], 1):
        print(
            f"  {index:2d} {row['bundle_id']:<20s} n={row['n']} event_p95={row['p95']:.1f} "
            f"event_p99={row['p99']:.1f} event_p999={row['p999']:.1f} event_max={row['max']:.1f} "
            f"miss300={row['over_300']} miss350={row['over_350']} miss500={row['over_500']} "
            f"sql_p99={float(row.get('sql_p99', 0.0)):.1f}"
        )


def format_count(value: object) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def format_ms(value: object) -> str:
    try:
        ms = float(value)
    except (TypeError, ValueError):
        return "0.0ms"
    if ms >= 1000:
        return f"{ms / 1000:.2f}s"
    return f"{ms:.1f}ms"


def format_percent(value: object) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "0.00%"


def format_eps(value: object) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def markdown_count_rate(row: dict[str, object], total_events: int) -> str:
    return (
        f"{format_count(row.get('events'))}/{format_count(total_events)} "
        f"({format_percent(row.get('percent'))}, {format_eps(row.get('eps'))} EPS)"
    )


def markdown_summary_row(name: str, detail: dict[str, object]) -> str:
    summary = detail.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    return (
        f"| {name} | {format_count(summary.get('n'))} | {format_ms(summary.get('p50'))} | "
        f"{format_ms(summary.get('p95'))} | {format_ms(summary.get('p99'))} | "
        f"{format_ms(summary.get('max'))} | {format_count(summary.get('over_300'))} | "
        f"{format_count(summary.get('over_350'))} | {format_count(summary.get('over_500'))} |"
    )


def markdown_histogram_row(name: str, detail: dict[str, object]) -> str:
    hist = detail.get("histogram", {})
    if not isinstance(hist, dict):
        hist = {}
    buckets = ["0-50ms", "50-100ms", "100-150ms", "150-200ms", "200-300ms", "300-350ms", "350-500ms", ">500/error"]
    cells = " | ".join(format_count(hist.get(bucket, 0)) for bucket in buckets)
    return f"| {name} | {cells} |"


def render_markdown_report(report: dict[str, object], runs: list[dict[str, object]]) -> str:
    total_events = int(report.get("completed_events", 0) or 0)
    realism = report.get("test_realism", {})
    if not isinstance(realism, dict):
        realism = {}
    workload_stats = realism.get("workload_stats") or {}
    if not isinstance(workload_stats, dict):
        workload_stats = {}
    workload_selection = realism.get("workload_selection") or {}
    if not isinstance(workload_selection, dict):
        workload_selection = {}
    binding_fields = workload_stats.get("binding_fields") or {}
    if not isinstance(binding_fields, dict):
        binding_fields = {}
    hot_values = workload_stats.get("hot_values") or {}
    if not isinstance(hot_values, dict):
        hot_values = {}
    sla = report.get("event_sla", {})
    if not isinstance(sla, dict):
        sla = {}
    latency = report.get("event_latency", {})
    if not isinstance(latency, dict):
        latency = {}
    sql_latency = report.get("sql_only_latency", {})
    if not isinstance(sql_latency, dict):
        sql_latency = {}
    sql_sla = report.get("sql_only_sla", {})
    if not isinstance(sql_sla, dict):
        sql_sla = {}
    result_files = report.get("result_files") or []
    if not isinstance(result_files, list):
        result_files = []
    cache_states = realism.get("cache_states") or []
    if not isinstance(cache_states, list):
        cache_states = []
    run_shape = report.get("run_shape") or {}
    if not isinstance(run_shape, dict):
        run_shape = {}
    report_window = report.get("report_window") or {}
    if not isinstance(report_window, dict):
        report_window = {}
    bundle_mode_counts = report.get("bundle_mode_counts") or {}
    if not isinstance(bundle_mode_counts, dict):
        bundle_mode_counts = {}

    score_ready = sla.get("score_ready_60_of_65", {})
    full_65 = sla.get("full_65_of_65", {})
    if not isinstance(score_ready, dict):
        score_ready = {}
    if not isinstance(full_65, dict):
        full_65 = {}

    event_mix = realism.get("event_mix") or {}
    if not isinstance(event_mix, dict):
        event_mix = {}

    def summary_cells(detail: dict[str, object]) -> str:
        summary = detail.get("summary", {}) if isinstance(detail, dict) else {}
        if not isinstance(summary, dict):
            summary = {}
        return (
            f"{format_count(summary.get('n'))} | {format_ms(summary.get('p50'))} | "
            f"{format_ms(summary.get('p95'))} | {format_ms(summary.get('p99'))} | "
            f"{format_ms(summary.get('max'))}"
        )

    def short_code(value: object, limit: int = 42) -> str:
        text = str(value or "")
        if len(text) > limit:
            text = text[:limit] + "..."
        return f"`{text}`" if text else ""

    lines: list[str] = []
    lines.append("# 1000 EPS Event-Level Benchmark Report")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Completed events: {format_count(total_events)}")
    lines.append(f"- Completed event throughput: {format_eps(report.get('completed_eps'))} events/sec")
    lines.append("- Primary latency scope: benchmark-harness event wall-clock. Timer starts immediately before launching the 65 bundle tasks for an event and stops when the 60th and 65th successful bundle return.")
    lines.append("- Diagnostic latency scope: SQL-only bundle runtime is retained separately for database-side analysis.")
    if report_window.get("applied"):
        lines.append(
            f"- Report window: {float(report_window.get('start_seconds', 0.0) or 0.0):.1f}s"
            f"-{float(report_window.get('end_seconds', 0.0) or 0.0):.1f}s from earliest worker start "
            f"({float(report_window.get('duration_seconds', 0.0) or 0.0):.1f}s steady-state slice)"
        )
    else:
        lines.append("- Report window: full fetched run")
    lines.append("")
    event_sampling_modes = run_shape.get("event_sampling_modes", [])
    event_random_seeds = run_shape.get("event_random_seeds", [])
    lines.append("## Test Shape")
    lines.append("")
    lines.append(f"- Target event EPS: {format_eps(run_shape.get('target_event_eps'))}")
    lines.append(f"- Duration: {format_eps(run_shape.get('duration_seconds'))} seconds")
    lines.append(f"- Fleet app processes: {format_count(run_shape.get('app_processes', len(runs)))}")
    lines.append(f"- Result JSON files merged: {format_count(len(result_files))}")
    lines.append(f"- Total configured connections: {format_count(run_shape.get('total_connections'))}")
    lines.append(f"- Ready connection workers: {format_count(run_shape.get('ready_workers'))}")
    lines.append(f"- Max pending events per process: {format_count(run_shape.get('max_pending_events_per_process'))}")
    lines.append(f"- Execution modes: {', '.join(run_shape.get('execution_modes', [])) if isinstance(run_shape.get('execution_modes'), list) else 'unknown'}")
    lines.append(f"- Prepared statements enabled on all processes: {run_shape.get('prepare_all', False)}")
    lines.append(f"- Max execution time settings: {run_shape.get('max_execution_time_ms', [])}")
    lines.append(f"- Event sampling modes: {event_sampling_modes}")
    lines.append(f"- Event random seeds: {event_random_seeds}")
    lines.append(f"- Event stride values: {run_shape.get('event_stride_values', [])}")
    lines.append(f"- Cache state labels: {', '.join(cache_states) if cache_states else 'unknown'}")
    lines.append("")
    lines.append("## Binding Reuse / Test Realism")
    lines.append("")
    lines.append(f"- Total generated workload rows: {format_count(realism.get('generated_workload_rows') or workload_stats.get('event_rows'))}")
    lines.append(f"- Unique source events generated: {format_count(realism.get('unique_source_events_generated') or workload_stats.get('unique_source_events'))}")
    lines.append(f"- Unique full 8-field binding sets generated: {format_count(realism.get('unique_binding_sets_generated') or workload_stats.get('unique_binding_sets'))}")
    lines.append(f"- Unique source events executed in this fleet run: {format_count(realism.get('unique_executed_source_events'))}")
    lines.append(f"- Unique full 8-field binding sets executed in this fleet run: {format_count(realism.get('unique_executed_binding_sets'))}")
    lines.append(f"- Unique workload rows executed in this fleet run: {format_count(realism.get('unique_executed_workload_rows'))}")
    lines.append(f"- Max workload row repeat in this fleet run: {format_count(realism.get('workload_row_reuse_max'))}")
    lines.append(f"- Source event sample reused during workload generation: {workload_selection.get('source_reused', 'unknown')}")
    lines.append(f"- Event sample cycled/reused during this run: {realism.get('event_sample_cycled_or_reused', 'unknown')}")
    lines.append(f"- Unique events required during generation: {workload_selection.get('unique_events_required', 'unknown')}")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'Field':<36s} {'Distinct values':>16s} {'Max repeat':>12s}  Max repeated value")
    for field in BINDING_FIELDS:
        stats = binding_fields.get(field, {})
        if not isinstance(stats, dict):
            stats = {}
        max_value = str(stats.get("max_value", ""))
        if len(max_value) > 48:
            max_value = max_value[:48] + "..."
        lines.append(f"{field:<36s} {format_count(stats.get('distinct')):>16s} {format_count(stats.get('max_repeat')):>12s}  {max_value}")
    lines.append("```")
    lines.append("")
    lines.append("## Event Mix")
    lines.append("")
    for key in EVENT_MIX_KEYS:
        lines.append(f"- {key}: {format_count(event_mix.get(key, 0))}")
    lines.append("")
    lines.append("## Runtime vs Pre-Agg / Serving Bundle Counts")
    lines.append("")
    lines.append("Each event runs 65 logical bundle queries. The classification below is based on the SQL templates in the Go workload.")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'Group':<8s} {'Runtime':>8s} {'Pre-agg':>8s} {'Serving':>8s} {'Total':>8s}")
    by_group = bundle_mode_counts.get("by_group", {}) if isinstance(bundle_mode_counts.get("by_group", {}), dict) else {}
    for group in sorted(by_group):
        row = by_group[group] if isinstance(by_group[group], dict) else {}
        lines.append(
            f"{group:<8s} {format_count(row.get('runtime')):>8s} {format_count(row.get('preagg')):>8s} "
            f"{format_count(row.get('serving')):>8s} {format_count(row.get('total')):>8s}"
        )
    lines.append(
        f"{'Total':<8s} {format_count(bundle_mode_counts.get('runtime')):>8s} "
        f"{format_count(bundle_mode_counts.get('preagg')):>8s} {format_count(bundle_mode_counts.get('serving')):>8s} "
        f"{format_count(bundle_mode_counts.get('total')):>8s}"
    )
    lines.append("```")
    lines.append("")
    lines.append("## Hot-Key Values Used")
    lines.append("")
    if hot_values:
        lines.append("| Field | Source | Hot value | Rows |")
        lines.append("| --- | --- | --- | ---: |")
        for field in BINDING_FIELDS:
            row = hot_values.get(field, {})
            if not isinstance(row, dict):
                row = {}
            source = row.get("table") or row.get("source") or ""
            lines.append(f"| {field} | {source} | {short_code(row.get('value'))} | {format_count(row.get('count'))} |")
    else:
        lines.append("Hot-key profile metadata was not embedded in this workload file.")
    lines.append("")
    lines.append("## Event Latency")
    lines.append("")
    lines.append("This is benchmark-harness event wall-clock latency: launch 65 bundle tasks for one event, then stop at the 60th and 65th successful bundle return.")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'View':<24s} {'n':>8s} {'p50':>10s} {'p95':>10s} {'p99':>10s} {'max':>10s}")
    for label, key in [("Score-ready 60/65", "score_ready_60_of_65"), ("Full 65/65", "full_65_of_65")]:
        parts = summary_cells(latency.get(key, {})).split(" | ")
        lines.append(f"{label:<24s} {parts[0]:>8s} {parts[1]:>10s} {parts[2]:>10s} {parts[3]:>10s} {parts[4]:>10s}")
    lines.append("```")
    lines.append("")
    lines.append("Diagnostic SQL-only bundle latency, for database-side comparison only:")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'View':<24s} {'n':>8s} {'p50':>10s} {'p95':>10s} {'p99':>10s} {'max':>10s}")
    for label, key in [("SQL score-ready 60/65", "score_ready_60_of_65"), ("SQL full 65/65", "full_65_of_65")]:
        parts = summary_cells(sql_latency.get(key, {})).split(" | ")
        lines.append(f"{label:<24s} {parts[0]:>8s} {parts[1]:>10s} {parts[2]:>10s} {parts[3]:>10s} {parts[4]:>10s}")
    lines.append("```")
    lines.append("")
    lines.append("## 60/65 and 65/65 SLA View")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'View':<24s} {'<=300ms':>24s} {'<=350ms':>24s} {'<=500ms':>24s}")
    lines.append(
        f"{'Score-ready >=60/65':<24s} "
        f"{markdown_count_rate(score_ready.get('300ms', {}), total_events):>24s} "
        f"{markdown_count_rate(score_ready.get('350ms', {}), total_events):>24s} "
        f"{markdown_count_rate(score_ready.get('500ms', {}), total_events):>24s}"
    )
    lines.append(
        f"{'Full 65/65':<24s} "
        f"{markdown_count_rate(full_65.get('300ms', {}), total_events):>24s} "
        f"{markdown_count_rate(full_65.get('350ms', {}), total_events):>24s} "
        f"{markdown_count_rate(full_65.get('500ms', {}), total_events):>24s}"
    )
    lines.append("```")
    lines.append("")
    lines.append("Average successful bundles returned by event wall-clock cutoff:")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'Cutoff':<10s} {'Average bundles returned':>28s}")
    lines.append(f"{'300ms':<10s} {float(report.get('avg_bundles_by_300_ms', 0.0) or 0.0):>25.2f}/65")
    lines.append(f"{'350ms':<10s} {float(report.get('avg_bundles_by_350_ms', 0.0) or 0.0):>25.2f}/65")
    lines.append(f"{'500ms':<10s} {float(report.get('avg_bundles_by_500_ms', 0.0) or 0.0):>25.2f}/65")
    lines.append("```")
    lines.append("")
    lines.append("## Return-Time Histogram")
    lines.append("")
    lines.append("```text")
    lines.append(f"{'View':<22s} " + " ".join(f"{bucket:>12s}" for bucket in HISTOGRAM_BUCKETS))
    for label, key in [("Score-ready 60/65", "score_ready_60_of_65"), ("Full 65/65", "full_65_of_65")]:
        hist = latency.get(key, {}).get("histogram", {}) if isinstance(latency.get(key, {}), dict) else {}
        if not isinstance(hist, dict):
            hist = {}
        lines.append(f"{label:<22s} " + " ".join(f"{format_count(hist.get(bucket)):>12s}" for bucket in HISTOGRAM_BUCKETS))
    lines.append("```")
    lines.append("")
    lines.append("## Tail Drivers")
    lines.append("")
    lines.append("Miss counts below are bundle executions that did not successfully return by each event wall-clock cutoff. SQL p99 is included only as a database-side diagnostic.")
    lines.append("")
    lines.append("| Bundle | n | event p95 | event p99 | event p999 | event max | miss by 300ms | miss by 350ms | miss by 500ms | SQL p99 |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in report.get("tail_drivers", []):
        if not isinstance(row, dict):
            continue
        lines.append(
            f"| {row.get('bundle_id', '')} | {format_count(row.get('n'))} | "
            f"{format_ms(row.get('p95'))} | {format_ms(row.get('p99'))} | "
            f"{format_ms(row.get('p999'))} | {format_ms(row.get('max'))} | "
            f"{format_count(row.get('over_300'))} | {format_count(row.get('over_350'))} | "
            f"{format_count(row.get('over_500'))} | {format_ms(row.get('sql_p99'))} |"
        )
    lines.append("")
    lines.append("## Merged Result Files")
    lines.append("")
    for item in result_files:
        lines.append(f"- `{item}`")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", required=True, help="Comma/newline separated SSH hosts, e.g. ec2-user@host1,ec2-user@host2")
    ap.add_argument("--ssh-key", required=True)
    ap.add_argument("--remote-dir", default="~/tidb_intuit_perf_support_bundle_lean/code")
    ap.add_argument("--workload", default="results/go_workload_1000_bundle_serving.json")
    ap.add_argument("--db-config", default=".db_config.json")
    ap.add_argument("--events-total", type=int, default=1000)
    ap.add_argument("--connections-total", type=int, default=1300)
    ap.add_argument("--processes-per-host", type=int, default=1)
    ap.add_argument("--read-timeout", default="5s")
    ap.add_argument("--setup-timeout", default="60s")
    ap.add_argument("--query-timeout", default="0s")
    ap.add_argument("--max-execution-time-ms", type=int, default=0)
    ap.add_argument("--execution-mode", default="event-fanout", choices=("worker-pool", "event-fanout", "conn-fanout"))
    ap.add_argument("--target-event-eps", type=float, default=0.0, help="Fleet-wide steady-state target event EPS.")
    ap.add_argument("--duration", default="0s", help="Steady-state duration passed to go-loadgen, e.g. 60s.")
    ap.add_argument("--max-pending-events", type=int, default=0, help="Per-process pending event cap for steady mode.")
    ap.add_argument("--event-sampling", default="stride", choices=("stride", "random"), help="Workload event selection mode passed to Go loadgen.")
    ap.add_argument("--event-random-seed", type=int, default=20260625, help="Seed passed to Go loadgen when --event-sampling=random.")
    ap.add_argument("--prepare-all", action="store_true")
    ap.add_argument(
        "--omit-event-results",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Pass through to Go loadgen. Default keeps per-event rows for customer SLA reporting.",
    )
    ap.add_argument(
        "--shard-workload",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Pass distinct --event-offset/--event-stride to each process so a large workload is covered across the fleet.",
    )
    ap.add_argument("--cache-state", default="unknown", help="Cache state label stored in each Go result JSON.")
    ap.add_argument("--cache-note", default="", help="Free-form cache-state note stored in each Go result JSON.")
    ap.add_argument(
        "--fetch-results",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch remote Go result JSON files and print an aggregate fleet customer report.",
    )
    ap.add_argument(
        "--start-delay-seconds",
        type=float,
        default=0.0,
        help="When >0, pass a shared Unix-millisecond start time to every worker so they run after setup/prepare.",
    )
    ap.add_argument(
        "--report-window-start",
        default=None,
        help="Optional stable-window report start offset from earliest Go worker start, e.g. 240s or 4m.",
    )
    ap.add_argument(
        "--report-window-duration",
        default=None,
        help="Optional stable-window report duration, e.g. 180s or 3m. Requires --report-window-start.",
    )
    ap.add_argument("--output-prefix", default=None)
    args = ap.parse_args()
    report_window_start_s = parse_duration_seconds(args.report_window_start)
    report_window_duration_s = parse_duration_seconds(args.report_window_duration)
    if (report_window_start_s is None) != (report_window_duration_s is None):
        raise SystemExit("--report-window-start and --report-window-duration must be set together")

    hosts = parse_hosts(args.hosts)
    workers = [(host, proc_idx) for host in hosts for proc_idx in range(args.processes_per_host)]
    run_id = int(time.time())
    output_prefix = args.output_prefix or f"go_fleet_{run_id}"
    events_each = math.ceil(args.events_total / len(workers))
    conns_each = math.ceil(args.connections_total / len(workers))
    start_at_unix_ms = 0
    if args.start_delay_seconds > 0:
        start_at_unix_ms = int((time.time() + args.start_delay_seconds) * 1000)
        print(f"global start_at_unix_ms={start_at_unix_ms} delay={args.start_delay_seconds:.1f}s")
    target_eps_each = args.target_event_eps / len(workers) if args.target_event_eps > 0 else 0.0

    procs: list[tuple[subprocess.Popen[str], Path, str]] = []
    local_log_dir = Path("results")
    local_log_dir.mkdir(exist_ok=True)
    steady_mode = args.target_event_eps > 0
    for index, (host, proc_idx) in enumerate(workers):
        if steady_mode:
            events = events_each
        else:
            events = min(events_each, max(0, args.events_total - index * events_each))
        if not steady_mode and events <= 0:
            continue
        remote_output = f"results/{output_prefix}_{index}.json"
        prepare = "--prepare-all=true" if args.prepare_all else "--prepare-all=false"
        omit_events = "--omit-event-results=true" if args.omit_event_results else "--omit-event-results=false"
        event_offset = index if args.shard_workload else 0
        event_stride = len(workers) if args.shard_workload else 1
        remote_cmd = (
            f"cd {args.remote_dir} && ulimit -n 50000 && "
            f"./go-loadgen/go-loadgen-linux-amd64 "
            f"--workload {args.workload} "
            f"--db-config {args.db_config} "
            f"--output {remote_output} "
            f"--events {events} "
            f"--connections {conns_each} "
            f"--setup-timeout {args.setup_timeout} "
            f"--read-timeout {args.read_timeout} "
            f"--query-timeout {args.query_timeout} "
            f"--max-execution-time-ms {args.max_execution_time_ms} "
            f"--execution-mode {args.execution_mode} "
            f"--target-event-eps {target_eps_each:.6f} "
            f"--duration {args.duration} "
            f"--max-pending-events {args.max_pending_events} "
            f"--event-sampling {args.event_sampling} "
            f"--event-random-seed {args.event_random_seed + index} "
            f"--start-at-unix-ms {start_at_unix_ms} "
            f"--event-offset {event_offset} "
            f"--event-stride {event_stride} "
            f"--cache-state {args.cache_state} "
            f"--cache-note {json.dumps(args.cache_note)} "
            f"{omit_events} "
            f"{prepare}"
        )
        local_log = local_log_dir / f"{output_prefix}_{index}.log"
        fh = local_log.open("w")
        cmd = [
            "ssh",
            "-i",
            args.ssh_key,
            "-o",
            "StrictHostKeyChecking=no",
            host,
            remote_cmd,
        ]
        print(
            f"start worker={index} host={host} proc={proc_idx} events={events} conns={conns_each} "
            f"target_eps={target_eps_each:.3f} event_offset={event_offset} event_stride={event_stride}"
        )
        procs.append((subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, text=True), local_log, host))

    started = time.time()
    for proc, log, host in procs:
        rc = proc.wait()
        print(f"finished host={host} log={log} rc={rc}")
    elapsed = time.time() - started
    print(f"fleet wall elapsed={elapsed:.3f}s")

    summaries = []
    fetched_results: list[Path] = []
    for _, log, host in procs:
        text = log.read_text(errors="replace")
        print(f"--- {host} {log}")
        for prefix in ("Workers ready=", "elapsed=", "event_completion", "full_65_of_65", "sql_only_full_65_of_65", "query_runtime", "task_queue", "Saved:"):
            lines = [line for line in text.splitlines() if line.startswith(prefix)]
            if lines:
                print(lines[-1])
        saved = [line.split("Saved:", 1)[1].strip() for line in text.splitlines() if line.startswith("Saved:")]
        if saved:
            remote_result = saved[-1]
            local_result = local_log_dir / Path(remote_result).name
            if args.fetch_results:
                scp_cmd = [
                    "scp",
                    "-i",
                    args.ssh_key,
                    "-o",
                    "StrictHostKeyChecking=no",
                    f"{host}:{args.remote_dir}/{remote_result}",
                    str(local_result),
                ]
                rc = subprocess.call(scp_cmd)
                if rc == 0:
                    fetched_results.append(local_result)
                    print(f"fetched {host}:{remote_result} -> {local_result}")
                else:
                    print(f"WARNING: failed to fetch {host}:{remote_result} rc={rc}")
            summaries.append({"host": host, "remote_result": remote_result, "local_result": str(local_result), "log": str(log)})
    summary_path = local_log_dir / f"{output_prefix}_summary.json"
    fleet_report = aggregate_result_jsons(fetched_results, report_window_start_s, report_window_duration_s) if fetched_results else {}
    if fleet_report:
        print_fleet_customer_report(fleet_report)
        report_path = local_log_dir / f"{output_prefix}_customer_report.md"
        report_path.write_text(render_markdown_report(fleet_report, summaries), encoding="utf-8")
        print(f"customer_report_md={report_path}")
        local_workload = Path(args.workload)
        if not local_workload.exists():
            code_relative_workload = Path(__file__).resolve().parent / args.workload
            if code_relative_workload.exists():
                local_workload = code_relative_workload
        if local_workload.exists():
            readable_report = local_log_dir / f"{output_prefix}_customer_event_report.md"
            readable_json = local_log_dir / f"{output_prefix}_customer_event_report.json"
            report_script = Path(__file__).resolve().parent / "customer_event_report.py"
            cmd = [
                sys.executable,
                str(report_script),
                "--workload",
                str(local_workload),
                "--results",
                *(str(path) for path in fetched_results),
                "--output-md",
                str(readable_report),
                "--output-json",
                str(readable_json),
            ]
            try:
                subprocess.run(cmd, check=True)
                print(f"customer_event_report_md={readable_report}")
                print(f"customer_event_report_json={readable_json}")
            except subprocess.CalledProcessError as exc:
                print(f"WARNING: customer_event_report.py failed rc={exc.returncode}")
        else:
            print(
                "WARNING: local workload JSON not found; skipped customer_event_report.py. "
                f"Looked for {args.workload} and {Path(__file__).resolve().parent / args.workload}."
            )
    summary_path.write_text(json.dumps({"elapsed": elapsed, "runs": summaries, "fleet_customer_report": fleet_report}, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")


if __name__ == "__main__":
    main()
