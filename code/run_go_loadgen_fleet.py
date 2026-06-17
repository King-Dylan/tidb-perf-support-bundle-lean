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
import time
from pathlib import Path


def parse_hosts(raw: str) -> list[str]:
    hosts: list[str] = []
    for part in raw.replace("\n", ",").split(","):
        part = part.strip()
        if part:
            hosts.append(part)
    if not hosts:
        raise ValueError("no hosts provided")
    return hosts


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
    buckets = {
        "0-50ms": 0,
        "50-100ms": 0,
        "100-150ms": 0,
        "150-200ms": 0,
        "200-300ms": 0,
        "300-350ms": 0,
        "350-500ms": 0,
        ">500/error": 0,
    }
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


def aggregate_result_jsons(paths: list[Path]) -> dict[str, object]:
    results = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    event_rows = [row for result in results for row in result.get("event_results", [])]
    total_events = len(event_rows)
    sql_score60 = [float(row.get("sql_score60_ms", -1)) for row in event_rows]
    sql_full65 = [float(row.get("sql_full65_ms", -1)) for row in event_rows]
    bundles_300 = [float(row.get("bundles_by_300_ms", -1)) for row in event_rows if float(row.get("bundles_by_300_ms", -1)) >= 0]
    bundles_350 = [float(row.get("bundles_by_350_ms", -1)) for row in event_rows if float(row.get("bundles_by_350_ms", -1)) >= 0]
    bundles_500 = [float(row.get("bundles_by_500_ms", -1)) for row in event_rows if float(row.get("bundles_by_500_ms", -1)) >= 0]
    event_mix = Counter(str(row.get("kind") or "<empty>") for row in event_rows)
    hot_field_mix = Counter(str(row.get("hot_field") or "<empty>") for row in event_rows)
    source_events = {str(row.get("source_event")) for row in event_rows if row.get("source_event")}
    workload_indices = [int(row.get("workload_idx", -1)) for row in event_rows if int(row.get("workload_idx", -1)) >= 0]
    workload_stats = next((result.get("workload_stats") for result in results if result.get("workload_stats")), {})
    workload_selection = next((result.get("workload_event_selection") for result in results if result.get("workload_event_selection")), {})
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
    }

    tail_by_bundle: dict[str, dict[str, float | int | str]] = {}
    for result in results:
        for bundle_id, summary in result.get("bundle_summaries", {}).items():
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
        key=lambda row: (float(row["p999"]), float(row["p99"]), float(row["p95"]), float(row["max"])),
        reverse=True,
    )[:10]

    elapsed = max((float(result.get("elapsed_seconds", 0.0)) for result in results), default=0.0)
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
        "elapsed_seconds": elapsed,
        "completed_eps": (total_events / elapsed) if elapsed else 0.0,
        "run_shape": run_shape,
        "sql_only_sla": {
            "score_ready_60_of_65": {"300ms": count_rate(sql_score60, 300), "350ms": count_rate(sql_score60, 350), "500ms": count_rate(sql_score60, 500)},
            "full_65_of_65": {"300ms": count_rate(sql_full65, 300), "350ms": count_rate(sql_full65, 350), "500ms": count_rate(sql_full65, 500)},
        },
        "sql_only_latency": {
            "score_ready_60_of_65": {"summary": summarize(sql_score60), "histogram": histogram(sql_score60, total_events)},
            "full_65_of_65": {"summary": summarize(sql_full65), "histogram": histogram(sql_full65, total_events)},
        },
        "avg_bundles_by_300_ms": (sum(bundles_300) / len(bundles_300)) if bundles_300 else 0.0,
        "avg_bundles_by_350_ms": (sum(bundles_350) / len(bundles_350)) if bundles_350 else 0.0,
        "avg_bundles_by_500_ms": (sum(bundles_500) / len(bundles_500)) if bundles_500 else 0.0,
        "test_realism": {
            "unique_executed_source_events": len(source_events),
            "unique_executed_workload_rows": len(set(workload_indices)),
            "workload_row_reuse_max": max(Counter(workload_indices).values()) if workload_indices else 0,
            "event_mix": dict(sorted(event_mix.items())),
            "hot_field_mix": dict(sorted(hot_field_mix.items())),
            "cache_states": cache_states,
            "workload_selection": workload_selection,
            "workload_stats": workload_stats,
        },
        "tail_drivers": tail_drivers,
    }


def print_fleet_customer_report(report: dict[str, object]) -> None:
    print()
    print("FLEET CUSTOMER SQL-ONLY EVENT REPORT")
    print(f"completed_events={report['completed_events']} completed_eps={report['completed_eps']:.2f}")
    sla = report["sql_only_sla"]
    for name in ("score_ready_60_of_65", "full_65_of_65"):
        for cutoff in ("300ms", "350ms", "500ms"):
            row = sla[name][cutoff]
            print(f"  {name} <= {cutoff}: events={row['events']} pct={row['percent']:.2f}% eps={row['eps']:.2f}")
    print(
        f"avg_bundles_by_300ms={report['avg_bundles_by_300_ms']:.2f} "
        f"avg_bundles_by_350ms={report['avg_bundles_by_350_ms']:.2f} "
        f"avg_bundles_by_500ms={report['avg_bundles_by_500_ms']:.2f}"
    )
    for name, detail in report["sql_only_latency"].items():
        print_summary_line(name, detail["summary"])
        print(f"  {name}_histogram {detail['histogram']}")
    realism = report["test_realism"]
    print("test_realism")
    print(
        f"  unique_executed_source_events={realism['unique_executed_source_events']} "
        f"unique_executed_workload_rows={realism['unique_executed_workload_rows']} "
        f"workload_row_reuse_max={realism['workload_row_reuse_max']} cache_states={realism['cache_states']}"
    )
    print(f"  event_mix={realism['event_mix']} hot_field_mix={realism['hot_field_mix']}")
    workload_stats = realism.get("workload_stats") or {}
    if workload_stats:
        print(
            f"  generated_workload_rows={workload_stats.get('event_rows')} "
            f"unique_source_events={workload_stats.get('unique_source_events')} "
            f"unique_binding_sets={workload_stats.get('unique_binding_sets')}"
        )
        print(f"  binding_fields={workload_stats.get('binding_fields')}")
    print("tail_drivers_by_bundle_worker_max_p999")
    for index, row in enumerate(report["tail_drivers"], 1):
        print(
            f"  {index:2d} {row['bundle_id']:<20s} n={row['n']} p95={row['p95']:.1f} "
            f"p99={row['p99']:.1f} p999={row['p999']:.1f} max={row['max']:.1f} "
            f">300={row['over_300']} >350={row['over_350']} >500={row['over_500']}"
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
    sla = report.get("sql_only_sla", {})
    if not isinstance(sla, dict):
        sla = {}
    latency = report.get("sql_only_latency", {})
    if not isinstance(latency, dict):
        latency = {}
    result_files = report.get("result_files") or []
    if not isinstance(result_files, list):
        result_files = []
    cache_states = realism.get("cache_states") or []
    if not isinstance(cache_states, list):
        cache_states = []
    run_shape = report.get("run_shape") or {}
    if not isinstance(run_shape, dict):
        run_shape = {}

    score_ready = sla.get("score_ready_60_of_65", {})
    full_65 = sla.get("full_65_of_65", {})
    if not isinstance(score_ready, dict):
        score_ready = {}
    if not isinstance(full_65, dict):
        full_65 = {}

    event_mix = realism.get("event_mix") or {}
    hot_field_mix = realism.get("hot_field_mix") or {}
    if not isinstance(event_mix, dict):
        event_mix = {}
    if not isinstance(hot_field_mix, dict):
        hot_field_mix = {}

    lines: list[str] = []
    lines.append("# 1000 EPS SQL-Only Event-Level Benchmark Report")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Completed events: {format_count(total_events)}")
    lines.append(f"- Completed event throughput: {format_eps(report.get('completed_eps'))} events/sec")
    lines.append(f"- Fleet app processes: {format_count(run_shape.get('app_processes', len(runs)))}")
    lines.append(f"- Result JSON files merged: {format_count(len(result_files))}")
    lines.append(f"- Cache state labels: {', '.join(cache_states) if cache_states else 'unknown'}")
    lines.append("- Latency scope: SQL-only TiDB-facing bundle runtime.")
    lines.append("")
    lines.append("Important: all customer-facing SLA and latency numbers below use SQL-only TiDB-facing bundle runtime. They exclude client task queue time, prepared statement setup, Go/Python scheduling, event fan-out/fan-in wall time, and application-side aggregation.")
    lines.append("")
    lines.append("## Test Shape")
    lines.append("")
    lines.append(f"- Target event EPS: {format_eps(run_shape.get('target_event_eps'))}")
    lines.append(f"- Duration: {format_eps(run_shape.get('duration_seconds'))} seconds")
    lines.append(f"- Total configured connections: {format_count(run_shape.get('total_connections'))}")
    lines.append(f"- Ready connection workers: {format_count(run_shape.get('ready_workers'))}")
    lines.append(f"- Max pending events per process: {format_count(run_shape.get('max_pending_events_per_process'))}")
    lines.append(f"- Execution modes: {', '.join(run_shape.get('execution_modes', [])) if isinstance(run_shape.get('execution_modes'), list) else 'unknown'}")
    lines.append(f"- Prepared statements enabled on all processes: {run_shape.get('prepare_all', False)}")
    lines.append(f"- Max execution time settings: {run_shape.get('max_execution_time_ms', [])}")
    lines.append(f"- Event stride values: {run_shape.get('event_stride_values', [])}")
    lines.append("")
    lines.append("## Event-Level SLA")
    lines.append("")
    lines.append("| View | <=300ms | <=350ms | <=500ms |")
    lines.append("| --- | ---: | ---: | ---: |")
    lines.append(
        "| Score-ready >=60/65 | "
        f"{markdown_count_rate(score_ready.get('300ms', {}), total_events)} | "
        f"{markdown_count_rate(score_ready.get('350ms', {}), total_events)} | "
        f"{markdown_count_rate(score_ready.get('500ms', {}), total_events)} |"
    )
    lines.append(
        "| Full 65/65 | "
        f"{markdown_count_rate(full_65.get('300ms', {}), total_events)} | "
        f"{markdown_count_rate(full_65.get('350ms', {}), total_events)} | "
        f"{markdown_count_rate(full_65.get('500ms', {}), total_events)} |"
    )
    lines.append("")
    lines.append("## Event Latency")
    lines.append("")
    lines.append("| View | n | p50 | p95 | p99 | max | >300ms | >350ms | >500ms |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    lines.append(markdown_summary_row("Score-ready 60/65", latency.get("score_ready_60_of_65", {})))
    lines.append(markdown_summary_row("Full 65/65", latency.get("full_65_of_65", {})))
    lines.append("")
    lines.append("## Return-Time Histogram")
    lines.append("")
    lines.append("| View | 0-50ms | 50-100ms | 100-150ms | 150-200ms | 200-300ms | 300-350ms | 350-500ms | >500/error |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    lines.append(markdown_histogram_row("Score-ready 60/65", latency.get("score_ready_60_of_65", {})))
    lines.append(markdown_histogram_row("Full 65/65", latency.get("full_65_of_65", {})))
    lines.append("")
    lines.append("## Average Bundles Returned")
    lines.append("")
    lines.append("| Cutoff | Average bundles returned |")
    lines.append("| --- | ---: |")
    lines.append(f"| 300ms | {float(report.get('avg_bundles_by_300_ms', 0.0) or 0.0):.2f}/65 |")
    lines.append(f"| 350ms | {float(report.get('avg_bundles_by_350_ms', 0.0) or 0.0):.2f}/65 |")
    lines.append(f"| 500ms | {float(report.get('avg_bundles_by_500_ms', 0.0) or 0.0):.2f}/65 |")
    lines.append("")
    lines.append("## Binding Reuse / Test Realism")
    lines.append("")
    lines.append(f"- Generated workload rows: {format_count(workload_stats.get('event_rows'))}")
    lines.append(f"- Unique source events in generated workload: {format_count(workload_stats.get('unique_source_events'))}")
    lines.append(f"- Unique full binding sets in generated workload: {format_count(workload_stats.get('unique_binding_sets'))}")
    lines.append(f"- Unique source events executed in this fleet run: {format_count(realism.get('unique_executed_source_events'))}")
    lines.append(f"- Unique workload rows executed in this fleet run: {format_count(realism.get('unique_executed_workload_rows'))}")
    lines.append(f"- Max workload row repeat in this fleet run: {format_count(realism.get('workload_row_reuse_max'))}")
    lines.append(f"- Source event sample reused by generator: {workload_selection.get('source_reused', 'unknown')}")
    lines.append(f"- Unique events required during generation: {workload_selection.get('unique_events_required', 'unknown')}")
    lines.append("")
    if binding_fields:
        lines.append("| Field | Distinct values | Max repeat | Max repeated value |")
        lines.append("| --- | ---: | ---: | --- |")
        for field in sorted(binding_fields):
            stats = binding_fields[field]
            if not isinstance(stats, dict):
                stats = {}
            lines.append(
                f"| {field} | {format_count(stats.get('distinct'))} | "
                f"{format_count(stats.get('max_repeat'))} | `{stats.get('max_value', '')}` |"
            )
        lines.append("")
    lines.append("## Event Mix")
    lines.append("")
    if event_mix:
        for key, value in sorted(event_mix.items()):
            lines.append(f"- {key}: {format_count(value)}")
    else:
        lines.append("- unavailable")
    lines.append("")
    lines.append("## Hot-Key Field Mix")
    lines.append("")
    if hot_field_mix:
        for key, value in sorted(hot_field_mix.items()):
            lines.append(f"- {key}: {format_count(value)}")
    else:
        lines.append("- unavailable")
    lines.append("")
    lines.append("## Tail Drivers")
    lines.append("")
    lines.append("The counts below are bundle executions whose SQL-only TiDB-facing runtime exceeded each cutoff. They are aggregated across all fetched app-process result files.")
    lines.append("")
    lines.append("| Bundle | n | p95 | p99 | p999 | max | >300ms | >350ms | >500ms |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in report.get("tail_drivers", []):
        if not isinstance(row, dict):
            continue
        lines.append(
            f"| {row.get('bundle_id', '')} | {format_count(row.get('n'))} | "
            f"{format_ms(row.get('p95'))} | {format_ms(row.get('p99'))} | "
            f"{format_ms(row.get('p999'))} | {format_ms(row.get('max'))} | "
            f"{format_count(row.get('over_300'))} | {format_count(row.get('over_350'))} | "
            f"{format_count(row.get('over_500'))} |"
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
    ap.add_argument("--output-prefix", default=None)
    args = ap.parse_args()

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
    fleet_report = aggregate_result_jsons(fetched_results) if fetched_results else {}
    if fleet_report:
        print_fleet_customer_report(fleet_report)
        report_path = local_log_dir / f"{output_prefix}_customer_report.md"
        report_path.write_text(render_markdown_report(fleet_report, summaries), encoding="utf-8")
        print(f"customer_report_md={report_path}")
    summary_path.write_text(json.dumps({"elapsed": elapsed, "runs": summaries, "fleet_customer_report": fleet_report}, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")


if __name__ == "__main__":
    main()
