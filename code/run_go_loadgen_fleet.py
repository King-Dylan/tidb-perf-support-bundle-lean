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
        "over_350": sum(1 for v in clean if v > 350),
        "over_500": sum(1 for v in clean if v > 500),
    }


def histogram(values: list[float], total_events: int) -> dict[str, int]:
    buckets = {
        "0-50ms": 0,
        "50-100ms": 0,
        "100-150ms": 0,
        "150-200ms": 0,
        "200-350ms": 0,
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
        elif value <= 350:
            buckets["200-350ms"] += 1
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
        f"max={summary.get('max', 0):.1f} >350={summary.get('over_350', 0)} >500={summary.get('over_500', 0)}"
    )


def aggregate_result_jsons(paths: list[Path]) -> dict[str, object]:
    results = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    event_rows = [row for result in results for row in result.get("event_results", [])]
    total_events = len(event_rows)
    sql_score60 = [float(row.get("sql_score60_ms", -1)) for row in event_rows]
    sql_full65 = [float(row.get("sql_full65_ms", -1)) for row in event_rows]
    bundles_350 = [float(row.get("bundles_by_350_ms", -1)) for row in event_rows if float(row.get("bundles_by_350_ms", -1)) >= 0]
    bundles_500 = [float(row.get("bundles_by_500_ms", -1)) for row in event_rows if float(row.get("bundles_by_500_ms", -1)) >= 0]
    event_mix = Counter(str(row.get("kind") or "<empty>") for row in event_rows)
    hot_field_mix = Counter(str(row.get("hot_field") or "<empty>") for row in event_rows)
    source_events = {str(row.get("source_event")) for row in event_rows if row.get("source_event")}
    workload_indices = [int(row.get("workload_idx", -1)) for row in event_rows if int(row.get("workload_idx", -1)) >= 0]
    workload_stats = next((result.get("workload_stats") for result in results if result.get("workload_stats")), {})
    cache_states = sorted({str(result.get("cache_state") or "unknown") for result in results})

    tail_by_bundle: dict[str, dict[str, float | int | str]] = {}
    for result in results:
        for bundle_id, summary in result.get("bundle_summaries", {}).items():
            row = tail_by_bundle.setdefault(
                bundle_id,
                {"bundle_id": bundle_id, "n": 0, "p95": 0.0, "p99": 0.0, "p999": 0.0, "max": 0.0, "over_350": 0, "over_500": 0},
            )
            row["n"] = int(row["n"]) + int(summary.get("n", 0))
            row["p95"] = max(float(row["p95"]), float(summary.get("p95", 0.0)))
            row["p99"] = max(float(row["p99"]), float(summary.get("p99", 0.0)))
            row["p999"] = max(float(row["p999"]), float(summary.get("p999", 0.0)))
            row["max"] = max(float(row["max"]), float(summary.get("max", 0.0)))
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
        "sql_only_sla": {
            "score_ready_60_of_65": {"350ms": count_rate(sql_score60, 350), "500ms": count_rate(sql_score60, 500)},
            "full_65_of_65": {"350ms": count_rate(sql_full65, 350), "500ms": count_rate(sql_full65, 500)},
        },
        "sql_only_latency": {
            "score_ready_60_of_65": {"summary": summarize(sql_score60), "histogram": histogram(sql_score60, total_events)},
            "full_65_of_65": {"summary": summarize(sql_full65), "histogram": histogram(sql_full65, total_events)},
        },
        "avg_bundles_by_350_ms": (sum(bundles_350) / len(bundles_350)) if bundles_350 else 0.0,
        "avg_bundles_by_500_ms": (sum(bundles_500) / len(bundles_500)) if bundles_500 else 0.0,
        "test_realism": {
            "unique_executed_source_events": len(source_events),
            "unique_executed_workload_rows": len(set(workload_indices)),
            "workload_row_reuse_max": max(Counter(workload_indices).values()) if workload_indices else 0,
            "event_mix": dict(sorted(event_mix.items())),
            "hot_field_mix": dict(sorted(hot_field_mix.items())),
            "cache_states": cache_states,
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
        for cutoff in ("350ms", "500ms"):
            row = sla[name][cutoff]
            print(f"  {name} <= {cutoff}: events={row['events']} pct={row['percent']:.2f}% eps={row['eps']:.2f}")
    print(f"avg_bundles_by_350ms={report['avg_bundles_by_350_ms']:.2f} avg_bundles_by_500ms={report['avg_bundles_by_500_ms']:.2f}")
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
            f">350={row['over_350']} >500={row['over_500']}"
        )


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
    summary_path.write_text(json.dumps({"elapsed": elapsed, "runs": summaries, "fleet_customer_report": fleet_report}, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")


if __name__ == "__main__":
    main()
