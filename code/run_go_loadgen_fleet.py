#!/usr/bin/env python3
"""Run the Go load generator across multiple SSH clients.

This script assumes each host already has the support bundle under
--remote-dir.  It is intentionally small: orchestration should not become
another benchmark bottleneck.
"""

from __future__ import annotations

import argparse
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
            events = args.events_total
        else:
            events = min(events_each, max(0, args.events_total - index * events_each))
        if not steady_mode and events <= 0:
            continue
        remote_output = f"results/{output_prefix}_{index}.json"
        prepare = "--prepare-all=true" if args.prepare_all else "--prepare-all=false"
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
        print(f"start worker={index} host={host} proc={proc_idx} events={events} conns={conns_each} target_eps={target_eps_each:.3f}")
        procs.append((subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, text=True), local_log, host))

    started = time.time()
    for proc, log, host in procs:
        rc = proc.wait()
        print(f"finished host={host} log={log} rc={rc}")
    elapsed = time.time() - started
    print(f"fleet wall elapsed={elapsed:.3f}s")

    summaries = []
    for _, log, host in procs:
        text = log.read_text(errors="replace")
        print(f"--- {host} {log}")
        for prefix in ("Workers ready=", "elapsed=", "event_completion", "full_65_of_65", "query_runtime", "task_queue", "Saved:"):
            lines = [line for line in text.splitlines() if line.startswith(prefix)]
            if lines:
                print(lines[-1])
        saved = [line.split("Saved:", 1)[1].strip() for line in text.splitlines() if line.startswith("Saved:")]
        if saved:
            summaries.append({"host": host, "remote_result": saved[-1], "log": str(log)})
    summary_path = local_log_dir / f"{output_prefix}_summary.json"
    summary_path.write_text(json.dumps({"elapsed": elapsed, "runs": summaries}, indent=2), encoding="utf-8")
    print(f"summary={summary_path}")


if __name__ == "__main__":
    main()
