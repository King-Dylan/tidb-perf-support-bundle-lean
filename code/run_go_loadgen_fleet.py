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
import re
import subprocess
import sys
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


def parse_duration_seconds(raw: str) -> float:
    text = raw.strip().lower()
    if not text:
        raise ValueError("duration cannot be empty")
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)?", text)
    if not match:
        raise ValueError(f"unsupported duration {raw!r}; use values like 300s, 5m, 10m, or 1h")
    value = float(match.group(1))
    unit = match.group(2) or "s"
    if unit == "ms":
        return value / 1000.0
    if unit == "s":
        return value
    if unit == "m":
        return value * 60.0
    if unit == "h":
        return value * 3600.0
    raise ValueError(f"unsupported duration unit {unit!r}")


def normalize_go_duration(raw: str) -> str:
    """Return a Go-flag.Duration-acceptable string.

    Go's flag.Duration rejects a bare number like "300" (it requires a unit
    suffix). parse_duration_seconds tolerates a bare number by assuming seconds,
    so without normalization a bare value passes local validation but every
    remote host rejects it. Validate the format and append "s" to a unitless
    integer/decimal before forwarding.
    """
    text = raw.strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)?", text)
    if not match:
        raise ValueError(f"unsupported duration {raw!r}; use values like 300s, 5m, 10m, or 1h")
    if match.group(2) is None:
        return f"{match.group(1)}s"
    return text


def events_from_rate(target_event_eps: float, duration: str) -> int:
    if target_event_eps <= 0:
        raise ValueError("--target-event-eps must be greater than 0")
    seconds = parse_duration_seconds(duration)
    if seconds <= 0:
        raise ValueError("--duration must be greater than 0")
    return int(math.ceil(target_event_eps * seconds))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hosts", required=True, help="Comma/newline separated SSH hosts, e.g. ec2-user@host1,ec2-user@host2")
    ap.add_argument("--ssh-key", required=True)
    ap.add_argument("--remote-dir", default="~/tidb_intuit_perf_support_bundle_lean/code")
    ap.add_argument("--workload", default=None, help="Path to the workload JSON. Required: pass the exact file the checklist produced; there is no safe default.")
    ap.add_argument("--db-config", default=".db_config.json")
    ap.add_argument("--events-total", type=int, default=0, help="Fleet-wide event count. In steady mode, computed as target EPS * duration when omitted.")
    ap.add_argument("--connections-total", type=int, default=1300)
    ap.add_argument("--processes-per-host", type=int, default=1)
    ap.add_argument("--read-timeout", default="5s")
    ap.add_argument("--setup-timeout", default="60s")
    ap.add_argument("--query-timeout", default="0s")
    ap.add_argument("--max-execution-time-ms", type=int, default=500,
                    help="Server-side per-query cap (TiDB max_execution_time, ms). Default 500 matches Intuit's "
                         "outer 500ms SLA deadline: a bundle exceeding it is aborted server-side and counts as a "
                         "miss, freeing its connection so the slow tail can't starve other events. Set 0 to "
                         "disable for an uncapped diagnostic run.")
    ap.add_argument("--execution-mode", default="conn-fanout", choices=("worker-pool", "event-fanout", "conn-fanout"),
                    help="Default conn-fanout: real backpressure across the pool, clean SQL-only timing. "
                         "event-fanout has no backpressure and folds connection-wait into query time.")
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

    if not args.workload:
        print(
            "ERROR: --workload is required. Pass the exact workload JSON file the checklist "
            "produced (the same file present under --remote-dir on every host). There is no "
            "safe default: a guessed path would let the run and the report describe different "
            "workloads.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(2)

    # Normalize --duration to a Go-flag.Duration-acceptable string before it is
    # forwarded to every remote host. A bare integer like "300" passes local
    # validation but Go's flag.Duration rejects it; append "s" / require a unit.
    args.duration = normalize_go_duration(args.duration)

    steady_mode = args.target_event_eps > 0
    computed_events_total = 0
    if steady_mode:
        computed_events_total = events_from_rate(args.target_event_eps, args.duration)
        if args.events_total and args.events_total != computed_events_total:
            # In steady mode the actual event count is derived from EPS*duration;
            # --events-total is only a hint. An over-estimate (>= computed) is
            # harmless, so warn instead of aborting. An under-estimate would
            # silently cap the run short of the requested duration, so reject it.
            if args.events_total < computed_events_total:
                raise ValueError(
                    f"--events-total ({args.events_total:,}) is LESS than --target-event-eps * --duration "
                    f"({computed_events_total:,}) and would cut the steady run short. "
                    "Omit --events-total (recommended in steady mode) or raise it."
                )
            print(
                f"WARNING: --events-total ({args.events_total:,}) differs from --target-event-eps * --duration "
                f"({computed_events_total:,}); in steady mode the event count is derived from EPS*duration, "
                f"so {computed_events_total:,} will be used. --events-total is ignored as a hint here.",
                flush=True,
            )
        args.events_total = computed_events_total
    elif args.events_total <= 0:
        args.events_total = 1000

    hosts = parse_hosts(args.hosts)
    workers = [(host, proc_idx) for host in hosts for proc_idx in range(args.processes_per_host)]
    if not workers:
        print(
            f"ERROR: resolved worker list is empty (hosts={len(hosts)}, "
            f"--processes-per-host={args.processes_per_host}). --processes-per-host must be >= 1 "
            "so each host runs at least one worker.",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(2)
    run_id = int(time.time())
    output_prefix = args.output_prefix or f"go_fleet_{run_id}"
    events_each = math.ceil(args.events_total / len(workers))
    conns_each = math.ceil(args.connections_total / len(workers))
    start_at_unix_ms = 0
    if args.start_delay_seconds > 0:
        start_at_unix_ms = int((time.time() + args.start_delay_seconds) * 1000)
        print(f"global start_at_unix_ms={start_at_unix_ms} delay={args.start_delay_seconds:.1f}s")
    target_eps_each = args.target_event_eps / len(workers) if args.target_event_eps > 0 else 0.0
    if steady_mode:
        print(
            f"steady run sizing: target_event_eps={args.target_event_eps} duration={args.duration} "
            f"computed_events_total={args.events_total} workers={len(workers)} "
            f"events_per_worker~={events_each}"
        )
        if args.events_total % len(workers) != 0:
            print(
                "WARNING: computed_events_total is not evenly divisible by worker count. "
                "A few event indexes can wrap when event-stride is used. For the final customer run, "
                "prefer EPS*duration divisible by host_count*processes_per_host.",
                flush=True,
            )

    # Surface the resolved workload path and warn loudly if it is missing
    # locally. The orchestrator runs the Go binary against args.workload on the
    # REMOTE host, but the report is generated against the named workload, so a
    # stale/typo'd default would have the report vouch for a workload the run
    # never used.
    local_workload = Path(args.workload)
    if local_workload.exists():
        print(f"workload (local copy resolved): {local_workload.resolve()}", flush=True)
    else:
        print(
            f"WARNING: --workload {args.workload!r} not found locally. Verify the SAME file exists "
            f"under {args.remote_dir!r} on every host; otherwise the run and the report may describe "
            "different workloads. Pass --workload explicitly to be safe.",
            flush=True,
        )

    # Backpressure / connection-provisioning guards. event-fanout has no
    # connection-wait separation: with no pending cap, in-flight event goroutines
    # pile up and connection-acquisition wait gets folded into the measured
    # query latency, re-creating the prior client-side starvation artifact.
    if args.execution_mode == "event-fanout":
        if args.max_pending_events <= 0 and steady_mode:
            print(
                "WARNING: execution-mode=event-fanout with --max-pending-events=0 has NO backpressure. "
                "In-flight events can pile up unboundedly and connection-acquisition wait is folded into "
                "the reported query latency (client-side starvation, not DB latency). For customer runs "
                "prefer --execution-mode conn-fanout, or set --max-pending-events ~= --connections-total.",
                flush=True,
            )

    # Connection under-provisioning guard. Under-provisioning inflates wall-clock
    # latency with client-side connection-pool waiting (not DB latency), and this
    # applies to conn-fanout (the recommended customer mode) just as much as to
    # event-fanout. Evaluate it for both modes, not only event-fanout.
    if args.execution_mode in ("event-fanout", "conn-fanout"):
        # Per-process bundled-SQL QPS = (per-process EPS) * 65 bundles.
        if steady_mode and conns_each > 0:
            per_proc_eps = args.target_event_eps / len(workers)
            per_proc_bundle_qps = per_proc_eps * 65.0
            # Heuristic: at least 1 connection per ~50 bundled queries/sec/process.
            min_conns = math.ceil(per_proc_bundle_qps / 50.0)
            if conns_each < min_conns:
                print(
                    f"WARNING: connections-per-process ({conns_each}) looks low for "
                    f"~{per_proc_bundle_qps:,.0f} bundled SQL/sec/process (65x fan-out). "
                    f"Consider raising --connections-total so each process gets >= ~{min_conns} "
                    "connections. Under-provisioning inflates wall-clock latency "
                    "with client-side connection-pool waiting, not DB latency.",
                    flush=True,
                )

    # --max-pending-events is a PER-PROCESS cap and is forwarded unscaled to every
    # worker, so the fleet-wide pending ceiling is (value * app_processes).
    if args.max_pending_events > 0:
        print(
            f"NOTE: --max-pending-events {args.max_pending_events} is a PER-PROCESS cap; "
            f"the effective fleet-wide pending ceiling is {args.max_pending_events * len(workers):,} "
            f"across {len(workers)} worker(s). Size accordingly.",
            flush=True,
        )

    procs: list[tuple[subprocess.Popen[str], Path, str]] = []
    local_log_dir = Path("results")
    local_log_dir.mkdir(exist_ok=True)
    for index, (host, proc_idx) in enumerate(workers):
        if steady_mode:
            # go-loadgen computes the actual steady-mode event count from the
            # per-process target EPS and duration. Pass the per-worker event
            # count as a consistency hint so logs and future binary versions do
            # not look like every process is expected to run the full fleet size.
            events = events_each
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
            f"--event-offset {index} "
            f"--event-stride {len(workers)} "
            f"--target-event-eps {target_eps_each:.6f} "
            f"--duration {args.duration} "
            f"--max-pending-events {args.max_pending_events} "
            f"--start-at-unix-ms {start_at_unix_ms} "
            f"--omit-event-results=false "
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
    failed_hosts: list[dict] = []
    for index, (proc, log, host) in enumerate(procs):
        rc = proc.wait()
        print(f"finished host={host} log={log} rc={rc}")
        if rc != 0:
            print(f"WARNING: host={host} worker={index} exited non-zero rc={rc}", flush=True)
            failed_hosts.append({"host": host, "worker": index, "reason": f"exit rc={rc}"})
    elapsed = time.time() - started
    print(f"fleet wall elapsed={elapsed:.3f}s")

    summaries = []
    for index, (_, log, host) in enumerate(procs):
        text = log.read_text(errors="replace")
        print(f"--- {host} {log}")
        for prefix in ("Workers ready=", "elapsed=", "event_completion", "full_65_of_65", "query_runtime", "task_queue", "Saved:"):
            lines = [line for line in text.splitlines() if line.startswith(prefix)]
            if lines:
                print(lines[-1])
        saved = [line.split("Saved:", 1)[1].strip() for line in text.splitlines() if line.startswith("Saved:")]
        if not saved:
            print(f"WARNING: host={host} worker={index} produced NO 'Saved:' result line (crashed before writing?)", flush=True)
            # Avoid double-listing if we already flagged a non-zero rc for this worker.
            if not any(f.get("worker") == index for f in failed_hosts):
                failed_hosts.append({"host": host, "worker": index, "reason": "no saved result"})
        if saved:
            remote_result = saved[-1]
            local_result = local_log_dir / f"{output_prefix}_{index}.json"
            remote_path = f"{host}:{args.remote_dir.rstrip('/')}/{remote_result}"
            scp_cmd = [
                "scp",
                "-i",
                args.ssh_key,
                "-o",
                "StrictHostKeyChecking=no",
                remote_path,
                str(local_result),
            ]
            try:
                subprocess.run(scp_cmd, check=True)
                summaries.append(
                    {
                        "host": host,
                        "remote_result": remote_result,
                        "local_result": str(local_result),
                        "log": str(log),
                    }
                )
            except subprocess.CalledProcessError as exc:
                print(f"failed to fetch {remote_path}: {exc}")
                summaries.append({"host": host, "remote_result": remote_result, "log": str(log), "fetch_error": str(exc)})
                if not any(f.get("worker") == index for f in failed_hosts):
                    failed_hosts.append({"host": host, "worker": index, "reason": f"fetch failed: {exc}"})
    summary_path = local_log_dir / f"{output_prefix}_summary.json"
    duration_seconds = parse_duration_seconds(args.duration) if steady_mode else 0.0
    summary_path.write_text(
        json.dumps(
            {
                "fleet_summary": True,
                # run_shape carries the authoritative FLEET-WIDE shape so the
                # customer report never has to (incorrectly) infer fleet totals
                # from a single per-process result. target_event_eps and
                # total_connections are the true fleet totals the operator
                # requested; per-process values were derived by dividing these.
                "run_shape": {
                    "target_event_eps": args.target_event_eps,
                    "total_connections": args.connections_total,
                    "app_processes": len(workers),
                    "hosts": len(hosts),
                    "duration_seconds": duration_seconds,
                    "execution_mode": args.execution_mode,
                    "max_pending_events_per_process": args.max_pending_events,
                },
                "elapsed": elapsed,
                "target_event_eps": args.target_event_eps,
                "duration": args.duration,
                "computed_events_total": computed_events_total,
                "events_total": args.events_total,
                "failed_hosts": failed_hosts,
                "runs": summaries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"summary={summary_path}")
    print(
        "NOTE: pass this summary to the report with "
        f"--fleet-summary {summary_path} so fleet-wide target EPS / connections / "
        "app_processes are reported correctly.",
        flush=True,
    )
    if failed_hosts:
        print(
            "==================================================================\n"
            f"FAILED HOSTS: {len(failed_hosts)} of {len(workers)} worker(s) did not "
            "produce a saved result. The merged report will cover a REDUCED fleet.\n"
            f"  {failed_hosts}\n"
            "Do NOT present these numbers as a full-scale result without re-running "
            "the failed hosts.\n"
            "==================================================================",
            flush=True,
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
