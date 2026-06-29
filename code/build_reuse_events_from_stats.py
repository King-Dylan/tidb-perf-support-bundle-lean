#!/usr/bin/env python3
"""Build a reusable mixed-traffic event sample without full-table hot-key scans.

The original benchmark can discover hot values itself, but on the restored
Premium cluster that discovery query can be expensive and can choose TiFlash.
This helper uses TiDB statistics TopN where available, and a bounded recent
indexed sample as fallback, then writes a JSON file that the existing benchmark
can consume via REUSE_EVENTS_JSON.
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import re
import time
from pathlib import Path

import pymysql

from lib.db_config import get_db_config
from mixed_traffic_test import FILTER_FIELDS, fetch_events_for_hot_value, sample_normal_events


KEY_FIELDS = [field_name for _table_name, _alias, _column, field_name in FILTER_FIELDS]


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


def events_from_rate(target_event_eps: float, duration: str) -> int:
    if target_event_eps <= 0:
        raise ValueError("--target-event-eps must be greater than 0")
    seconds = parse_duration_seconds(duration)
    if seconds <= 0:
        raise ValueError("--duration must be greater than 0")
    return int(math.ceil(target_event_eps * seconds))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--normal-events", type=int, default=11000)
    ap.add_argument("--hot-events-per-field", type=int, default=100)
    ap.add_argument(
        "--target-workload-events",
        type=int,
        default=0,
        help="If set, size the source pool for a final no-reuse workload of this many event executions.",
    )
    ap.add_argument("--target-event-eps", type=float, default=0.0, help="Fleet-wide target event EPS used to compute --target-workload-events.")
    ap.add_argument("--duration", default="", help="Run duration used to compute --target-workload-events, for example 300s, 5m, or 10m.")
    # 0.07 measured from cluster stats (2026-06-26): ~5-7% of events sit on a hot
    # key (>10k occurrences), dominated by check_bank_routing_number (~4.3%). 0.07
    # is the rounded-up (conservative) event-hot rate. NOTE: field mix is still even
    # here; real hot traffic is ~57% routing-driven (see FINAL_RUN_CHECKLIST.md).
    ap.add_argument("--hot-event-pct", type=float, default=0.07)
    ap.add_argument("--recent-limit", type=int, default=100000)
    ap.add_argument(
        "--min-hot-frequency",
        type=int,
        default=50,
        help="Minimum occurrence count for a value to be treated as a real hot key. "
        "Applied to fallback (recent-window) hot keys when SHOW STATS_TOPN has no entry; "
        "the builder fails loudly rather than fabricating a hot key from a value that is not actually hot.",
    )
    ap.add_argument("--max-payment-rows", type=int, default=10000)
    ap.add_argument("--max-device-rows", type=int, default=10000)
    ap.add_argument("--validate-normal-counts", action="store_true")
    ap.add_argument(
        "--allow-partial-hot-fields",
        action="store_true",
        help="Do not fail if a hot field returns fewer rows than requested. Use only for diagnostics/smoke tests.",
    )
    ap.add_argument(
        "--no-hot-field",
        action="append",
        default=[],
        help="Field(s) to exclude from hot-key injection (high-cardinality fields with no real hot key); they stay unique in normal events.",
    )
    ap.add_argument("--output", default="results/reuse_events_hua_fullscale.json")
    args = ap.parse_args()

    unknown_no_hot = [f for f in args.no_hot_field if f not in KEY_FIELDS]
    if unknown_no_hot:
        raise ValueError(
            f"--no-hot-field value(s) {unknown_no_hot!r} are not known field names. "
            f"Valid field names are: {', '.join(KEY_FIELDS)}."
        )

    computed_target = 0
    if args.target_event_eps > 0 or args.duration:
        if not (args.target_event_eps > 0 and args.duration):
            raise ValueError("--target-event-eps and --duration must be provided together")
        computed_target = events_from_rate(args.target_event_eps, args.duration)
        if args.target_workload_events and args.target_workload_events != computed_target:
            raise ValueError(
                f"--target-workload-events ({args.target_workload_events:,}) does not match "
                f"--target-event-eps * --duration ({computed_target:,}). Omit --target-workload-events to avoid mismatch."
            )
        args.target_workload_events = computed_target

    if args.target_workload_events:
        hot_stride = int(round(1 / args.hot_event_pct)) if args.hot_event_pct > 0 else 0
        hot_needed = (
            sum(1 for idx in range(args.target_workload_events) if hot_stride > 0 and idx % hot_stride == 0)
            if hot_stride > 0
            else 0
        )
        normal_needed = args.target_workload_events - hot_needed
        args.normal_events = max(args.normal_events, normal_needed)
        if hot_needed:
            args.hot_events_per_field = max(args.hot_events_per_field, math.ceil(hot_needed / max(1, len([f for f in KEY_FIELDS if f not in args.no_hot_field]))))
        print(
            "target sizing "
            f"target_workload_events={args.target_workload_events} "
            f"hot_event_pct={args.hot_event_pct} "
            f"normal_events={args.normal_events} "
            f"hot_events_per_field={args.hot_events_per_field}",
            flush=True,
        )

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    print("START", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), flush=True)
    conn = pymysql.connect(**get_db_config(save_msg="build reusable event sample"))
    conn.autocommit(True)
    profile = {"hot_fields": {}}
    hot_events = []
    excluded = {}

    try:
        with conn.cursor() as cur:
            # Force the sampler to TiKV so event selection does not consume TiFlash memory.
            cur.execute("SET SESSION tidb_isolation_read_engines='tikv'")
            cur.execute("SHOW STATS_TOPN WHERE Db_name = DATABASE()")
            rows = cur.fetchall()
            topn = {}
            for _db, table, _part, col, _is_idx, val, cnt in rows:
                key = (table, col)
                if key not in topn or int(cnt) > topn[key][1]:
                    topn[key] = (str(val), int(cnt), "SHOW STATS_TOPN")

            def fallback_top(table: str, column: str) -> tuple[str, int, str]:
                date_col = "event_date" if table == "pmt_txn_fact" else "jms_timestamp"
                cur.execute(
                    f"""
                    SELECT {column}
                    FROM {table}
                    WHERE {column} IS NOT NULL
                    ORDER BY {date_col} DESC
                    LIMIT %s
                    """,
                    (args.recent_limit,),
                )
                values = [str(row[0]) for row in cur.fetchall()]
                if not values:
                    raise RuntimeError(
                        f"fallback hot-key lookup for {table}.{column} returned no rows; "
                        "cannot derive a hot key. Exclude this field with --no-hot-field "
                        "or provide a source pool with data."
                    )
                value, count = collections.Counter(values).most_common(1)[0]
                if count < args.min_hot_frequency:
                    raise RuntimeError(
                        f"fallback hot-key for {table}.{column} value={value!r} occurs only "
                        f"{count:,} time(s) within the most recent {args.recent_limit:,} rows, "
                        f"below --min-hot-frequency ({args.min_hot_frequency:,}). This value is not "
                        "actually hot; refusing to fabricate a hot key. Exclude this field with "
                        "--no-hot-field, or lower --min-hot-frequency only if you have verified it is hot."
                    )
                # NOTE: this count is the frequency within the recent-window sample, NOT a
                # table-wide row count. The source label below makes that explicit so the report
                # does not present it as an authoritative table-wide hot-key count.
                return value, count, f"recent_window_count_top_{args.recent_limit}"

            for table_name, alias, column, field_name in FILTER_FIELDS:
                if field_name in args.no_hot_field:
                    continue
                table = "pmt_txn_fact" if alias == "p" else "deviceprofile_fact"
                value, count, source = topn.get((table, column)) or fallback_top(table, column)
                excluded[field_name] = value
                profile["hot_fields"][field_name] = {
                    "table": table_name,
                    "value": value,
                    "count": count,
                    "source": source,
                }
                events = fetch_events_for_hot_value(
                    cur, alias, column, value, field_name, count, args.hot_events_per_field
                )
                if not args.allow_partial_hot_fields and len(events) < args.hot_events_per_field:
                    raise RuntimeError(
                        f"hot field {field_name} returned only {len(events):,} events; "
                        f"requested {args.hot_events_per_field:,}. "
                        "Do not run the final benchmark until the source pool covers all hot fields."
                    )
                print(
                    f"hot {field_name} value={value} count={count} "
                    f"source={source} events={len(events)}",
                    flush=True,
                )
                hot_events.extend(events)

            normal_events = []
            if args.normal_events > 0:
                normal_events = sample_normal_events(
                    cur,
                    args.normal_events,
                    excluded,
                    max_payment_rows=args.max_payment_rows,
                    max_device_rows=args.max_device_rows,
                    validate_counts=args.validate_normal_counts,
                )
            else:
                print("normal_events=0 requested; skipping normal sampler", flush=True)
            if len(normal_events) < args.normal_events:
                raise RuntimeError(
                    f"normal sampler returned only {len(normal_events):,} events; "
                    f"requested {args.normal_events:,}. "
                    "Increase the candidate limits/source data, or do not run the final benchmark."
                )
            print(f"normal_events={len(normal_events)} hot_events={len(hot_events)}", flush=True)
    finally:
        conn.close()

    payload = {
        "profile": profile,
        "sampled_normal_events": normal_events,
        "sampled_hot_events": hot_events,
        "source_pool_stats": {
            "normal_events": len(normal_events),
            "hot_events": len(hot_events),
            "hot_events_by_field": collections.Counter(event.get("hot_field") for event in hot_events),
            "target_workload_events": args.target_workload_events,
            "target_event_eps": args.target_event_eps,
            "duration": args.duration,
            "computed_target_workload_events": computed_target,
            "hot_event_pct": args.hot_event_pct,
            "requested_hot_events_per_field": args.hot_events_per_field,
            "validate_normal_counts": args.validate_normal_counts,
        },
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "note": (
            "Reusable event sample for QPS ladder; hot values sourced from TiDB "
            "SHOW STATS_TOPN where available, otherwise recent indexed TiKV sample."
        ),
    }
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print("WROTE", out, "bytes", out.stat().st_size, flush=True)
    print("STOP", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), flush=True)


if __name__ == "__main__":
    main()
