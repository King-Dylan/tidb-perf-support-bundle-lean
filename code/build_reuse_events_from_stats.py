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
import time
from pathlib import Path

import pymysql

from lib.db_config import get_db_config
from mixed_traffic_test import FILTER_FIELDS, fetch_events_for_hot_value, sample_normal_events


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--normal-events", type=int, default=11000)
    ap.add_argument("--hot-events-per-field", type=int, default=100)
    ap.add_argument("--recent-limit", type=int, default=100000)
    ap.add_argument("--normal-candidate-multiplier", type=float, default=25.0)
    ap.add_argument("--output", default="results/reuse_events_hua_fullscale.json")
    args = ap.parse_args()

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
                value, count = collections.Counter(values).most_common(1)[0]
                return value, count, f"recent_top_{args.recent_limit}"

            for table_name, alias, column, field_name in FILTER_FIELDS:
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
                print(
                    f"hot {field_name} value={value} count={count} "
                    f"source={source} events={len(events)}",
                    flush=True,
                )
                hot_events.extend(events)

            normal_events = sample_normal_events(
                cur,
                args.normal_events,
                excluded,
                max_payment_rows=10000,
                max_device_rows=10000,
                validate_counts=False,
                candidate_multiplier=args.normal_candidate_multiplier,
            )
            print(f"normal_events={len(normal_events)} hot_events={len(hot_events)}", flush=True)
    finally:
        conn.close()

    payload = {
        "profile": profile,
        "sampled_normal_events": normal_events,
        "sampled_hot_events": hot_events,
        "normal_candidate_multiplier": args.normal_candidate_multiplier,
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
