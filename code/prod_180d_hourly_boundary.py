#!/usr/bin/env python3 -u
"""Build hourly boundary helpers for exact 180d prod pre-agg queries.

The daily prod180 helpers cover full days after the cutoff day. These hourly
helpers cover the remaining full-hour part of the cutoff day so the online path
only reads the sub-hour raw tail.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.db_config import get_db_config
from optimized_config import PROD180_PREAGG_BUNDLES
from preagg_rollups import (
    all_bundles,
    bundle_rollup_metrics,
    configure_build_session,
    execute_sql,
    is_avg_helper,
    key_not_null_predicates,
    presence_column,
    prod180_distinct_table,
    prod180_hourly_distinct_table,
    prod180_hourly_rollup_table,
    prod180_rollup_metrics,
    quote_ident,
    source_parts,
    sql_literal,
)
from prod_180d_preagg import GROUPS, TABLE_OPTIONS, group_key_selects, metric_columns, parse_groups

sys.stdout.reconfigure(line_buffering=True)


def hour_selects(group: str) -> tuple[list[str], list[str], list[str]]:
    if group == "A":
        return (
            ["CAST(FROM_UNIXTIME(FLOOR(p.event_date / 3600000) * 3600) AS DATETIME) AS event_hour"],
            ["event_hour"],
            ["event_hour"],
        )
    if group == "B":
        return (
            ["CAST(DATE_FORMAT(d.jms_timestamp, '%Y-%m-%d %H:00:00') AS DATETIME) AS event_hour"],
            ["event_hour"],
            ["event_hour"],
        )
    return (
        [
            "CAST(FROM_UNIXTIME(FLOOR(p.event_date / 3600000) * 3600) AS DATETIME) AS p_event_hour",
            "CAST(DATE_FORMAT(d.jms_timestamp, '%Y-%m-%d %H:00:00') AS DATETIME) AS d_event_hour",
        ],
        ["p_event_hour", "d_event_hour"],
        ["p_event_hour", "d_event_hour"],
    )


def create_rollup_sql(group: str) -> str:
    hour_cols = (
        ["p_event_hour DATETIME NOT NULL", "d_event_hour DATETIME NOT NULL"]
        if group == "C"
        else ["event_hour DATETIME NOT NULL"]
    )
    pk_hour_cols = "p_event_hour, d_event_hour" if group == "C" else "event_hour"
    hour_indexes = "INDEX idx_event_hour (p_event_hour, d_event_hour)" if group == "C" else "INDEX idx_event_hour (event_hour)"
    cols = [
        "bundle_id VARCHAR(64) NOT NULL",
        *hour_cols,
        "key1 VARCHAR(128) NOT NULL",
        "key2 VARCHAR(128) NOT NULL DEFAULT ''",
        *metric_columns(group),
    ]
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(prod180_hourly_rollup_table(group))} (
  {",\n  ".join(cols)},
  PRIMARY KEY (bundle_id, key1, key2, {pk_hour_cols}) NONCLUSTERED,
  {hour_indexes}
) {TABLE_OPTIONS};
""".strip()


def create_distinct_sql(group: str) -> str:
    hour_cols = (
        ["p_event_hour DATETIME NOT NULL", "d_event_hour DATETIME NOT NULL"]
        if group == "C"
        else ["event_hour DATETIME NOT NULL"]
    )
    pk_hour_cols = "p_event_hour, d_event_hour" if group == "C" else "event_hour"
    hour_indexes = "INDEX idx_event_hour (p_event_hour, d_event_hour)" if group == "C" else "INDEX idx_event_hour (event_hour)"
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(prod180_hourly_distinct_table(group))} (
  bundle_id VARCHAR(64) NOT NULL,
  template_id VARCHAR(64) NOT NULL,
  {",\n  ".join(hour_cols)},
  key1 VARCHAR(128) NOT NULL,
  key2 VARCHAR(128) NOT NULL DEFAULT '',
  distinct_value VARCHAR(256) NOT NULL,
  PRIMARY KEY (bundle_id, template_id, key1, key2, {pk_hour_cols}, distinct_value) NONCLUSTERED,
  {hour_indexes}
) {TABLE_OPTIONS};
""".strip()


def day_bounds(day: date) -> tuple[int, int, str, str]:
    next_day = day + timedelta(days=1)
    start_dt = datetime.combine(day, datetime.min.time())
    end_dt = datetime.combine(next_day, datetime.min.time())
    return (
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        f"{day.isoformat()} 00:00:00",
        f"{next_day.isoformat()} 00:00:00",
    )


def boundary_day_filter(group: str, day: date, side: str) -> str:
    start_ms, end_ms, start_ts, end_ts = day_bounds(day)
    p_day = f"p.event_date >= {start_ms} AND p.event_date < {end_ms}"
    d_day = f"d.jms_timestamp >= '{start_ts}' AND d.jms_timestamp < '{end_ts}'"
    if group == "A":
        return p_day
    if group == "B":
        return d_day
    if side == "p":
        return p_day
    if side == "d_not_p":
        return f"{d_day} AND NOT ({p_day})"
    raise ValueError(f"unknown build side: {side}")


def selected_items(groups: tuple[str, ...], bundle_ids: set[str] | None) -> list[tuple[str, str, object]]:
    result: list[tuple[str, str, object]] = []
    allowed_groups = set(groups)
    for bundle_id, (group, bundle) in sorted(all_bundles().items()):
        if group not in allowed_groups:
            continue
        if bundle.window_days != 180 or bundle_id not in PROD180_PREAGG_BUNDLES:
            continue
        if bundle_ids and bundle_id not in bundle_ids:
            continue
        result.append((bundle_id, group, bundle))
    return result


def build_rollup_insert_sql(group: str, bundle, build_day: date, side: str) -> str | None:
    rollups = prod180_rollup_metrics(group, bundle)
    if not rollups:
        return None

    _, _, from_sql = source_parts(group)
    key1, key2 = group_key_selects(bundle)
    hour_select_parts, hour_columns, hour_group_parts = hour_selects(group)
    select_parts = [
        f"{sql_literal(bundle.bundle_id)} AS bundle_id",
        *hour_select_parts,
        f"{key1} AS key1",
        f"{key2} AS key2",
    ]
    columns = ["bundle_id", *hour_columns, "key1", "key2"]
    where_parts = key_not_null_predicates(bundle)
    for metric in rollups:
        if is_avg_helper(metric):
            select_parts.append(metric.daily_expr)
            columns.extend([metric.output_column + "__sum", metric.output_column + "__count"])
        else:
            select_parts.append(f"{metric.daily_expr} AS {quote_ident(metric.output_column)}")
            columns.append(metric.output_column)
        if metric.extra_predicate and metric.output_column.startswith("present__"):
            where_parts.append(f"({metric.extra_predicate})")
        elif metric.extra_predicate:
            select_parts.append(
                f"SUM(CASE WHEN {metric.extra_predicate} THEN 1 ELSE 0 END) AS {quote_ident(presence_column(metric.template_id))}"
            )
            columns.append(presence_column(metric.template_id))
    if group in {"A", "C"}:
        where_parts.append("p.event_date IS NOT NULL")
    if group in {"B", "C"}:
        where_parts.append("d.jms_timestamp IS NOT NULL")
    where_parts.append(boundary_day_filter(group, build_day, side))
    return f"""
REPLACE INTO {quote_ident(prod180_hourly_rollup_table(group))} ({", ".join(quote_ident(c) for c in columns)})
SELECT
  {",\n  ".join(select_parts)}
{from_sql}
WHERE {" AND ".join(where_parts)}
GROUP BY {", ".join(hour_group_parts + list(bundle.group_by_fields))};
""".strip()


def build_distinct_insert_sql(group: str, bundle, distinct, build_day: date, side: str) -> str:
    _, _, from_sql = source_parts(group)
    key1, key2 = group_key_selects(bundle)
    hour_select_parts, hour_columns, hour_group_parts = hour_selects(group)
    select_parts = [
        f"{sql_literal(bundle.bundle_id)} AS bundle_id",
        f"{sql_literal(distinct.template_id)} AS template_id",
        *hour_select_parts,
        f"{key1} AS key1",
        f"{key2} AS key2",
        f"CAST({distinct.distinct_expr} AS CHAR(256)) AS distinct_value",
    ]
    where_parts = key_not_null_predicates(bundle)
    where_parts.extend(
        [
            f"{distinct.distinct_expr} IS NOT NULL",
        ]
    )
    if group in {"A", "C"}:
        where_parts.append("p.event_date IS NOT NULL")
    if group in {"B", "C"}:
        where_parts.append("d.jms_timestamp IS NOT NULL")
    where_parts.append(boundary_day_filter(group, build_day, side))
    if distinct.extra_predicate:
        where_parts.append(f"({distinct.extra_predicate})")
    return f"""
REPLACE INTO {quote_ident(prod180_hourly_distinct_table(group))} (
  {", ".join(["bundle_id", "template_id", *hour_columns, "key1", "key2", "distinct_value"])}
)
SELECT
  {",\n  ".join(select_parts)}
{from_sql}
WHERE {" AND ".join(where_parts)}
GROUP BY {", ".join(hour_group_parts + list(bundle.group_by_fields) + ["distinct_value"])};
""".strip()


def analyze_sql(group: str) -> list[str]:
    return [
        f"ANALYZE TABLE {quote_ident(prod180_hourly_rollup_table(group))};",
        f"ANALYZE TABLE {quote_ident(prod180_hourly_distinct_table(group))};",
    ]


def build_one_day(conn, dry_run: bool, build_day: date, groups: tuple[str, ...], bundle_ids: set[str] | None) -> None:
    items = selected_items(groups, bundle_ids)
    print(f"\n===== Building hourly boundary day={build_day.isoformat()} bundles={len(items)} groups={','.join(groups)} =====")
    started = time.perf_counter()
    for bundle_id, group, bundle in items:
        rollups, distincts = bundle_rollup_metrics(group, bundle)
        print(f"-- {bundle_id} rollups={len(rollups)} distincts={len(distincts)}")
        sides = ("p", "d_not_p") if group == "C" else ("base",)
        for side in sides:
            rollup_sql = build_rollup_insert_sql(group, bundle, build_day, side)
            if rollup_sql:
                execute_sql(conn, rollup_sql, dry_run=dry_run)
            for distinct in distincts:
                execute_sql(conn, build_distinct_insert_sql(group, bundle, distinct, build_day, side), dry_run=dry_run)
    print(f"===== Completed hourly boundary day={build_day.isoformat()} in {(time.perf_counter() - started):.1f}s =====\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["drop", "create", "build", "create-build", "analyze", "rebuild"])
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--day", required=False, help="Boundary day to build, YYYY-MM-DD")
    ap.add_argument("--groups", help="Comma-separated groups. Default: A,B,C.")
    ap.add_argument("--bundle", action="append", dest="bundles", help="Bundle id. Defaults to prod180 Group C bundles.")
    args = ap.parse_args()

    groups = parse_groups(args.groups)
    build_day = date.fromisoformat(args.day) if args.day else None
    bundle_ids = set(args.bundles or [])
    dry_run = not args.execute
    conn = None
    if args.execute:
        import pymysql

        conn = pymysql.connect(**get_db_config(save_msg="Group C hourly boundary pre-agg builder"))
        conn.autocommit(True)
        configure_build_session(conn)

    try:
        if args.action in {"build", "create-build", "rebuild"} and build_day is None:
            raise SystemExit("--day is required for hourly boundary build actions.")
        for group in groups:
            if args.action in {"drop", "rebuild"}:
                execute_sql(conn, f"DROP TABLE IF EXISTS {quote_ident(prod180_hourly_rollup_table(group))};", dry_run=dry_run)
                execute_sql(conn, f"DROP TABLE IF EXISTS {quote_ident(prod180_hourly_distinct_table(group))};", dry_run=dry_run)
            if args.action in {"create", "create-build", "rebuild"}:
                execute_sql(conn, create_rollup_sql(group), dry_run=dry_run)
                execute_sql(conn, create_distinct_sql(group), dry_run=dry_run)
        if args.action in {"build", "create-build", "rebuild"}:
            build_one_day(conn, dry_run, build_day, groups, bundle_ids or None)
        if args.action in {"analyze", "rebuild"}:
            for group in groups:
                for sql in analyze_sql(group):
                    execute_sql(conn, sql, dry_run=dry_run)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
