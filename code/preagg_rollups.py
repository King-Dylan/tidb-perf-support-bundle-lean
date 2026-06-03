#!/usr/bin/env python3
"""Create/build exact daily rollups for selected Intuit bundles.

The rollup model is bundle-oriented on purpose. Each selected 30d+ bundle gets:

1. a daily summary table for reducible metrics such as COUNT/SUM/MIN/MAX; and
2. one distinct-helper table per COUNT(DISTINCT ...) metric.

This keeps the prototype honest: we do not sum daily distinct counts, because
that would over-count values that appear on multiple days.
"""

from __future__ import annotations

import argparse
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from demo import (
    build_group_a_metric_expr,
    build_group_b_metric_expr,
    build_group_c_metric_expr,
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
    metric_column,
    presence_column,
)


DEFAULT_BUNDLES = [
    # Group A hot payment paths.
    "group_a_bundle_010",
    "group_a_bundle_012",
    "group_a_bundle_014",
    "group_a_bundle_016",
    "group_a_bundle_018",
    "group_a_bundle_020",
    # Group B hot device paths.
    "group_b_bundle_009",
    "group_b_bundle_010",
    "group_b_bundle_011",
    "group_b_bundle_012",
    "group_b_bundle_013",
    "group_b_bundle_014",
    "group_b_bundle_015",
    "group_b_bundle_016",
    "group_b_bundle_017",
    "group_b_bundle_018",
    "group_b_bundle_019",
    "group_b_bundle_020",
    # Group C joined hot paths.
    "group_c_bundle_015",
    "group_c_bundle_016",
    "group_c_bundle_017",
    "group_c_bundle_018",
    "group_c_bundle_019",
    "group_c_bundle_020",
    "group_c_bundle_021",
    "group_c_bundle_022",
    "group_c_bundle_023",
    "group_c_bundle_024",
    "group_c_bundle_025",
]


@dataclass(frozen=True)
class RollupMetric:
    template_id: str
    output_column: str
    daily_expr: str
    combine_expr: str
    extra_predicate: str | None


@dataclass(frozen=True)
class DistinctMetric:
    template_id: str
    output_column: str
    distinct_expr: str
    extra_predicate: str | None


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def safe_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_").lower()


def all_bundles() -> dict[str, tuple[str, Any]]:
    result: dict[str, tuple[str, Any]] = {}
    for group, bundles in (
        ("A", cluster_group_a_templates()),
        ("B", cluster_group_b_templates()),
        ("C", cluster_group_c_templates()),
    ):
        for bundle in bundles:
            result[bundle.bundle_id] = (group, bundle)
    return result


def table_prefix(bundle_id: str) -> str:
    return f"preagg_{safe_name(bundle_id)}"


def daily_table(bundle_id: str) -> str:
    return f"{table_prefix(bundle_id)}_daily"


def distinct_table(bundle_id: str, template_id: str) -> str:
    return f"{table_prefix(bundle_id)}_{safe_name(template_id)}_distinct"


def key_fields(bundle) -> list[str]:
    fields: list[str] = []
    for field in bundle.group_by_fields:
        if "." not in field:
            raise ValueError(f"Unexpected group-by field {field!r} for {bundle.bundle_id}")
        alias, col = field.split(".", 1)
        fields.append(col)
    return fields


def source_parts(group: str) -> tuple[str, str, str]:
    if group == "A":
        return ("p", "DATE(FROM_UNIXTIME(p.event_date / 1000))", "FROM pmt_txn_fact p")
    if group == "B":
        return ("d", "DATE(d.jms_timestamp)", "FROM deviceprofile_fact d")
    if group == "C":
        return (
            "p",
            "DATE(FROM_UNIXTIME(p.event_date / 1000))",
            "FROM pmt_txn_fact p JOIN deviceprofile_fact d ON p.parsed_interaction_id = d.interaction_id",
        )
    raise ValueError(f"Unknown group {group}")


def metric_expr(group: str, tmpl) -> str:
    if group == "A":
        return build_group_a_metric_expr(tmpl)
    if group == "B":
        return build_group_b_metric_expr(tmpl)
    if group == "C":
        return build_group_c_metric_expr(tmpl)
    raise ValueError(f"Unknown group {group}")


def classify_template(group: str, tmpl) -> tuple[RollupMetric | None, DistinctMetric | None]:
    expr = tmpl.select_expr.strip()
    output = metric_column(tmpl.template_id)
    cond = tmpl.extra_predicate

    if re.fullmatch(r"COUNT\(DISTINCT\((.*?)\)\)", expr, re.I):
        distinct_expr = re.fullmatch(r"COUNT\(DISTINCT\((.*?)\)\)", expr, re.I).group(1).strip()
        return None, DistinctMetric(tmpl.template_id, output, distinct_expr, cond)

    if expr == "COUNT(*)":
        daily_expr = metric_expr(group, tmpl)
        return RollupMetric(tmpl.template_id, output, daily_expr, f"SUM({quote_ident(output)})", cond), None

    if expr == "SUM(p.amount)":
        daily_expr = metric_expr(group, tmpl)
        return RollupMetric(tmpl.template_id, output, daily_expr, f"SUM({quote_ident(output)})", cond), None

    if expr == "MIN(p.amount)" or re.fullmatch(r"MIN\(CAST\(.*?\)\)", expr, re.I):
        daily_expr = metric_expr(group, tmpl)
        return RollupMetric(tmpl.template_id, output, daily_expr, f"MIN({quote_ident(output)})", cond), None

    if expr == "MAX(p.amount)" or re.fullmatch(r"MAX\(CAST\(.*?\)\)", expr, re.I):
        daily_expr = metric_expr(group, tmpl)
        return RollupMetric(tmpl.template_id, output, daily_expr, f"MAX({quote_ident(output)})", cond), None

    avg_match = re.fullmatch(r"AVG\((CAST\(.*?\))\)", expr, re.I)
    if avg_match:
        # AVG is represented as two daily columns so the final query can combine
        # exactly: SUM(avg_sum) / SUM(avg_count).
        base = avg_match.group(1)
        if cond:
            sum_expr = f"SUM(CASE WHEN {cond} THEN {base} END)"
            count_expr = f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END)"
        else:
            sum_expr = f"SUM({base})"
            count_expr = "COUNT(*)"
        return RollupMetric(
            tmpl.template_id,
            output,
            f"{sum_expr} AS {quote_ident(output + '__sum')}, {count_expr} AS {quote_ident(output + '__count')}",
            f"SUM({quote_ident(output + '__sum')}) / NULLIF(SUM({quote_ident(output + '__count')}), 0)",
            cond,
        ), None

    raise ValueError(f"Unsupported metric expression for {tmpl.template_id}: {expr}")


def bundle_rollup_metrics(group: str, bundle) -> tuple[list[RollupMetric], list[DistinctMetric]]:
    rollups: list[RollupMetric] = []
    distincts: list[DistinctMetric] = []
    for tmpl in bundle.templates:
        rollup, distinct = classify_template(group, tmpl)
        if rollup:
            rollups.append(rollup)
        if distinct:
            distincts.append(distinct)
    return rollups, distincts


def distinct_presence_rollup_metrics(group: str, bundle) -> list[RollupMetric]:
    """Presence columns emitted with filtered COUNT(DISTINCT ...) raw queries.

    The raw query builder emits both metric__X and present__X when a distinct
    metric has an extra predicate. The distinct helper table stores unique
    values, so the companion presence count must be represented as a reducible
    daily rollup.
    """
    metrics: list[RollupMetric] = []
    for tmpl in bundle.templates:
        _, distinct = classify_template(group, tmpl)
        if not distinct or not distinct.extra_predicate:
            continue
        output = presence_column(tmpl.template_id)
        metrics.append(
            RollupMetric(
                tmpl.template_id,
                output,
                "COUNT(*)",
                f"SUM({quote_ident(output)})",
                distinct.extra_predicate,
            )
        )
    return metrics


def prod180_rollup_metrics(group: str, bundle) -> list[RollupMetric]:
    rollups, _ = bundle_rollup_metrics(group, bundle)
    return rollups + distinct_presence_rollup_metrics(group, bundle)


def is_avg_helper(metric: RollupMetric) -> bool:
    """AVG rollups are stored as daily sum/count helper columns."""
    return (
        quote_ident(metric.output_column + "__sum") in metric.daily_expr
        and quote_ident(metric.output_column + "__count") in metric.daily_expr
    )


def create_daily_table_sql(group: str, bundle) -> str | None:
    rollups, _ = bundle_rollup_metrics(group, bundle)
    if not rollups:
        return None
    keys = key_fields(bundle)
    cols = ["event_day DATE NOT NULL"] + [f"{quote_ident(k)} VARCHAR(128) NOT NULL" for k in keys]
    metric_cols: list[str] = []
    for metric in rollups:
        if is_avg_helper(metric):
            metric_cols.append(f"{quote_ident(metric.output_column + '__sum')} DECIMAL(38,6) DEFAULT NULL")
            metric_cols.append(f"{quote_ident(metric.output_column + '__count')} BIGINT DEFAULT NULL")
        elif metric.daily_expr.startswith("COUNT") or metric.daily_expr.startswith("SUM(CASE"):
            metric_cols.append(f"{quote_ident(metric.output_column)} DECIMAL(38,6) DEFAULT NULL")
        else:
            metric_cols.append(f"{quote_ident(metric.output_column)} DECIMAL(38,6) DEFAULT NULL")
        if metric.extra_predicate and not metric.output_column.startswith("present__"):
            metric_cols.append(f"{quote_ident(presence_column(metric.template_id))} BIGINT DEFAULT NULL")
    cols.extend(metric_cols)
    pk_cols = ", ".join([quote_ident(k) for k in keys] + ["event_day"])
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(daily_table(bundle.bundle_id))} (
  {",\n  ".join(cols)},
  PRIMARY KEY ({pk_cols})
);
""".strip()


def create_distinct_table_sql(bundle, distinct: DistinctMetric) -> str:
    keys = key_fields(bundle)
    cols = ["event_day DATE NOT NULL"] + [f"{quote_ident(k)} VARCHAR(128) NOT NULL" for k in keys]
    cols.append("distinct_value VARCHAR(256) NOT NULL")
    pk_cols = ", ".join([quote_ident(k) for k in keys] + ["event_day", "distinct_value"])
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(distinct_table(bundle.bundle_id, distinct.template_id))} (
  {",\n  ".join(cols)},
  PRIMARY KEY ({pk_cols})
);
""".strip()


def key_selects(bundle) -> list[str]:
    return [field for field in bundle.group_by_fields]


def key_not_null_predicates(bundle) -> list[str]:
    return [f"{field} IS NOT NULL" for field in bundle.group_by_fields]


def day_filter(group: str, day: date | None) -> str | None:
    if day is None:
        return None
    next_day = day + timedelta(days=1)
    if group == "A":
        start_ms = int(datetime.combine(day, datetime.min.time()).timestamp() * 1000)
        end_ms = int(datetime.combine(next_day, datetime.min.time()).timestamp() * 1000)
        return f"p.event_date >= {start_ms} AND p.event_date < {end_ms}"
    if group == "C":
        start_dt = f"{day.isoformat()} 00:00:00"
        end_dt = f"{next_day.isoformat()} 00:00:00"
        start_ms = int(datetime.combine(day, datetime.min.time()).timestamp() * 1000)
        end_ms = int(datetime.combine(next_day, datetime.min.time()).timestamp() * 1000)
        return (
            f"p.event_date >= {start_ms} AND p.event_date < {end_ms} "
            f"AND d.jms_timestamp >= '{start_dt}' AND d.jms_timestamp < '{end_dt}'"
        )
    return f"d.jms_timestamp >= '{day.isoformat()} 00:00:00' AND d.jms_timestamp < '{next_day.isoformat()} 00:00:00'"


def build_daily_insert_sql(group: str, bundle, day: date | None = None) -> str | None:
    rollups, _ = bundle_rollup_metrics(group, bundle)
    if not rollups:
        return None
    _, day_expr, from_sql = source_parts(group)
    keys = key_fields(bundle)
    select_parts = [f"{day_expr} AS event_day"] + [f"{field} AS {quote_ident(field.split('.', 1)[1])}" for field in bundle.group_by_fields]
    for metric in rollups:
        select_parts.append(metric.daily_expr if is_avg_helper(metric) else f"{metric.daily_expr} AS {quote_ident(metric.output_column)}")
        if metric.extra_predicate and not metric.output_column.startswith("present__"):
            select_parts.append(f"SUM(CASE WHEN {metric.extra_predicate} THEN 1 ELSE 0 END) AS {quote_ident(presence_column(metric.template_id))}")
    where_parts = key_not_null_predicates(bundle)
    if group in {"A", "C"}:
        where_parts.append("p.event_date IS NOT NULL")
    if group == "C":
        where_parts.append("d.jms_timestamp IS NOT NULL")
    elif group == "B":
        where_parts.append("d.jms_timestamp IS NOT NULL")
    df = day_filter(group, day)
    if df:
        where_parts.append(df)
    group_parts = ["event_day"] + key_selects(bundle)
    columns = ["event_day"] + keys
    for metric in rollups:
        if is_avg_helper(metric):
            # AVG helper emits two columns with fixed suffixes.
            columns.extend([metric.output_column + "__sum", metric.output_column + "__count"])
        else:
            columns.append(metric.output_column)
        if metric.extra_predicate and metric.output_column.startswith("present__"):
            where_parts.append(f"({metric.extra_predicate})")
        elif metric.extra_predicate:
            columns.append(presence_column(metric.template_id))
    return f"""
REPLACE INTO {quote_ident(daily_table(bundle.bundle_id))} ({", ".join(quote_ident(c) for c in columns)})
SELECT
  {",\n  ".join(select_parts)}
{from_sql}
WHERE {" AND ".join(where_parts)}
GROUP BY {", ".join(group_parts)};
""".strip()


def build_distinct_insert_sql(group: str, bundle, distinct: DistinctMetric, day: date | None = None) -> str:
    _, day_expr, from_sql = source_parts(group)
    keys = key_fields(bundle)
    select_parts = [f"{day_expr} AS event_day"] + [f"{field} AS {quote_ident(field.split('.', 1)[1])}" for field in bundle.group_by_fields]
    select_parts.append(f"CAST({distinct.distinct_expr} AS CHAR(256)) AS distinct_value")
    where_parts = key_not_null_predicates(bundle)
    where_parts.append(f"{distinct.distinct_expr} IS NOT NULL")
    if distinct.extra_predicate:
        where_parts.append(f"({distinct.extra_predicate})")
    if group in {"A", "C"}:
        where_parts.append("p.event_date IS NOT NULL")
    if group == "C":
        where_parts.append("d.jms_timestamp IS NOT NULL")
    elif group == "B":
        where_parts.append("d.jms_timestamp IS NOT NULL")
    df = day_filter(group, day)
    if df:
        where_parts.append(df)
    group_parts = ["event_day"] + key_selects(bundle) + ["distinct_value"]
    columns = ["event_day"] + keys + ["distinct_value"]
    return f"""
REPLACE INTO {quote_ident(distinct_table(bundle.bundle_id, distinct.template_id))} ({", ".join(quote_ident(c) for c in columns)})
SELECT
  {",\n  ".join(select_parts)}
{from_sql}
WHERE {" AND ".join(where_parts)}
GROUP BY {", ".join(group_parts)};
""".strip()


def render_runtime_query(group: str, bundle, reference_time: datetime) -> str:
    rollups, distincts = bundle_rollup_metrics(group, bundle)
    keys = key_fields(bundle)
    cutoff_days = bundle.window_days
    select_parts: list[str] = []
    for metric in rollups:
        select_parts.append(f"{metric.combine_expr} AS {quote_ident(metric.output_column)}")
        if metric.extra_predicate:
            select_parts.append(f"SUM({quote_ident(presence_column(metric.template_id))}) AS {quote_ident(presence_column(metric.template_id))}")
    key_predicates = [f"r.{quote_ident(k)} = %s" for k in keys]
    if rollups:
        base_query = f"""
SELECT
  {",\n  ".join(select_parts)}
FROM {quote_ident(daily_table(bundle.bundle_id))} r
WHERE {" AND ".join(key_predicates)}
  AND r.event_day >= DATE_SUB(DATE(%s), INTERVAL {cutoff_days} DAY)
GROUP BY {", ".join("r." + quote_ident(k) for k in keys)}
""".strip()
    else:
        base_query = "SELECT"
    if distincts:
        distinct_selects = []
        for distinct in distincts:
            predicates = [f"x.{quote_ident(k)} = %s" for k in keys]
            distinct_selects.append(
                f"(SELECT COUNT(DISTINCT x.distinct_value) FROM {quote_ident(distinct_table(bundle.bundle_id, distinct.template_id))} x "
                f"WHERE {' AND '.join(predicates)} AND x.event_day >= DATE_SUB(DATE(%s), INTERVAL {cutoff_days} DAY)) AS {quote_ident(distinct.output_column)}"
            )
        if rollups:
            # This render is illustrative; final query runner stitches params.
            base_query = base_query.replace("SELECT\n  ", "SELECT\n  " + ",\n  ".join(distinct_selects) + ",\n  ")
        else:
            base_query = "SELECT\n  " + ",\n  ".join(distinct_selects)
    return base_query + ";"


def prod180_rollup_table(group: str) -> str:
    return f"group_{group.lower()}_180d_daily_rollup"


def prod180_distinct_table(group: str) -> str:
    return f"group_{group.lower()}_180d_daily_distinct"


def prod180_hourly_rollup_table(group: str) -> str:
    return f"group_{group.lower()}_180d_hourly_rollup"


def prod180_hourly_distinct_table(group: str) -> str:
    return f"group_{group.lower()}_180d_hourly_distinct"


def prod180_use_hourly_boundary() -> bool:
    return os.getenv("INTUIT_PROD180_HOURLY_BOUNDARY", "0").lower() in {"1", "true", "yes", "on"}


def prod180_hourly_boundary_groups() -> set[str]:
    raw = os.getenv("INTUIT_PROD180_HOURLY_BOUNDARY_GROUPS", "C")
    groups = {part.strip().upper() for part in raw.split(",") if part.strip()}
    return groups or {"C"}


def prod180_distinct_uses_hourly(group: str, bundle, distincts: list[DistinctMetric]) -> bool:
    return (
        prod180_use_hourly_boundary()
        and group in prod180_hourly_boundary_groups()
        and bool(distincts)
        and not any(distinct.extra_predicate for distinct in distincts)
    )


def prod180_mixed_uses_hourly(group: str, bundle, rollups: list[RollupMetric], distincts: list[DistinctMetric]) -> bool:
    return (
        prod180_use_hourly_boundary()
        and group in prod180_hourly_boundary_groups()
        and group == "C"
        and bool(rollups)
        and bool(distincts)
        and not any(metric.extra_predicate for metric in rollups)
        and not any(distinct.extra_predicate for distinct in distincts)
    )


def prod180_group_c_mixed_uses_hourly(group: str, bundle, rollups: list[RollupMetric], distincts: list[DistinctMetric]) -> bool:
    return prod180_mixed_uses_hourly(group, bundle, rollups, distincts)


def prod180_group_c_distinct_uses_hourly(group: str, bundle, distincts: list[DistinctMetric]) -> bool:
    return prod180_distinct_uses_hourly(group, bundle, distincts)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def prod180_key_predicates(alias: str, keys: list[str]) -> list[str]:
    predicates = [f"{alias}.key1 = %s"]
    if len(keys) > 1:
        predicates.append(f"{alias}.key2 = %s")
    else:
        predicates.append(f"{alias}.key2 = ''")
    return predicates


def prod180_day_predicate(group: str, alias: str) -> str:
    if group == "C":
        return (
            f"{alias}.p_event_day >= DATE_SUB(DATE(%s), INTERVAL 180 DAY) "
            f"AND {alias}.d_event_day >= DATE_SUB(DATE(%s), INTERVAL 180 DAY)"
        )
    return f"{alias}.event_day >= DATE_SUB(DATE(%s), INTERVAL 180 DAY)"


def prod180_cutoff_parts(reference_time: datetime) -> dict[str, Any]:
    cutoff = reference_time - timedelta(days=180)
    cutoff_day = cutoff.date()
    next_day = datetime.combine(cutoff_day + timedelta(days=1), datetime.min.time())
    next_hour = cutoff.replace(minute=0, second=0, microsecond=0)
    if next_hour < cutoff:
        next_hour += timedelta(hours=1)
    return {
        "cutoff": cutoff,
        "cutoff_ts": cutoff.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "cutoff_day": cutoff_day.isoformat(),
        "next_hour": next_hour,
        "next_hour_ts": next_hour.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "next_hour_ms": int(next_hour.timestamp() * 1000),
        "next_day_ts": next_day.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "cutoff_ms": int(cutoff.timestamp() * 1000),
        "next_day_ms": int(next_day.timestamp() * 1000),
    }


def prod180_full_day_predicate(group: str, alias: str, cutoff_day: str) -> str:
    if group == "C":
        return f"{alias}.p_event_day > '{cutoff_day}' AND {alias}.d_event_day > '{cutoff_day}'"
    return f"{alias}.event_day > '{cutoff_day}'"


def raw_key_predicates(bundle) -> list[str]:
    return [f"{field} = %s" for field in bundle.group_by_fields]


def raw_window_predicate(group: str, parts: dict[str, Any]) -> str:
    if group == "A":
        return f"p.event_date >= {parts['cutoff_ms']} AND p.event_date < {parts['next_day_ms']}"
    if group == "B":
        return f"d.jms_timestamp >= '{parts['cutoff_ts']}' AND d.jms_timestamp < '{parts['next_day_ts']}'"
    return (
        f"p.event_date >= {parts['cutoff_ms']} "
        f"AND d.jms_timestamp >= '{parts['cutoff_ts']}' "
        f"AND (p.event_date < {parts['next_day_ms']} OR d.jms_timestamp < '{parts['next_day_ts']}')"
    )


def raw_tail_window_predicate(group: str, parts: dict[str, Any]) -> str:
    if group == "A":
        return f"p.event_date >= {parts['cutoff_ms']} AND p.event_date < {parts['next_hour_ms']}"
    if group == "B":
        return f"d.jms_timestamp >= '{parts['cutoff_ts']}' AND d.jms_timestamp < '{parts['next_hour_ts']}'"
    return (
        f"p.event_date >= {parts['cutoff_ms']} "
        f"AND d.jms_timestamp >= '{parts['cutoff_ts']}' "
        f"AND (p.event_date < {parts['next_hour_ms']} OR d.jms_timestamp < '{parts['next_hour_ts']}')"
    )


def prod180_hour_predicate(group: str, alias: str, parts: dict[str, Any]) -> str:
    if group == "C":
        return (
            f"{alias}.p_event_hour >= '{parts['next_hour_ts']}' "
            f"AND {alias}.d_event_hour >= '{parts['next_hour_ts']}' "
            f"AND ({alias}.p_event_hour < '{parts['next_day_ts']}' "
            f"OR {alias}.d_event_hour < '{parts['next_day_ts']}')"
        )
    return (
        f"{alias}.event_hour >= '{parts['next_hour_ts']}' "
        f"AND {alias}.event_hour < '{parts['next_day_ts']}'"
    )


def raw_group_c_hourly_tail_cte(raw_column_sql: str, from_sql: str, bundle, parts: dict[str, Any]) -> str:
    base_predicates = raw_key_predicates(bundle) + raw_not_null_predicates("C")
    d_tail = (
        base_predicates
        + [
            f"p.event_date >= {parts['cutoff_ms']}",
            f"d.jms_timestamp >= '{parts['cutoff_ts']}'",
            f"d.jms_timestamp < '{parts['next_hour_ts']}'",
        ]
    )
    p_tail = (
        base_predicates
        + [
            f"p.event_date >= {parts['cutoff_ms']}",
            f"p.event_date < {parts['next_hour_ms']}",
            f"d.jms_timestamp >= '{parts['next_hour_ts']}'",
        ]
    )
    return f"""raw_boundary AS (
  SELECT
    {raw_column_sql}
  {from_sql}
  WHERE {" AND ".join(d_tail)}
  UNION ALL
  SELECT
    {raw_column_sql}
  {from_sql}
  WHERE {" AND ".join(p_tail)}
)"""


def raw_hourly_tail_cte(group: str, raw_column_sql: str, from_sql: str, bundle, parts: dict[str, Any]) -> str:
    if group == "C":
        return raw_group_c_hourly_tail_cte(raw_column_sql, from_sql, bundle, parts)
    raw_where_parts = raw_key_predicates(bundle) + raw_not_null_predicates(group) + [raw_tail_window_predicate(group, parts)]
    return f"""raw_boundary AS (
  SELECT
    {raw_column_sql}
  {from_sql}
  WHERE {" AND ".join(raw_where_parts)}
)"""


def raw_not_null_predicates(group: str) -> list[str]:
    predicates: list[str] = []
    if group in {"A", "C"}:
        predicates.append("p.event_date IS NOT NULL")
    if group in {"B", "C"}:
        predicates.append("d.jms_timestamp IS NOT NULL")
    return predicates


def render_prod180_distinct_only_query(group: str, bundle, distincts: list[DistinctMetric], cutoff_parts: dict[str, Any]) -> str:
    """Render distinct-only 180d runtime query with one raw-boundary scan.

    The scalar-subquery form scans the raw cutoff-day boundary once per distinct
    metric. For distinct-only bundles we can materialize that raw boundary once
    and unpivot it into (template_id, distinct_value), while still de-duping
    exactly across helper-table full days and raw boundary rows.
    """
    if any(distinct.extra_predicate for distinct in distincts):
        raise ValueError("filtered distinct metrics need the scalar presence-count path")

    keys = key_fields(bundle)
    key_predicates = prod180_key_predicates("x", keys)
    hourly_key_predicates = prod180_key_predicates("hx", keys)
    _, _, from_sql = source_parts(group)
    use_hourly = prod180_distinct_uses_hourly(group, bundle, distincts)
    raw_where_parts = (
        raw_key_predicates(bundle)
        + raw_not_null_predicates(group)
        + [raw_tail_window_predicate(group, cutoff_parts) if use_hourly else raw_window_predicate(group, cutoff_parts)]
    )
    raw_columns: list[str] = []
    raw_unions: list[str] = []
    final_selects: list[str] = []
    template_ids = ", ".join(sql_literal(distinct.template_id) for distinct in distincts)
    for index, distinct in enumerate(distincts):
        raw_col = f"raw_distinct_{index}"
        raw_columns.append(f"{distinct.distinct_expr} AS {quote_ident(raw_col)}")
        raw_unions.append(
            f"SELECT {sql_literal(distinct.template_id)} AS template_id, "
            f"CAST({quote_ident(raw_col)} AS CHAR(256)) AS distinct_value "
            f"FROM raw_boundary WHERE {quote_ident(raw_col)} IS NOT NULL"
        )
        final_selects.append(
            f"COUNT(DISTINCT CASE WHEN template_id = {sql_literal(distinct.template_id)} "
            f"THEN distinct_value END) AS {quote_ident(distinct.output_column)}"
        )

    raw_column_sql = ",\n    ".join(raw_columns)
    if use_hourly:
        raw_boundary_cte = raw_hourly_tail_cte(group, raw_column_sql, from_sql, bundle, cutoff_parts)
    else:
        raw_boundary_cte = f"""raw_boundary AS (
  SELECT
    {raw_column_sql}
  {from_sql}
  WHERE {" AND ".join(raw_where_parts)}
)"""
    hourly_distinct_union = ""
    if use_hourly:
        hourly_distinct_union = f"""
  UNION ALL
  SELECT hx.template_id, hx.distinct_value
  FROM {quote_ident(prod180_hourly_distinct_table(group))} hx
  WHERE hx.bundle_id = {sql_literal(bundle.bundle_id)}
    AND hx.template_id IN ({template_ids})
    AND {" AND ".join(hourly_key_predicates)}
    AND {prod180_hour_predicate(group, "hx", cutoff_parts)}"""
    raw_union_sql = "\n  UNION ALL\n  ".join(raw_unions)
    final_select_sql = ",\n  ".join(final_selects)
    return f"""
WITH {raw_boundary_cte}, distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM {quote_ident(prod180_distinct_table(group))} x
  WHERE x.bundle_id = {sql_literal(bundle.bundle_id)}
    AND x.template_id IN ({template_ids})
    AND {" AND ".join(key_predicates)}
    AND {prod180_full_day_predicate(group, "x", cutoff_parts["cutoff_day"])}
  {hourly_distinct_union}
  UNION ALL
  {raw_union_sql}
)
SELECT
  {final_select_sql}
	FROM distinct_values
	""".strip()


def render_prod180_distinct_filtered_query(group: str, bundle, distincts: list[DistinctMetric], cutoff_parts: dict[str, Any]) -> str | None:
    """Render distinct-only 180d query with shared unfiltered helper scan.

    Some Group B bundles mix one filtered distinct/presence metric with several
    unfiltered distinct metrics. The scalar fallback scans the large helper table
    once per unfiltered metric. We can scan all unfiltered template_ids together
    and keep the filtered metric on the scalar path where its extra predicate and
    presence count are easier to preserve exactly.
    """
    filtered = [distinct for distinct in distincts if distinct.extra_predicate]
    unfiltered = [distinct for distinct in distincts if not distinct.extra_predicate]
    if not filtered or not unfiltered:
        return None

    keys = key_fields(bundle)
    key_predicates = prod180_key_predicates("x", keys)
    hourly_key_predicates = prod180_key_predicates("hx", keys)
    _, _, from_sql = source_parts(group)
    use_hourly = prod180_use_hourly_boundary() and group in prod180_hourly_boundary_groups()
    raw_where_parts = (
        raw_key_predicates(bundle)
        + raw_not_null_predicates(group)
        + [raw_tail_window_predicate(group, cutoff_parts) if use_hourly else raw_window_predicate(group, cutoff_parts)]
    )

    raw_columns: list[str] = []
    raw_unions: list[str] = []
    unfiltered_selects: list[str] = []
    template_ids = ", ".join(sql_literal(distinct.template_id) for distinct in unfiltered)
    for index, distinct in enumerate(unfiltered):
        raw_col = f"raw_distinct_{index}"
        raw_columns.append(f"{distinct.distinct_expr} AS {quote_ident(raw_col)}")
        raw_unions.append(
            f"SELECT {sql_literal(distinct.template_id)} AS template_id, "
            f"CAST({quote_ident(raw_col)} AS CHAR(256)) AS distinct_value "
            f"FROM raw_boundary WHERE {quote_ident(raw_col)} IS NOT NULL"
        )
        unfiltered_selects.append(
            f"COUNT(DISTINCT CASE WHEN template_id = {sql_literal(distinct.template_id)} "
            f"THEN distinct_value END) AS {quote_ident(distinct.output_column)}"
        )

    raw_boundary_column_sql = ",\n    ".join(raw_columns)
    if use_hourly:
        raw_boundary_cte = raw_hourly_tail_cte(group, raw_boundary_column_sql, from_sql, bundle, cutoff_parts)
    else:
        raw_boundary_cte = f"""raw_boundary AS (
  SELECT
    {raw_boundary_column_sql}
  {from_sql}
  WHERE {" AND ".join(raw_where_parts)}
)"""
    hourly_distinct_union = ""
    if use_hourly:
        hourly_distinct_union = f"""
  UNION ALL
  SELECT hx.template_id, hx.distinct_value
  FROM {quote_ident(prod180_hourly_distinct_table(group))} hx
  WHERE hx.bundle_id = {sql_literal(bundle.bundle_id)}
    AND hx.template_id IN ({template_ids})
    AND {" AND ".join(hourly_key_predicates)}
    AND {prod180_hour_predicate(group, "hx", cutoff_parts)}"""

    ctes: list[str] = [
        raw_boundary_cte,
        f"""distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM {quote_ident(prod180_distinct_table(group))} x
  WHERE x.bundle_id = {sql_literal(bundle.bundle_id)}
    AND x.template_id IN ({template_ids})
    AND {" AND ".join(key_predicates)}
    AND {prod180_full_day_predicate(group, "x", cutoff_parts["cutoff_day"])}
  {hourly_distinct_union}
  UNION ALL
  {"\n  UNION ALL\n  ".join(raw_unions)}
)""",
        f"""unfiltered_counts AS (
  SELECT
    {",\n    ".join(unfiltered_selects)}
  FROM distinct_values
)""",
    ]

    filtered_names: dict[str, str] = {}
    for index, distinct in enumerate(filtered):
        cte_name = f"filtered_{index}"
        filtered_names[distinct.template_id] = cte_name
        predicates = prod180_key_predicates("x", keys)
        hourly_predicates = prod180_key_predicates("hx", keys)
        raw_distinct_where_parts = (
            raw_key_predicates(bundle)
            + [f"{distinct.distinct_expr} IS NOT NULL"]
            + raw_not_null_predicates(group)
            + [raw_tail_window_predicate(group, cutoff_parts) if use_hourly else raw_window_predicate(group, cutoff_parts)]
        )
        if distinct.extra_predicate:
            raw_distinct_where_parts.append(f"({distinct.extra_predicate})")

        presence_predicates = prod180_key_predicates("x", keys)
        hourly_presence_predicates = prod180_key_predicates("h", keys)
        presence_col = presence_column(distinct.template_id)
        raw_presence_where_parts = (
            raw_key_predicates(bundle)
            + raw_not_null_predicates(group)
            + [raw_tail_window_predicate(group, cutoff_parts) if use_hourly else raw_window_predicate(group, cutoff_parts)]
        )
        if distinct.extra_predicate:
            raw_presence_where_parts.append(f"({distinct.extra_predicate})")

        filtered_hourly_distinct_union = ""
        filtered_hourly_presence_union = ""
        if use_hourly:
            filtered_hourly_distinct_union = f"""
      UNION ALL
      SELECT hx.distinct_value
      FROM {quote_ident(prod180_hourly_distinct_table(group))} hx
      WHERE hx.bundle_id = {sql_literal(bundle.bundle_id)}
        AND hx.template_id = {sql_literal(distinct.template_id)}
        AND {" AND ".join(hourly_predicates)}
        AND {prod180_hour_predicate(group, "hx", cutoff_parts)}"""
            filtered_hourly_presence_union = f"""
      UNION ALL
      SELECT h.{quote_ident(presence_col)} AS presence_count
      FROM {quote_ident(prod180_hourly_rollup_table(group))} h
      WHERE h.bundle_id = {sql_literal(bundle.bundle_id)}
        AND {" AND ".join(hourly_presence_predicates)}
        AND {prod180_hour_predicate(group, "h", cutoff_parts)}"""

        ctes.append(
            f"""{cte_name} AS (
  SELECT
    (SELECT COUNT(DISTINCT u.distinct_value) FROM (
      SELECT x.distinct_value
      FROM {quote_ident(prod180_distinct_table(group))} x
      WHERE x.bundle_id = {sql_literal(bundle.bundle_id)}
        AND x.template_id = {sql_literal(distinct.template_id)}
        AND {" AND ".join(predicates)}
        AND {prod180_full_day_predicate(group, "x", cutoff_parts["cutoff_day"])}
      {filtered_hourly_distinct_union}
      UNION ALL
      SELECT CAST({distinct.distinct_expr} AS CHAR(256)) AS distinct_value
      {from_sql}
      WHERE {" AND ".join(raw_distinct_where_parts)}
    ) u) AS {quote_ident(distinct.output_column)},
    COALESCE((SELECT SUM(u.presence_count) FROM (
      SELECT x.{quote_ident(presence_col)} AS presence_count
      FROM {quote_ident(prod180_rollup_table(group))} x
      WHERE x.bundle_id = {sql_literal(bundle.bundle_id)}
        AND {" AND ".join(presence_predicates)}
        AND {prod180_full_day_predicate(group, "x", cutoff_parts["cutoff_day"])}
      {filtered_hourly_presence_union}
      UNION ALL
      SELECT COUNT(*) AS presence_count
      {from_sql}
      WHERE {" AND ".join(raw_presence_where_parts)}
    ) u), 0) AS {quote_ident(presence_col)}
)"""
        )

    final_selects: list[str] = []
    for distinct in distincts:
        if distinct.extra_predicate:
            cte_name = filtered_names[distinct.template_id]
            final_selects.append(f"{cte_name}.{quote_ident(distinct.output_column)}")
            final_selects.append(f"{cte_name}.{quote_ident(presence_column(distinct.template_id))}")
        else:
            final_selects.append(f"unfiltered_counts.{quote_ident(distinct.output_column)}")

    cross_joins = "\n".join(f"CROSS JOIN {name}" for name in filtered_names.values())
    return f"""
WITH {",\n".join(ctes)}
SELECT
  {",\n  ".join(final_selects)}
FROM unfiltered_counts
{cross_joins}
""".strip()


def raw_rollup_expr_from_metric(metric: RollupMetric) -> str | None:
    expr = metric.daily_expr.strip()
    if metric.extra_predicate:
        return None
    if expr == "COUNT(*)":
        return f"COUNT(*) AS {quote_ident(metric.output_column)}"
    if expr == "SUM(p.amount)":
        return f"SUM(raw_p_amount) AS {quote_ident(metric.output_column)}"
    if expr == "MIN(p.amount)":
        return f"MIN(raw_p_amount) AS {quote_ident(metric.output_column)}"
    if expr == "MAX(p.amount)":
        return f"MAX(raw_p_amount) AS {quote_ident(metric.output_column)}"
    return None


def render_prod180_mixed_unfiltered_query(
    group: str,
    bundle,
    rollups: list[RollupMetric],
    distincts: list[DistinctMetric],
    cutoff_parts: dict[str, Any],
) -> str | None:
    """Render 180d rollup+distinct query with one raw-boundary scan.

    The scalar-subquery fallback scans the raw cutoff-day boundary once per
    distinct metric. For unfiltered mixed Group C bundles, a materialized
    raw_boundary CTE lets rollups and all distinct metrics share that scan.
    """
    if group != "C" or any(metric.extra_predicate for metric in rollups) or any(distinct.extra_predicate for distinct in distincts):
        return None

    raw_rollup_selects = [raw_rollup_expr_from_metric(metric) for metric in rollups]
    if any(item is None for item in raw_rollup_selects):
        return None

    keys = key_fields(bundle)
    _, _, from_sql = source_parts(group)
    use_hourly = prod180_mixed_uses_hourly(group, bundle, rollups, distincts)
    raw_where_parts = (
        raw_key_predicates(bundle)
        + raw_not_null_predicates(group)
        + [raw_tail_window_predicate(group, cutoff_parts) if use_hourly else raw_window_predicate(group, cutoff_parts)]
    )
    raw_boundary_columns = ["p.amount AS raw_p_amount"]
    raw_unions: list[str] = []
    distinct_final_selects: list[str] = []
    for index, distinct in enumerate(distincts):
        raw_col = f"raw_distinct_{index}"
        raw_boundary_columns.append(f"{distinct.distinct_expr} AS {quote_ident(raw_col)}")
        raw_unions.append(
            f"SELECT {sql_literal(distinct.template_id)} AS template_id, "
            f"CAST({quote_ident(raw_col)} AS CHAR(256)) AS distinct_value "
            f"FROM raw_boundary WHERE {quote_ident(raw_col)} IS NOT NULL"
        )
        distinct_final_selects.append(
            f"COUNT(DISTINCT CASE WHEN template_id = {sql_literal(distinct.template_id)} "
            f"THEN distinct_value END) AS {quote_ident(distinct.output_column)}"
        )

    helper_rollup_selects: list[str] = []
    rollup_final_selects: list[str] = []
    rollup_output_columns: list[str] = []
    for metric in rollups:
        if is_avg_helper(metric):
            helper_rollup_selects.append(quote_ident(metric.output_column + "__sum"))
            helper_rollup_selects.append(quote_ident(metric.output_column + "__count"))
        else:
            helper_rollup_selects.append(quote_ident(metric.output_column))
        rollup_final_selects.append(f"{metric.combine_expr} AS {quote_ident(metric.output_column)}")
        rollup_output_columns.append(metric.output_column)

    template_ids = ", ".join(sql_literal(distinct.template_id) for distinct in distincts)
    key_predicates = prod180_key_predicates("x", keys)
    rollup_key_predicates = prod180_key_predicates("r", keys)
    hourly_key_predicates = prod180_key_predicates("h", keys)
    hourly_distinct_key_predicates = prod180_key_predicates("hx", keys)
    final_selects = [f"rollup_final.{quote_ident(column)}" for column in rollup_output_columns] + [
        f"distinct_counts.{quote_ident(distinct.output_column)}" for distinct in distincts
    ]
    raw_boundary_column_sql = ",\n    ".join(raw_boundary_columns)
    if use_hourly:
        raw_boundary_cte = raw_hourly_tail_cte(group, raw_boundary_column_sql, from_sql, bundle, cutoff_parts)
    else:
        raw_boundary_cte = f"""raw_boundary AS (
  SELECT
    {raw_boundary_column_sql}
  {from_sql}
  WHERE {" AND ".join(raw_where_parts)}
)"""
    hourly_rollup_union = ""
    hourly_distinct_union = ""
    if use_hourly:
        hourly_rollup_union = f"""
  UNION ALL
  SELECT {", ".join(helper_rollup_selects)}
  FROM {quote_ident(prod180_hourly_rollup_table(group))} h
  WHERE h.bundle_id = {sql_literal(bundle.bundle_id)}
    AND {" AND ".join(hourly_key_predicates)}
    AND {prod180_hour_predicate(group, "h", cutoff_parts)}"""
        hourly_distinct_union = f"""
  UNION ALL
  SELECT hx.template_id, hx.distinct_value
  FROM {quote_ident(prod180_hourly_distinct_table(group))} hx
  WHERE hx.bundle_id = {sql_literal(bundle.bundle_id)}
    AND hx.template_id IN ({template_ids})
    AND {" AND ".join(hourly_distinct_key_predicates)}
    AND {prod180_hour_predicate(group, "hx", cutoff_parts)}"""

    return f"""
WITH {raw_boundary_cte}, rollup_rows AS (
  SELECT {", ".join(helper_rollup_selects)}
  FROM {quote_ident(prod180_rollup_table(group))} r
  WHERE r.bundle_id = {sql_literal(bundle.bundle_id)}
    AND {" AND ".join(rollup_key_predicates)}
    AND {prod180_full_day_predicate(group, "r", cutoff_parts["cutoff_day"])}
  {hourly_rollup_union}
  UNION ALL
  SELECT {", ".join(item for item in raw_rollup_selects if item)}
  FROM raw_boundary
  HAVING COUNT(*) > 0
), rollup_final AS (
  SELECT
    {",\n    ".join(rollup_final_selects)}
  FROM rollup_rows
), distinct_values AS (
  SELECT x.template_id, x.distinct_value
  FROM {quote_ident(prod180_distinct_table(group))} x
  WHERE x.bundle_id = {sql_literal(bundle.bundle_id)}
    AND x.template_id IN ({template_ids})
    AND {" AND ".join(key_predicates)}
    AND {prod180_full_day_predicate(group, "x", cutoff_parts["cutoff_day"])}
  {hourly_distinct_union}
  UNION ALL
  {"\n  UNION ALL\n  ".join(raw_unions)}
), distinct_counts AS (
  SELECT
    {",\n    ".join(distinct_final_selects)}
  FROM distinct_values
)
SELECT
  {",\n  ".join(final_selects)}
FROM rollup_final
CROSS JOIN distinct_counts
""".strip()


def render_prod180_runtime_query(group: str, bundle, reference_time: datetime) -> str:
    """Render a production-style consolidated 180d pre-agg query.

    This is exact for timestamp-level 180d windows. Daily rollups cover only
    full days after the cutoff day; rows on the cutoff day are read from the
    raw tables and UNIONed back in so we do not accidentally include rows that
    occurred before the precise cutoff timestamp.
    """
    if bundle.window_days != 180:
        raise ValueError(f"prod180 pre-agg only supports 180d bundles, got {bundle.bundle_id}")

    rollups, distincts = bundle_rollup_metrics(group, bundle)
    keys = key_fields(bundle)
    cutoff_parts = prod180_cutoff_parts(reference_time)
    if distincts and not rollups and not any(distinct.extra_predicate for distinct in distincts):
        return render_prod180_distinct_only_query(group, bundle, distincts, cutoff_parts) + ";"
    if distincts and not rollups:
        cte_sql = render_prod180_distinct_filtered_query(group, bundle, distincts, cutoff_parts)
        if cte_sql:
            return cte_sql + ";"
    if distincts and rollups and not any(distinct.extra_predicate for distinct in distincts):
        cte_sql = render_prod180_mixed_unfiltered_query(group, bundle, rollups, distincts, cutoff_parts)
        if cte_sql:
            return cte_sql + ";"

    rollup_select_parts: list[str] = []
    for metric in rollups:
        rollup_select_parts.append(f"{metric.combine_expr} AS {quote_ident(metric.output_column)}")
        if metric.extra_predicate:
            rollup_select_parts.append(
                f"SUM({quote_ident(presence_column(metric.template_id))}) AS {quote_ident(presence_column(metric.template_id))}"
            )

    distinct_selects: list[str] = []
    for distinct in distincts:
        predicates = prod180_key_predicates("x", keys)
        _, _, from_sql = source_parts(group)
        raw_where_parts = (
            raw_key_predicates(bundle)
            + [f"{distinct.distinct_expr} IS NOT NULL"]
            + raw_not_null_predicates(group)
            + [raw_window_predicate(group, cutoff_parts)]
        )
        if distinct.extra_predicate:
            raw_where_parts.append(f"({distinct.extra_predicate})")
        distinct_selects.append(
            f"(SELECT COUNT(DISTINCT u.distinct_value) FROM ("
            f"SELECT x.distinct_value FROM {quote_ident(prod180_distinct_table(group))} x "
            f"WHERE x.bundle_id = {sql_literal(bundle.bundle_id)} "
            f"AND x.template_id = {sql_literal(distinct.template_id)} "
            f"AND {' AND '.join(predicates)} "
            f"AND {prod180_full_day_predicate(group, 'x', cutoff_parts['cutoff_day'])} "
            f"UNION ALL "
            f"SELECT CAST({distinct.distinct_expr} AS CHAR(256)) AS distinct_value "
            f"{from_sql} "
            f"WHERE {' AND '.join(raw_where_parts)}) u) AS {quote_ident(distinct.output_column)}"
        )
        if distinct.extra_predicate:
            presence_predicates = prod180_key_predicates("x", keys)
            presence_col = presence_column(distinct.template_id)
            raw_presence_where_parts = (
                raw_key_predicates(bundle)
                + raw_not_null_predicates(group)
                + [raw_window_predicate(group, cutoff_parts)]
                + [f"({distinct.extra_predicate})"]
            )
            distinct_selects.append(
                f"COALESCE((SELECT SUM(u.presence_count) FROM ("
                f"SELECT x.{quote_ident(presence_col)} AS presence_count "
                f"FROM {quote_ident(prod180_rollup_table(group))} x "
                f"WHERE x.bundle_id = {sql_literal(bundle.bundle_id)} "
                f"AND {' AND '.join(presence_predicates)} "
                f"AND {prod180_full_day_predicate(group, 'x', cutoff_parts['cutoff_day'])} "
                f"UNION ALL "
                f"SELECT COUNT(*) AS presence_count "
                f"{from_sql} "
                f"WHERE {' AND '.join(raw_presence_where_parts)}) u), 0) AS {quote_ident(presence_col)}"
            )

    if rollups:
        key_predicates = prod180_key_predicates("r", keys)
        _, _, from_sql = source_parts(group)
        raw_where_parts = (
            raw_key_predicates(bundle)
            + raw_not_null_predicates(group)
            + [raw_window_predicate(group, cutoff_parts)]
        )
        helper_select_parts: list[str] = []
        raw_select_parts: list[str] = []
        for metric in rollups:
            if is_avg_helper(metric):
                helper_select_parts.append(quote_ident(metric.output_column + "__sum"))
                helper_select_parts.append(quote_ident(metric.output_column + "__count"))
                raw_select_parts.append(metric.daily_expr)
            else:
                helper_select_parts.append(quote_ident(metric.output_column))
                raw_select_parts.append(f"{metric.daily_expr} AS {quote_ident(metric.output_column)}")
            if metric.extra_predicate and not metric.output_column.startswith("present__"):
                helper_select_parts.append(quote_ident(presence_column(metric.template_id)))
                raw_select_parts.append(
                    f"SUM(CASE WHEN {metric.extra_predicate} THEN 1 ELSE 0 END) AS {quote_ident(presence_column(metric.template_id))}"
                )
        raw_group_sql = ", ".join(bundle.group_by_fields)
        select_parts = rollup_select_parts + distinct_selects
        base_query = f"""
SELECT
  {",\n  ".join(select_parts)}
FROM (
  SELECT {", ".join(helper_select_parts)}
  FROM {quote_ident(prod180_rollup_table(group))} r
  WHERE r.bundle_id = {sql_literal(bundle.bundle_id)}
    AND {" AND ".join(key_predicates)}
    AND {prod180_full_day_predicate(group, "r", cutoff_parts["cutoff_day"])}
  UNION ALL
  SELECT {", ".join(raw_select_parts)}
  {from_sql}
  WHERE {" AND ".join(raw_where_parts)}
  GROUP BY {raw_group_sql}
) r
""".strip()
    else:
        if not distinct_selects:
            raise ValueError(f"No prod180 metrics found for {bundle.bundle_id}")
        base_query = "SELECT\n  " + ",\n  ".join(distinct_selects)
    return base_query + ";"


def execute_sql(conn, sql: str, dry_run: bool) -> None:
    print(sql)
    print()
    if dry_run:
        return
    started = time.perf_counter()
    max_retries = int(os.environ.get("PREAGG_SQL_RETRIES", "4"))
    retry_codes = {1105, 8027, 9002}
    for attempt in range(max_retries + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            break
        except Exception as exc:
            code = getattr(exc, "args", [None])[0] if getattr(exc, "args", None) else None
            if code not in retry_codes or attempt >= max_retries:
                raise
            sleep_s = min(120.0, 10.0 * (2 ** attempt)) + random.random() * 5.0
            print(
                f"-- retryable preagg SQL error {code}; retry "
                f"{attempt + 1}/{max_retries} after {sleep_s:.1f}s"
            )
            time.sleep(sleep_s)
    print(f"-- done in {(time.perf_counter() - started):.1f}s\n")


def configure_build_session(conn) -> None:
    """Give offline rollup builds enough memory for large daily distinct groups."""
    mem_quota = os.environ.get("PREAGG_TIDB_MEM_QUOTA_QUERY", str(16 * 1024 * 1024 * 1024))
    with conn.cursor() as cur:
        cur.execute(f"SET SESSION tidb_mem_quota_query = {int(mem_quota)}")
        try:
            cur.execute("SET SESSION tidb_enable_tmp_storage_on_oom = ON")
        except Exception as exc:
            # TiDB Cloud Premium exposes this as GLOBAL-only in some builds.
            # The pre-agg build can proceed with the memory quota setting alone.
            print(f"-- skipping session tmp-storage setting: {exc}")


def selected_bundle_items(bundle_ids: list[str]) -> list[tuple[str, Any]]:
    catalog = all_bundles()
    items = []
    for bundle_id in bundle_ids:
        if bundle_id not in catalog:
            raise SystemExit(f"Unknown bundle_id: {bundle_id}")
        items.append(catalog[bundle_id])
    return items


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["create", "build", "render"])
    ap.add_argument("--bundle", action="append", dest="bundles", help="Bundle id. Repeatable. Defaults to first-cut candidate set.")
    ap.add_argument("--day", help="Build only one day, YYYY-MM-DD. Recommended for large rollups.")
    ap.add_argument("--execute", action="store_true", help="Actually execute SQL. Without this, prints SQL only.")
    args = ap.parse_args()

    bundle_ids = args.bundles or DEFAULT_BUNDLES
    items = selected_bundle_items(bundle_ids)
    build_day = date.fromisoformat(args.day) if args.day else None
    conn = None
    if args.execute:
        import pymysql

        from lib.db_config import get_db_config

        conn = pymysql.connect(**get_db_config(save_msg="preagg rollup builder"))
        conn.autocommit(True)
        configure_build_session(conn)

    try:
        for group, bundle in items:
            rollups, distincts = bundle_rollup_metrics(group, bundle)
            print(f"-- {bundle.bundle_id} group={group} window={bundle.window_days} rollups={len(rollups)} distincts={len(distincts)}")
            if args.action == "create":
                daily_sql = create_daily_table_sql(group, bundle)
                if daily_sql:
                    execute_sql(conn, daily_sql, dry_run=not args.execute)
                for distinct in distincts:
                    execute_sql(conn, create_distinct_table_sql(bundle, distinct), dry_run=not args.execute)
            elif args.action == "build":
                daily_sql = build_daily_insert_sql(group, bundle, day=build_day)
                if daily_sql:
                    execute_sql(conn, daily_sql, dry_run=not args.execute)
                for distinct in distincts:
                    execute_sql(conn, build_distinct_insert_sql(group, bundle, distinct, day=build_day), dry_run=not args.execute)
            else:
                print(render_runtime_query(group, bundle, datetime.now()))
                print()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
