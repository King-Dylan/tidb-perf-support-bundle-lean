#!/usr/bin/env python3
"""Build and query a targeted exact feature-serving table.

The existing prod180 helper tables reduce some 180-day work, but hot-key
distinct bundles can still scan hundreds of thousands or millions of helper
rows at event time. This module adds a final serving layer: precompute the
bundle output columns for selected key/as-of combinations and make the scoring
path a point lookup. The original narrow key/value table is still supported for
flexible inspection; the wide layout is the runtime path for high-QPS tests.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from demo import (
    GROUP_C_DEVICE_FIRST_JOIN_BUNDLES,
    GROUP_C_INNER_JOIN_BUNDLES,
    build_group_a_metric_expr,
    build_group_b_metric_expr,
    build_group_c_metric_expr,
    build_presence_expr,
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
    metric_column,
    presence_column,
)
from optimized_config import EXACT_SERVING_BUNDLES, PROD180_PREAGG_BUNDLES
from preagg_rollups import (
    bundle_rollup_metrics,
    key_fields,
    prod180_group_c_distinct_uses_hourly,
    prod180_group_c_mixed_uses_hourly,
    prod180_hourly_boundary_groups,
    prod180_use_hourly_boundary,
    quote_ident,
    render_prod180_runtime_query,
)


ROOT = Path(__file__).resolve().parent
SERVING_TABLE = os.getenv("INTUIT_EXACT_SERVING_TABLE", "risk_feature_serving")
WIDE_SERVING_TABLE = os.getenv("INTUIT_EXACT_SERVING_WIDE_TABLE", "risk_feature_serving_wide")
ARRAY_SERVING_TABLE = os.getenv("INTUIT_EXACT_SERVING_ARRAY_TABLE", "risk_feature_serving_array")
SERVING_LAYOUT = os.getenv("INTUIT_SERVING_LAYOUT", "kv").strip().lower()
MAX_METRIC_SLOTS = int(os.getenv("INTUIT_EXACT_SERVING_WIDE_SLOTS", "512"))
VALID_AS_OF_GRAINS = {"day", "timestamp"}
VALID_SERVING_LAYOUTS = {"kv", "wide"}


@dataclass(frozen=True)
class ServingKey:
    bundle_id: str
    group: str
    as_of_grain: str
    as_of_key: str
    reference_time: datetime
    key1: str
    key2: str
    bindings: dict[str, Any]


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


def metric_names_for_bundle(bundle) -> list[str]:
    names: list[str] = []
    for tmpl in bundle.templates:
        names.append(metric_column(tmpl.template_id))
        if tmpl.extra_predicate:
            names.append(presence_column(tmpl.template_id))
    return names


def validate_as_of_grain(as_of_grain: str) -> str:
    if as_of_grain not in VALID_AS_OF_GRAINS:
        raise ValueError(f"as_of_grain must be one of {sorted(VALID_AS_OF_GRAINS)}, got {as_of_grain!r}")
    return as_of_grain


def validate_serving_layout(layout: str) -> str:
    normalized = layout.strip().lower()
    if normalized not in VALID_SERVING_LAYOUTS:
        raise ValueError(f"serving layout must be one of {sorted(VALID_SERVING_LAYOUTS)}, got {layout!r}")
    return normalized


def default_table_for_layout(layout: str) -> str:
    return WIDE_SERVING_TABLE if validate_serving_layout(layout) == "wide" else SERVING_TABLE


def metric_slot_column(index: int) -> str:
    if index < 1 or index > MAX_METRIC_SLOTS:
        raise ValueError(f"metric slot index {index} exceeds MAX_METRIC_SLOTS={MAX_METRIC_SLOTS}")
    return f"c{index:03d}"


def metric_slot_columns(count: int) -> list[str]:
    if count > MAX_METRIC_SLOTS:
        raise ValueError(f"bundle needs {count} metric slots but MAX_METRIC_SLOTS={MAX_METRIC_SLOTS}")
    return [metric_slot_column(index) for index in range(1, count + 1)]


def serving_lookup_key(reference_time: datetime, as_of_grain: str) -> str:
    validate_as_of_grain(as_of_grain)
    if as_of_grain == "day":
        return reference_time.date().isoformat()
    return reference_time.strftime("%Y-%m-%d %H:%M:%S.%f")


def serving_reference_time(reference_time: datetime, as_of_grain: str) -> datetime:
    """Return the timestamp represented by the serving row.

    Timestamp-grain rows preserve the exact event reference time. Day-grain rows
    intentionally represent the end of that calendar day, which is a serving
    design choice for high reuse rather than per-event rolling timestamp exactness.
    """

    validate_as_of_grain(as_of_grain)
    if as_of_grain == "timestamp":
        return reference_time
    return datetime.combine(reference_time.date(), datetime.max.time()).replace(tzinfo=reference_time.tzinfo)


def create_serving_table_sql(table_name: str = SERVING_TABLE) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (
  as_of_grain VARCHAR(16) NOT NULL,
  as_of_key VARCHAR(32) NOT NULL,
  bundle_id VARCHAR(64) NOT NULL,
  key1 VARCHAR(256) NOT NULL,
  key2 VARCHAR(256) NOT NULL DEFAULT '',
  metric_name VARCHAR(128) NOT NULL,
  metric_value DECIMAL(38,6) DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (as_of_grain, as_of_key, bundle_id, key1, key2, metric_name),
  KEY idx_serving_lookup (bundle_id, key1, key2, as_of_grain, as_of_key)
);
""".strip()


def create_wide_serving_table_sql(table_name: str = WIDE_SERVING_TABLE) -> str:
    metric_columns = ",\n  ".join(
        f"{quote_ident(metric_slot_column(index))} DECIMAL(38,6) DEFAULT NULL"
        for index in range(1, MAX_METRIC_SLOTS + 1)
    )
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (
  as_of_grain VARCHAR(16) NOT NULL,
  as_of_key VARCHAR(32) NOT NULL,
  bundle_id VARCHAR(64) NOT NULL,
  key1 VARCHAR(256) NOT NULL,
  key2 VARCHAR(256) NOT NULL DEFAULT '',
  metric_count SMALLINT UNSIGNED NOT NULL,
  {metric_columns},
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (as_of_grain, as_of_key, bundle_id, key1, key2),
  KEY idx_serving_lookup (bundle_id, key1, key2, as_of_grain, as_of_key)
);
""".strip()


def create_array_serving_table_sql(table_name: str = ARRAY_SERVING_TABLE) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (
  as_of_grain VARCHAR(16) NOT NULL,
  as_of_key VARCHAR(32) NOT NULL,
  bundle_id VARCHAR(64) NOT NULL,
  key1 VARCHAR(256) NOT NULL,
  key2 VARCHAR(256) NOT NULL DEFAULT '',
  metric_count SMALLINT UNSIGNED NOT NULL,
  metric_values JSON DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (as_of_grain, as_of_key, bundle_id, key1, key2),
  KEY idx_serving_lookup (bundle_id, key1, key2, as_of_grain, as_of_key)
);
""".strip()


def render_kv_serving_query(bundle, as_of_grain: str = "day", table_name: str = SERVING_TABLE) -> str:
    select_parts = [
        f"MAX(CASE WHEN metric_name = '{name}' THEN metric_value END) AS {quote_ident(name)}"
        for name in metric_names_for_bundle(bundle)
    ]
    key2_predicate = "s.key2 = %s" if len(key_fields(bundle)) > 1 else "s.key2 = ''"
    return f"""
SELECT
  {",\n  ".join(select_parts)}
FROM {quote_ident(table_name)} s
WHERE s.as_of_grain = %s
  AND s.as_of_key = %s
  AND s.bundle_id = '{bundle.bundle_id}'
  AND s.key1 = %s
  AND {key2_predicate};
""".strip()


def render_wide_serving_query(bundle, as_of_grain: str = "day", table_name: str = WIDE_SERVING_TABLE) -> str:
    names = metric_names_for_bundle(bundle)
    select_parts = [
        f"s.{quote_ident(slot)} AS {quote_ident(name)}"
        for slot, name in zip(metric_slot_columns(len(names)), names)
    ]
    key2_predicate = "s.key2 = %s" if len(key_fields(bundle)) > 1 else "s.key2 = ''"
    return f"""
SELECT
  {",\n  ".join(select_parts)}
FROM {quote_ident(table_name)} s
WHERE s.as_of_grain = %s
  AND s.as_of_key = %s
  AND s.bundle_id = '{bundle.bundle_id}'
  AND s.key1 = %s
  AND {key2_predicate};
""".strip()


def render_serving_query(
    bundle,
    reference_time: datetime,
    as_of_grain: str = "day",
    table_name: str | None = None,
    layout: str | None = None,
) -> str:
    selected_layout = validate_serving_layout(layout or SERVING_LAYOUT)
    selected_table = table_name or default_table_for_layout(selected_layout)
    if selected_layout == "wide":
        return render_wide_serving_query(bundle, as_of_grain, selected_table)
    return render_kv_serving_query(bundle, as_of_grain, selected_table)


def serving_params(bundle, reference_time: datetime, bindings: dict[str, Any], as_of_grain: str = "day") -> tuple[Any, ...]:
    fields = key_fields(bundle)
    values = ["" if bindings.get(field) is None else str(bindings.get(field)) for field in fields]
    params: list[Any] = [as_of_grain, serving_lookup_key(reference_time, as_of_grain), values[0]]
    if len(values) > 1:
        params.append(values[1])
    return tuple(params)


def combined_serving_slots(bundles: list[Any]) -> list[str]:
    max_metrics = max((len(metric_names_for_bundle(bundle)) for bundle in bundles), default=0)
    return metric_slot_columns(max_metrics)


def render_combined_serving_query(
    bundles: list[Any],
    as_of_grain: str = "day",
    table_name: str | None = None,
) -> str:
    selected_table = table_name or WIDE_SERVING_TABLE
    slots = combined_serving_slots(bundles)
    row_placeholders = ", ".join(["(%s, %s, %s)"] * len(bundles))
    slot_select = ",\n  ".join(f"s.{quote_ident(slot)}" for slot in slots)
    return f"""
SELECT
  s.bundle_id,
  s.key1,
  s.key2,
  s.metric_count{"," if slot_select else ""}
  {slot_select}
FROM {quote_ident(selected_table)} s
WHERE s.as_of_grain = %s
  AND s.as_of_key = %s
  AND (s.bundle_id, s.key1, s.key2) IN ({row_placeholders});
""".strip()


def render_combined_array_serving_query(
    bundles: list[Any],
    as_of_grain: str = "day",
    table_name: str | None = None,
) -> str:
    selected_table = table_name or ARRAY_SERVING_TABLE
    row_placeholders = ", ".join(["(%s, %s, %s)"] * len(bundles))
    return f"""
SELECT
  s.bundle_id,
  s.key1,
  s.key2,
  s.metric_count,
  s.metric_values
FROM {quote_ident(selected_table)} s
WHERE s.as_of_grain = %s
  AND s.as_of_key = %s
  AND (s.bundle_id, s.key1, s.key2) IN ({row_placeholders});
""".strip()


def combined_serving_params(
    bundles: list[Any],
    reference_time: datetime,
    bindings: dict[str, Any],
    as_of_grain: str = "day",
) -> tuple[Any, ...]:
    params: list[Any] = [as_of_grain, serving_lookup_key(reference_time, as_of_grain)]
    for bundle in bundles:
        fields = key_fields(bundle)
        values = ["" if bindings.get(field) is None else str(bindings.get(field)) for field in fields]
        key1 = values[0] if values else ""
        key2 = values[1] if len(values) > 1 else ""
        params.extend([bundle.bundle_id, key1, key2])
    return tuple(params)


def prod180_params_for_bundle(bundle, group: str, reference_time: datetime, bindings: dict[str, Any]) -> tuple[Any, ...]:
    key_values = tuple("" if bindings.get(k) is None else str(bindings.get(k)) for k in key_fields(bundle))
    rollups, distincts = bundle_rollup_metrics(group, bundle)
    params: list[Any] = []
    if rollups == [] and all(not tmpl.extra_predicate for tmpl in bundle.templates):
        if prod180_group_c_distinct_uses_hourly(group, bundle, distincts):
            for _ in range(2 if group == "C" else 1):
                params.extend(key_values)
            params.extend(key_values)
            params.extend(key_values)
        else:
            params.extend(key_values)
            params.extend(key_values)
        return tuple(params)
    if not rollups and any(distinct.extra_predicate for distinct in distincts) and any(not distinct.extra_predicate for distinct in distincts):
        use_hourly = prod180_use_hourly_boundary() and group in prod180_hourly_boundary_groups()
        if use_hourly:
            for _ in range(2 if group == "C" else 1):
                params.extend(key_values)
            params.extend(key_values)
            params.extend(key_values)
        else:
            params.extend(key_values)
            params.extend(key_values)
        for distinct in distincts:
            if distinct.extra_predicate:
                if use_hourly:
                    params.extend(key_values)
                    params.extend(key_values)
                    params.extend(key_values)
                    params.extend(key_values)
                    params.extend(key_values)
                    params.extend(key_values)
                else:
                    params.extend(key_values)
                    params.extend(key_values)
                    params.extend(key_values)
                    params.extend(key_values)
        return tuple(params)
    if group == "C" and rollups and distincts and all(not tmpl.extra_predicate for tmpl in bundle.templates):
        params.extend(key_values)
        params.extend(key_values)
        params.extend(key_values)
        if prod180_group_c_mixed_uses_hourly(group, bundle, rollups, distincts):
            params.extend(key_values)
            params.extend(key_values)
            params.extend(key_values)
        return tuple(params)
    for tmpl in bundle.templates:
        if not tmpl.select_expr.strip().upper().startswith("COUNT(DISTINCT"):
            continue
        params.extend(key_values)
        params.extend(key_values)
        if tmpl.extra_predicate:
            params.extend(key_values)
            params.extend(key_values)
    if rollups:
        params.extend(key_values)
        params.extend(key_values)
    return tuple(params)


def runtime_params_for_bundle(bundle, reference_time: datetime, bindings: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(bindings.get(name) for name in bundle.param_names)


def source_query_and_params(group: str, bundle, reference_time: datetime, bindings: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    if bundle.bundle_id in PROD180_PREAGG_BUNDLES and bundle.window_days == 180:
        return render_prod180_runtime_query(group, bundle, reference_time), prod180_params_for_bundle(
            bundle, group, reference_time, bindings
        )
    return bundle.render_sql(reference_time), runtime_params_for_bundle(bundle, reference_time, bindings)


def metric_expr_for_group(group: str, tmpl) -> str:
    if group == "A":
        return build_group_a_metric_expr(tmpl)
    if group == "B":
        return build_group_b_metric_expr(tmpl)
    if group == "C":
        return build_group_c_metric_expr(tmpl)
    raise ValueError(f"Unknown group {group!r}")


def key_batch_filter(group_by_fields: tuple[str, ...], keys: list[ServingKey]) -> tuple[str, tuple[Any, ...]]:
    if not keys:
        raise ValueError("keys must not be empty")
    if len(group_by_fields) == 1:
        placeholders = ", ".join(["%s"] * len(keys))
        return f"{group_by_fields[0]} IN ({placeholders})", tuple(key.key1 for key in keys)
    if len(group_by_fields) == 2:
        placeholders = ", ".join(["(%s, %s)"] * len(keys))
        params: list[Any] = []
        for key in keys:
            params.extend([key.key1, key.key2])
        return f"({group_by_fields[0]}, {group_by_fields[1]}) IN ({placeholders})", tuple(params)
    raise ValueError(f"Unsupported key width for batch serving build: {group_by_fields!r}")


def render_batch_source_query(group: str, bundle, reference_time: datetime, keys: list[ServingKey]) -> tuple[str, tuple[Any, ...]]:
    key_filter, params = key_batch_filter(bundle.group_by_fields, keys)
    key_select = [f"{field} AS __key{index}" for index, field in enumerate(bundle.group_by_fields, start=1)]
    metric_select: list[str] = []
    for tmpl in bundle.templates:
        metric_select.append(f"{metric_expr_for_group(group, tmpl)} AS {quote_ident(metric_column(tmpl.template_id))}")
        if tmpl.extra_predicate:
            metric_select.append(f"{build_presence_expr(tmpl.extra_predicate)} AS {quote_ident(presence_column(tmpl.template_id))}")
    select_clause = ",\n  ".join([*key_select, *metric_select])
    group_by_clause = ", ".join(bundle.group_by_fields)

    if group == "A":
        cutoff_ms = int((reference_time.timestamp() - (bundle.window_days * 86400)) * 1000)
        sql = f"""
SELECT
  {select_clause}
FROM pmt_txn_fact p
WHERE {key_filter}
  AND p.event_date >= {cutoff_ms}
GROUP BY {group_by_clause}
""".strip()
        return sql, params

    if group == "B":
        cutoff_literal = (reference_time - timedelta(days=bundle.window_days)).strftime("%Y-%m-%d %H:%M:%S.%f")
        sql = f"""
SELECT
  {select_clause}
FROM deviceprofile_fact d
WHERE {key_filter}
  AND d.jms_timestamp >= '{cutoff_literal}'
GROUP BY {group_by_clause}
""".strip()
        return sql, params

    if group == "C":
        cutoff_ms = int(reference_time.timestamp() * 1000) - (bundle.window_days * 86400 * 1000)
        cutoff_dt = datetime.fromtimestamp(cutoff_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        if bundle.bundle_id in GROUP_C_DEVICE_FIRST_JOIN_BUNDLES:
            from_clause = "FROM deviceprofile_fact d\nJOIN pmt_txn_fact p\n  ON p.parsed_interaction_id = d.interaction_id"
        else:
            join_keyword = "JOIN" if bundle.bundle_id in GROUP_C_INNER_JOIN_BUNDLES else "LEFT OUTER JOIN"
            from_clause = (
                "FROM pmt_txn_fact p\n"
                f"{join_keyword} deviceprofile_fact d\n"
                "  ON p.parsed_interaction_id = d.interaction_id"
            )
        sql = f"""
SELECT
  {select_clause}
{from_clause}
WHERE {key_filter}
  AND p.event_date >= {cutoff_ms}
  AND d.jms_timestamp >= '{cutoff_dt}'
GROUP BY {group_by_clause}
""".strip()
        return sql, params

    raise ValueError(f"Unknown group {group!r}")


def iter_events_from_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return list(payload.get("sampled_normal_events", [])) + list(payload.get("sampled_hot_events", []))


def collect_serving_keys(
    events: list[dict[str, Any]],
    bundle_ids: list[str],
    as_of_grain: str,
    limit_keys: int | None = None,
) -> list[ServingKey]:
    catalog = all_bundles()
    seen: set[tuple[str, str, str, str, str]] = set()
    keys: list[ServingKey] = []
    for event in events:
        reference_time = datetime.fromisoformat(event["reference_time"])
        build_reference_time = serving_reference_time(reference_time, as_of_grain)
        as_of_key = serving_lookup_key(reference_time, as_of_grain)
        bindings = event["bindings"]
        for bundle_id in bundle_ids:
            group, bundle = catalog[bundle_id]
            fields = key_fields(bundle)
            values = [bindings.get(field) for field in fields]
            if not values or any(value is None for value in values):
                continue
            key1 = str(values[0])
            key2 = str(values[1]) if len(values) > 1 else ""
            dedupe_key = (bundle_id, as_of_grain, as_of_key, key1, key2)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            keys.append(
                ServingKey(
                    bundle_id=bundle_id,
                    group=group,
                    as_of_grain=as_of_grain,
                    as_of_key=as_of_key,
                    reference_time=build_reference_time,
                    key1=key1,
                    key2=key2,
                    bindings=bindings,
                )
            )
            if limit_keys is not None and len(keys) >= limit_keys:
                return keys
    return keys


def upsert_metric_rows(cur, table_name: str, key: ServingKey, columns: list[str], row: tuple[Any, ...]) -> None:
    sql = f"""
REPLACE INTO {quote_ident(table_name)}
  (as_of_grain, as_of_key, bundle_id, key1, key2, metric_name, metric_value)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""".strip()
    rows = [
        (key.as_of_grain, key.as_of_key, key.bundle_id, key.key1, key.key2, column, value)
        for column, value in zip(columns, row)
    ]
    cur.executemany(sql, rows)


def upsert_wide_row(cur, table_name: str, key: ServingKey, bundle, columns: list[str], row: tuple[Any, ...]) -> None:
    metric_names = metric_names_for_bundle(bundle)
    slot_columns = metric_slot_columns(len(metric_names))
    value_by_name = {column: value for column, value in zip(columns, row)}
    metric_values = [value_by_name.get(name) for name in metric_names]
    insert_columns = ["as_of_grain", "as_of_key", "bundle_id", "key1", "key2", "metric_count", *slot_columns]
    placeholders = ", ".join(["%s"] * len(insert_columns))
    sql = f"""
REPLACE INTO {quote_ident(table_name)}
  ({", ".join(quote_ident(column) for column in insert_columns)})
VALUES ({placeholders})
""".strip()
    cur.execute(
        sql,
        (
            key.as_of_grain,
            key.as_of_key,
            key.bundle_id,
            key.key1,
            key.key2,
            len(metric_names),
            *metric_values,
        ),
    )


def connect_for_builder():
    import pymysql

    from lib.db_config import get_db_config

    conn = pymysql.connect(**get_db_config(save_msg="exact serving builder"))
    conn.autocommit(True)
    configure_session(conn)
    return conn


def build_one_serving_row(
    conn,
    key: ServingKey,
    table_name: str,
    index: int,
    total: int,
    dry_run: bool = False,
    layout: str = "kv",
) -> dict[str, Any]:
    selected_layout = validate_serving_layout(layout)
    catalog = all_bundles()
    group, bundle = catalog[key.bundle_id]
    sql, params = source_query_and_params(group, bundle, key.reference_time, key.bindings)
    if dry_run:
        print(f"-- [{index}/{total}] {key.bundle_id} {key.as_of_grain}:{key.as_of_key} key=({key.key1}, {key.key2})")
        print(sql)
        print(f"-- params={params}\n")
        return {
            "bundle_id": key.bundle_id,
            "as_of_grain": key.as_of_grain,
            "as_of_key": key.as_of_key,
            "key1": key.key1,
            "key2": key.key2,
            "metric_count": 0,
            "elapsed_ms": 0.0,
        }
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        columns = [desc[0] for desc in cur.description]
        if row is None:
            row = tuple(None for _ in columns)
        if selected_layout == "wide":
            upsert_wide_row(cur, table_name, key, bundle, columns, row)
        else:
            upsert_metric_rows(cur, table_name, key, columns, row)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    result = {
        "bundle_id": key.bundle_id,
        "as_of_grain": key.as_of_grain,
        "as_of_key": key.as_of_key,
        "key1": key.key1,
        "key2": key.key2,
        "metric_count": len(columns),
        "elapsed_ms": elapsed_ms,
    }
    print(
        f"[{index}/{total}] {key.bundle_id} {key.as_of_key} "
        f"key=({key.key1}, {key.key2}) metrics={len(columns)} build_ms={elapsed_ms:.1f}",
        flush=True,
    )
    return result


def build_serving_rows(
    conn,
    keys: list[ServingKey],
    table_name: str,
    dry_run: bool = False,
    workers: int = 1,
    layout: str = "kv",
) -> list[dict[str, Any]]:
    if workers <= 1 or dry_run:
        if conn is None and not dry_run:
            raise ValueError("conn is required for single-worker serving build")
        return [
            build_one_serving_row(conn, key, table_name, index, len(keys), dry_run=dry_run, layout=layout)
            for index, key in enumerate(keys, start=1)
        ]

    local = threading.local()
    created_conns: list[Any] = []
    conn_lock = threading.Lock()

    def worker_conn():
        worker_connection = getattr(local, "conn", None)
        if worker_connection is None:
            worker_connection = connect_for_builder()
            local.conn = worker_connection
            with conn_lock:
                created_conns.append(worker_connection)
        return worker_connection

    def run(index_key: tuple[int, ServingKey]) -> dict[str, Any]:
        index, key = index_key
        return build_one_serving_row(worker_conn(), key, table_name, index, len(keys), dry_run=False, layout=layout)

    results: list[dict[str, Any]] = []
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(run, index_key) for index_key in enumerate(keys, start=1)]
            for future in as_completed(futures):
                results.append(future.result())
    finally:
        for worker_connection in created_conns:
            worker_connection.close()
    return results


def grouped_batch_keys(keys: list[ServingKey]) -> list[tuple[tuple[str, str, str], list[ServingKey]]]:
    grouped: dict[tuple[str, str, str], list[ServingKey]] = {}
    for key in keys:
        grouped.setdefault((key.bundle_id, key.as_of_grain, key.as_of_key), []).append(key)
    return sorted(grouped.items(), key=lambda item: item[0])


def build_one_batch_serving_group(
    conn,
    group_key: tuple[str, str, str],
    keys: list[ServingKey],
    table_name: str,
    index: int,
    total: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    catalog = all_bundles()
    bundle_id, as_of_grain, as_of_key = group_key
    group, bundle = catalog[bundle_id]
    reference_time = keys[0].reference_time
    sql, params = render_batch_source_query(group, bundle, reference_time, keys)
    if dry_run:
        print(f"-- [batch {index}/{total}] {bundle_id} {as_of_grain}:{as_of_key} keys={len(keys)}")
        print(sql)
        print(f"-- params={params[:20]}{' ...' if len(params) > 20 else ''}\n")
        return {
            "bundle_id": bundle_id,
            "as_of_grain": as_of_grain,
            "as_of_key": as_of_key,
            "keys": len(keys),
            "returned_rows": 0,
            "elapsed_ms": 0.0,
        }

    metric_names = metric_names_for_bundle(bundle)
    key_width = len(bundle.group_by_fields)
    key_map = {(key.key1,) if key_width == 1 else (key.key1, key.key2): key for key in keys}
    started = time.perf_counter()
    returned_by_key: dict[tuple[str, ...], tuple[list[str], tuple[Any, ...]]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        descriptions = [desc[0] for desc in cur.description]
        metric_columns = descriptions[key_width:]
        for row in cur.fetchall():
            returned_key = tuple("" if value is None else str(value) for value in row[:key_width])
            returned_by_key[returned_key] = (metric_columns, tuple(row[key_width:]))
        null_row = tuple(None for _ in metric_names)
        for key_tuple, key in key_map.items():
            columns, metric_row = returned_by_key.get(key_tuple, (metric_names, null_row))
            upsert_wide_row(cur, table_name, key, bundle, columns, metric_row)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    result = {
        "bundle_id": bundle_id,
        "as_of_grain": as_of_grain,
        "as_of_key": as_of_key,
        "keys": len(keys),
        "returned_rows": len(returned_by_key),
        "elapsed_ms": elapsed_ms,
    }
    print(
        f"[batch {index}/{total}] {bundle_id} {as_of_key} "
        f"keys={len(keys)} rows={len(returned_by_key)} build_ms={elapsed_ms:.1f}",
        flush=True,
    )
    return result


def build_batch_serving_rows(
    conn,
    keys: list[ServingKey],
    table_name: str,
    dry_run: bool = False,
    workers: int = 1,
) -> list[dict[str, Any]]:
    batches = grouped_batch_keys(keys)
    if workers <= 1 or dry_run:
        if conn is None and not dry_run:
            raise ValueError("conn is required for single-worker serving batch build")
        return [
            build_one_batch_serving_group(conn, group_key, group_keys, table_name, index, len(batches), dry_run=dry_run)
            for index, (group_key, group_keys) in enumerate(batches, start=1)
        ]

    local = threading.local()
    created_conns: list[Any] = []
    conn_lock = threading.Lock()

    def worker_conn():
        worker_connection = getattr(local, "conn", None)
        if worker_connection is None:
            worker_connection = connect_for_builder()
            local.conn = worker_connection
            with conn_lock:
                created_conns.append(worker_connection)
        return worker_connection

    def run(index_group: tuple[int, tuple[tuple[str, str, str], list[ServingKey]]]) -> dict[str, Any]:
        index, (group_key, group_keys) = index_group
        return build_one_batch_serving_group(worker_conn(), group_key, group_keys, table_name, index, len(batches), dry_run=False)

    results: list[dict[str, Any]] = []
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(run, index_group) for index_group in enumerate(batches, start=1)]
            for future in as_completed(futures):
                results.append(future.result())
    finally:
        for worker_connection in created_conns:
            worker_connection.close()
    return results


def migrate_wide_rows(conn, bundle_ids: list[str], source_table: str, dest_table: str) -> list[dict[str, Any]]:
    catalog = all_bundles()
    results: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        for bundle_id in bundle_ids:
            _, bundle = catalog[bundle_id]
            metric_names = metric_names_for_bundle(bundle)
            slot_columns = metric_slot_columns(len(metric_names))
            select_parts = [
                f"MAX(CASE WHEN metric_name = '{name}' THEN metric_value END) AS {quote_ident(slot)}"
                for slot, name in zip(slot_columns, metric_names)
            ]
            insert_columns = ["as_of_grain", "as_of_key", "bundle_id", "key1", "key2", "metric_count", *slot_columns]
            sql = f"""
REPLACE INTO {quote_ident(dest_table)}
  ({", ".join(quote_ident(column) for column in insert_columns)})
SELECT
  as_of_grain,
  as_of_key,
  bundle_id,
  key1,
  key2,
  {len(metric_names)} AS metric_count,
  {",\n  ".join(select_parts)}
FROM {quote_ident(source_table)}
WHERE bundle_id = %s
GROUP BY as_of_grain, as_of_key, bundle_id, key1, key2
""".strip()
            started = time.perf_counter()
            cur.execute(sql, (bundle_id,))
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            result = {
                "bundle_id": bundle_id,
                "metric_count": len(metric_names),
                "affected_rows": cur.rowcount,
                "elapsed_ms": elapsed_ms,
            }
            results.append(result)
            print(
                f"[migrate-wide] {bundle_id} metrics={len(metric_names)} rows={cur.rowcount} elapsed_ms={elapsed_ms:.1f}",
                flush=True,
            )
    return results


def migrate_array_rows(conn, bundle_ids: list[str], source_table: str, dest_table: str) -> list[dict[str, Any]]:
    catalog = all_bundles()
    results: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        for bundle_id in bundle_ids:
            _, bundle = catalog[bundle_id]
            metric_names = metric_names_for_bundle(bundle)
            slot_columns = metric_slot_columns(len(metric_names))
            sql = f"""
REPLACE INTO {quote_ident(dest_table)}
  (as_of_grain, as_of_key, bundle_id, key1, key2, metric_count, metric_values)
SELECT
  as_of_grain,
  as_of_key,
  bundle_id,
  key1,
  key2,
  metric_count,
  JSON_ARRAY({", ".join(quote_ident(column) for column in slot_columns)}) AS metric_values
FROM {quote_ident(source_table)}
WHERE bundle_id = %s
""".strip()
            started = time.perf_counter()
            cur.execute(sql, (bundle_id,))
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            result = {
                "bundle_id": bundle_id,
                "metric_count": len(metric_names),
                "affected_rows": cur.rowcount,
                "elapsed_ms": elapsed_ms,
            }
            results.append(result)
            print(
                f"[migrate-array] {bundle_id} metrics={len(metric_names)} rows={cur.rowcount} elapsed_ms={elapsed_ms:.1f}",
                flush=True,
            )
    return results


def load_existing_serving_keys(conn, table_name: str, bundle_ids: list[str]) -> set[tuple[str, str, str, str, str]]:
    if not bundle_ids:
        return set()
    placeholders = ", ".join(["%s"] * len(bundle_ids))
    sql = f"""
SELECT bundle_id, as_of_grain, as_of_key, key1, key2
FROM {quote_ident(table_name)}
WHERE bundle_id IN ({placeholders})
""".strip()
    with conn.cursor() as cur:
        cur.execute(sql, tuple(bundle_ids))
        return {
            (
                str(bundle_id),
                str(as_of_grain),
                str(as_of_key),
                str(key1),
                str(key2),
            )
            for bundle_id, as_of_grain, as_of_key, key1, key2 in cur.fetchall()
        }


def selected_bundle_ids(raw: list[str] | None) -> list[str]:
    bundle_ids = raw or sorted(EXACT_SERVING_BUNDLES)
    catalog = all_bundles()
    unknown = sorted(set(bundle_ids) - set(catalog))
    if unknown:
        raise SystemExit(f"Unknown bundle ids: {', '.join(unknown)}")
    return bundle_ids


def configure_session(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET SESSION tidb_isolation_read_engines = %s", (os.getenv("TIDB_ISOLATION_READ_ENGINES", "tikv,tidb"),))
        if os.getenv("INTUIT_FORCE_INLINE_CTE", "0") in {"0", "1"}:
            cur.execute(f"SET SESSION tidb_opt_force_inline_cte = {int(os.getenv('INTUIT_FORCE_INLINE_CTE', '0'))}")
        if os.getenv("INTUIT_DISTINCT_AGG_PUSH_DOWN") == "1":
            cur.execute("SET SESSION tidb_opt_distinct_agg_push_down = 1")
        if os.getenv("READ_MAX_EXECUTION_TIME_MS"):
            cur.execute("SET SESSION max_execution_time = %s", (int(os.environ["READ_MAX_EXECUTION_TIME_MS"]),))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["create", "create-array", "build", "build-batch", "render", "migrate-wide", "migrate-array"])
    parser.add_argument("--bundle", action="append", dest="bundles", help="Bundle id to serve. Repeatable.")
    parser.add_argument("--source-events-json", default="results/mixed_traffic_1780094259.json")
    parser.add_argument("--as-of-grain", choices=sorted(VALID_AS_OF_GRAINS), default=os.getenv("INTUIT_SERVING_AS_OF_GRAIN", "day"))
    parser.add_argument("--layout", choices=sorted(VALID_SERVING_LAYOUTS), default=SERVING_LAYOUT)
    parser.add_argument("--table", default=None)
    parser.add_argument("--source-table", default=SERVING_TABLE)
    parser.add_argument("--limit-keys", type=int, default=None)
    parser.add_argument("--workers", type=int, default=1, help="Parallel build workers. Each worker opens its own TiDB connection.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip bundle/key/as-of rows already present in the target serving table.")
    parser.add_argument("--execute", action="store_true", help="Execute create/build. Without this, print SQL/work only.")
    parser.add_argument("--output", default=None, help="Optional JSON result path for build action.")
    args = parser.parse_args()

    bundle_ids = selected_bundle_ids(args.bundles)
    layout = validate_serving_layout(args.layout)
    table_name = args.table or default_table_for_layout(layout)
    if args.action == "render":
        catalog = all_bundles()
        now = datetime.now()
        for bundle_id in bundle_ids:
            _, bundle = catalog[bundle_id]
            print(f"-- {bundle_id}")
            print(render_serving_query(bundle, now, args.as_of_grain, table_name, layout=layout))
            print()
        return

    conn = None
    if args.execute:
        import pymysql

        from lib.db_config import get_db_config

        conn = pymysql.connect(**get_db_config(save_msg="exact serving builder"))
        conn.autocommit(True)
        configure_session(conn)

    try:
        if args.action == "create":
            sql = create_wide_serving_table_sql(table_name) if layout == "wide" else create_serving_table_sql(table_name)
            print(sql)
            if conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
            return

        if args.action == "create-array":
            array_table = args.table or ARRAY_SERVING_TABLE
            sql = create_array_serving_table_sql(array_table)
            print(sql)
            if conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
            return

        if args.action == "migrate-wide":
            if layout != "wide":
                raise SystemExit("migrate-wide requires --layout wide")
            if not conn:
                print(f"-- would migrate bundles={','.join(bundle_ids)} from {args.source_table} to {table_name}")
                return
            results = migrate_wide_rows(conn, bundle_ids, args.source_table, table_name)
            if args.output:
                out = ROOT / args.output
                out.write_text(json.dumps({"results": results}, indent=2, default=str), encoding="utf-8")
                print(f"Saved: {out}")
            return

        if args.action == "migrate-array":
            array_table = args.table or ARRAY_SERVING_TABLE
            if not conn:
                print(f"-- would migrate bundles={','.join(bundle_ids)} from {args.source_table} to {array_table}")
                return
            results = migrate_array_rows(conn, bundle_ids, args.source_table, array_table)
            if args.output:
                out = ROOT / args.output
                out.write_text(json.dumps({"results": results}, indent=2, default=str), encoding="utf-8")
                print(f"Saved: {out}")
            return

        events = iter_events_from_json(ROOT / args.source_events_json)
        keys = collect_serving_keys(events, bundle_ids, args.as_of_grain, args.limit_keys)
        if args.skip_existing:
            if not conn:
                print("-- skip-existing needs --execute so the target table can be inspected")
            else:
                existing = load_existing_serving_keys(conn, table_name, bundle_ids)
                before = len(keys)
                keys = [
                    key
                    for key in keys
                    if (key.bundle_id, key.as_of_grain, key.as_of_key, key.key1, key.key2) not in existing
                ]
                print(f"-- skip-existing removed {before - len(keys)} existing rows from {before} collected keys")
        print(
            f"-- serving build keys={len(keys)} bundles={','.join(bundle_ids)} "
            f"as_of_grain={args.as_of_grain} layout={layout} table={table_name}"
        )
        if args.action == "build-batch":
            if layout != "wide":
                raise SystemExit("build-batch currently requires --layout wide")
            results = build_batch_serving_rows(
                conn,
                keys,
                table_name,
                dry_run=not args.execute,
                workers=max(1, args.workers),
            )
        else:
            results = build_serving_rows(
                conn,
                keys,
                table_name,
                dry_run=not args.execute,
                workers=max(1, args.workers),
                layout=layout,
            )
        if args.output:
            out = ROOT / args.output
            out.write_text(json.dumps({"keys": len(keys), "results": results}, indent=2, default=str), encoding="utf-8")
            print(f"Saved: {out}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
