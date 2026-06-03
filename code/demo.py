#!/usr/bin/env python3
"""Single entrypoint for the Intuit TiDB demo benchmark.

Use this file for handoff:

  python3 demo.py schema
  python3 demo.py load --scale small
  python3 demo.py baseline --output benchmark_results/naive.json
  python3 demo.py optimized --baseline benchmark_results/naive.json
  python3 demo.py full --baseline-output benchmark_results/naive.json --optimized-output benchmark_results/optimized.json

The optimized command contains the final 65-bundle implementation directly,
including the two validated Group A TiFlash hints.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
from typing import Any

from lib.db_config import get_db_config
from lib.event_benchmark import ConnectionPool, _json_safe
from lib.query_templates import load_query_templates


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "benchmark_results"
DEFAULT_HINTED_GROUP_A_BUNDLES = {"group_a_bundle_002", "group_a_bundle_006"}
DEFAULT_GROUP_A_DIMENSION_ROLLUP_BUNDLES = {
    "group_a_bundle_002",
    "group_a_bundle_006",
    "group_a_bundle_010",
    "group_a_bundle_012",
    "group_a_bundle_014",
}
# Bundles verified in the support run. The renderer also auto-detects the same
# shape for other bundles whose GROUP BY keys are fixed by equality predicates.
DEFAULT_NO_GROUP_BY_BUNDLES = {
    "group_a_bundle_008",
    "group_a_bundle_010",
    "group_a_bundle_014",
    "group_b_bundle_008",
    "group_b_bundle_012",
    "group_c_bundle_007",
    "group_c_bundle_011",
    "group_c_bundle_014",
    "group_c_bundle_018",
    "group_c_bundle_021",
}
GROUP_FILTER_RE = re.compile(r"\b([pd]\.[a-z0-9_]+)\s*=\s*%s\b", re.I)


def parse_bundle_set_env(env_name: str, default: set[str]) -> set[str]:
    raw = os.getenv(env_name)
    if raw is None:
        return set(default)
    stripped = raw.strip()
    if stripped.lower() in {"", "0", "false", "none", "off"}:
        return set()
    if stripped.lower() in {"1", "true", "default", "auto"}:
        return set(default)
    return {item for item in re.split(r"[\s,]+", stripped) if item}


GROUP_A_DIMENSION_ROLLUP_BUNDLES = parse_bundle_set_env(
    "INTUIT_GROUP_A_DIMENSION_ROLLUP_BUNDLES",
    DEFAULT_GROUP_A_DIMENSION_ROLLUP_BUNDLES,
)

DEFAULT_GROUP_C_INNER_JOIN_BUNDLES: set[str] = {
    "group_c_bundle_016",
    "group_c_bundle_017",
    "group_c_bundle_018",
    "group_c_bundle_021",
}
DEFAULT_GROUP_C_DEVICE_FIRST_JOIN_BUNDLES: set[str] = set()
GROUP_C_INNER_JOIN_BUNDLES = parse_bundle_set_env(
    "INTUIT_GROUP_C_INNER_JOIN_BUNDLES",
    DEFAULT_GROUP_C_INNER_JOIN_BUNDLES,
)
GROUP_C_DEVICE_FIRST_JOIN_BUNDLES = parse_bundle_set_env(
    "INTUIT_GROUP_C_DEVICE_FIRST_JOIN_BUNDLES",
    DEFAULT_GROUP_C_DEVICE_FIRST_JOIN_BUNDLES,
)
RUNTIME_WINDOW_UPPER_BOUND = os.getenv("INTUIT_RUNTIME_WINDOW_UPPER_BOUND", "0").strip().lower() in {
    "1",
    "true",
    "on",
    "yes",
}


def has_redundant_group_by(bundle_id: str, base_filter: str, group_by_fields: tuple[str, ...]) -> bool:
    if bundle_id in DEFAULT_NO_GROUP_BY_BUNDLES:
        return True
    fixed_fields = {match.lower() for match in GROUP_FILTER_RE.findall(base_filter)}
    return bool(group_by_fields) and {field.lower() for field in group_by_fields}.issubset(fixed_fields)


def run_command(args: list[str], env: dict[str, str] | None = None) -> int:
    print("+ " + " ".join(args), flush=True)
    return subprocess.call(args, env=env)


def metric_column(template_id: str) -> str:
    return f"metric__{template_id}"


def presence_column(template_id: str) -> str:
    return f"present__{template_id}"


def load_baseline(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return Decimal(value)
    return value


def normalize_rows(rows: list[list[Any]]) -> list[list[Any]]:
    return [[normalize_scalar(cell) for cell in row] for row in rows]


def safe_percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    rank = (len(ordered) - 1) * (pct / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


# ---------------------------------------------------------------------------
# Group A bundling
# ---------------------------------------------------------------------------

A_SELECT_RE = re.compile(r"^SELECT\s+(.*?)\s+FROM\s+pmt_txn_fact", re.I)
A_WHERE_RE = re.compile(
    r"WHERE\s+(.*?)\s+AND\s+p\.event_date\s*>=\s*(unix_timestamp\(current_timestamp\s*-\s*interval\s+\d+\s+day\)\s*\*\s*1000)"
    r"(?:\s+AND\s+(.*?))?\s+GROUP\s+BY",
    re.I,
)
A_WINDOW_RE = re.compile(r"INTERVAL\s+(\d+)\s+DAY", re.I)
A_GROUP_FIELD_RE = re.compile(r"p\.([a-z0-9_]+)\s*=\s*%s", re.I)


@dataclass(frozen=True)
class GroupATemplateSpec:
    template_id: str
    sql: str
    param_names: tuple[str, ...]
    select_expr: str
    base_filter: str
    window_days: int
    extra_predicate: str | None
    group_by_fields: tuple[str, ...]


@dataclass(frozen=True)
class GroupABundleSpec:
    bundle_id: str
    base_filter: str
    window_days: int
    group_by_fields: tuple[str, ...]
    param_names: tuple[str, ...]
    templates: tuple[GroupATemplateSpec, ...]

    def render_sql(self, reference_time: datetime, hinted: bool = False) -> str:
        base_bundle_id = self.bundle_id.split("_split", 1)[0]
        if not hinted and base_bundle_id in GROUP_A_DIMENSION_ROLLUP_BUNDLES:
            return render_group_a_dimension_rollup_sql(self, reference_time)
        select_parts: list[str] = []
        for tmpl in self.templates:
            select_parts.append(f"{build_group_a_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
            if tmpl.extra_predicate:
                select_parts.append(f"{build_presence_expr(tmpl.extra_predicate)} AS `{presence_column(tmpl.template_id)}`")
        cutoff_ms = int((reference_time.timestamp() - (self.window_days * 86400)) * 1000)
        reference_ms = int(reference_time.timestamp() * 1000)
        window_predicate = f"p.event_date >= {cutoff_ms}"
        if RUNTIME_WINDOW_UPPER_BOUND:
            window_predicate += f" AND p.event_date < {reference_ms}"
        select_prefix = "SELECT /*+ READ_FROM_STORAGE(TIFLASH[p]) */" if hinted else "SELECT"
        sql = (
            f"{select_prefix}\n  "
            + ",\n  ".join(select_parts)
            + "\nFROM pmt_txn_fact p\nWHERE "
            + f"{self.base_filter} AND {window_predicate}"
        )
        if has_redundant_group_by(self.bundle_id, self.base_filter, self.group_by_fields):
            return sql + "\nHAVING COUNT(*) > 0;"
        return sql + "\nGROUP BY " + ", ".join(self.group_by_fields) + ";"


def parse_group_a_template(template) -> GroupATemplateSpec:
    select_match = A_SELECT_RE.search(template.sql)
    where_match = A_WHERE_RE.search(template.sql)
    window_match = A_WINDOW_RE.search(template.sql)
    if not (select_match and where_match and window_match):
        raise ValueError(f"Could not parse Group A template {template.template_id}")
    group_fields = tuple(f"p.{name}" for name in A_GROUP_FIELD_RE.findall(where_match.group(1).strip()))
    if not group_fields:
        raise ValueError(f"No GROUP BY fields detected for Group A template {template.template_id}")
    return GroupATemplateSpec(
        template_id=template.template_id,
        sql=template.sql,
        param_names=template.param_names,
        select_expr=select_match.group(1).strip(),
        base_filter=where_match.group(1).strip(),
        window_days=int(window_match.group(1)),
        extra_predicate=where_match.group(3).strip() if where_match.group(3) else None,
        group_by_fields=group_fields,
    )


def build_group_a_metric_expr(tmpl: GroupATemplateSpec) -> str:
    expr = tmpl.select_expr
    cond = tmpl.extra_predicate
    if cond is None:
        return expr
    if expr == "COUNT(*)":
        return f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END)"
    if expr == "SUM(p.amount)":
        return f"SUM(CASE WHEN {cond} THEN p.amount END)"
    if expr == "MIN(p.amount)":
        return f"MIN(CASE WHEN {cond} THEN p.amount END)"
    if expr == "MAX(p.amount)":
        return f"MAX(CASE WHEN {cond} THEN p.amount END)"
    distinct_match = re.fullmatch(r"COUNT\(DISTINCT\((.*?)\)\)", expr, re.I)
    if distinct_match:
        return f"COUNT(DISTINCT CASE WHEN {cond} THEN {distinct_match.group(1).strip()} END)"
    raise ValueError(f"Unsupported Group A aggregate expression: {expr}")


def group_a_rollup_columns(bundle: GroupABundleSpec) -> tuple[str, ...]:
    columns: set[str] = set()
    for tmpl in bundle.templates:
        if tmpl.extra_predicate:
            columns.update(re.findall(r"\bp\.([a-z0-9_]+)\s*=", tmpl.extra_predicate, flags=re.I))
        distinct_match = re.fullmatch(r"COUNT\(DISTINCT\(p\.([a-z0-9_]+)\)\)", tmpl.select_expr, flags=re.I)
        if distinct_match:
            columns.add(distinct_match.group(1))
    preferred = ("mt_gateway", "transaction_type", "card_type", "entry_method")
    ordered = [column for column in preferred if column in columns]
    ordered.extend(sorted(columns - set(ordered)))
    return tuple(ordered)


def group_a_rollup_metric_expr(tmpl: GroupATemplateSpec, rollup_alias: str = "b") -> str:
    expr = tmpl.select_expr
    cond = tmpl.extra_predicate
    if cond is None:
        if expr == "COUNT(*)":
            return f"SUM({rollup_alias}.row_count)"
        if expr == "SUM(p.amount)":
            return f"SUM({rollup_alias}.amount_sum)"
        if expr == "MIN(p.amount)":
            return f"MIN({rollup_alias}.amount_min)"
        if expr == "MAX(p.amount)":
            return f"MAX({rollup_alias}.amount_max)"
        distinct_match = re.fullmatch(r"COUNT\(DISTINCT\(p\.([a-z0-9_]+)\)\)", expr, re.I)
        if distinct_match:
            return f"COUNT(DISTINCT {rollup_alias}.{distinct_match.group(1)})"
    else:
        outer_cond = re.sub(r"\bp\.", f"{rollup_alias}.", cond)
        if expr == "COUNT(*)":
            return f"SUM(CASE WHEN {outer_cond} THEN {rollup_alias}.row_count ELSE 0 END)"
        if expr == "SUM(p.amount)":
            return f"SUM(CASE WHEN {outer_cond} THEN {rollup_alias}.amount_sum END)"
        if expr == "MIN(p.amount)":
            return f"MIN(CASE WHEN {outer_cond} THEN {rollup_alias}.amount_min END)"
        if expr == "MAX(p.amount)":
            return f"MAX(CASE WHEN {outer_cond} THEN {rollup_alias}.amount_max END)"
    raise ValueError(f"Unsupported Group A rollup expression: {expr}")


def group_a_rollup_presence_expr(tmpl: GroupATemplateSpec, rollup_alias: str = "b") -> str:
    if not tmpl.extra_predicate:
        raise ValueError(f"{tmpl.template_id} has no presence predicate")
    outer_cond = re.sub(r"\bp\.", f"{rollup_alias}.", tmpl.extra_predicate)
    return f"SUM(CASE WHEN {outer_cond} THEN {rollup_alias}.row_count ELSE 0 END)"


def render_group_a_dimension_rollup_sql(bundle: GroupABundleSpec, reference_time: datetime) -> str:
    rollup_columns = group_a_rollup_columns(bundle)
    if not rollup_columns:
        raise ValueError(f"No dimension rollup columns detected for {bundle.bundle_id}")
    cutoff_ms = int((reference_time.timestamp() - (bundle.window_days * 86400)) * 1000)
    reference_ms = int(reference_time.timestamp() * 1000)
    window_predicate = f"p.event_date >= {cutoff_ms}"
    if RUNTIME_WINDOW_UPPER_BOUND:
        window_predicate += f" AND p.event_date < {reference_ms}"
    select_parts: list[str] = []
    for tmpl in bundle.templates:
        select_parts.append(f"{group_a_rollup_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        if tmpl.extra_predicate:
            select_parts.append(f"{group_a_rollup_presence_expr(tmpl)} AS `{presence_column(tmpl.template_id)}`")
    dimension_select = ", ".join(f"p.{column}" for column in rollup_columns)
    dimension_group_by = ", ".join(f"p.{column}" for column in rollup_columns)
    return (
        "SELECT\n  "
        + ",\n  ".join(select_parts)
        + "\nFROM (\n"
        + f"  SELECT {dimension_select}, COUNT(*) AS row_count, SUM(p.amount) AS amount_sum, "
        + "MIN(p.amount) AS amount_min, MAX(p.amount) AS amount_max\n"
        + "  FROM pmt_txn_fact p\nWHERE "
        + f"{bundle.base_filter} AND {window_predicate}\n"
        + f"  GROUP BY {dimension_group_by}\n"
        + ") b\n"
        + "HAVING SUM(b.row_count) > 0;"
    )


def cluster_group_a_templates() -> list[GroupABundleSpec]:
    templates = [parse_group_a_template(t) for t in load_query_templates() if t.group == "A"]
    grouped: dict[tuple[str, int], list[GroupATemplateSpec]] = defaultdict(list)
    for tmpl in templates:
        grouped[(tmpl.base_filter, tmpl.window_days)].append(tmpl)
    bundles: list[GroupABundleSpec] = []
    for index, ((base_filter, window_days), members) in enumerate(
        sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])),
        start=1,
    ):
        first = members[0]
        bundles.append(
            GroupABundleSpec(
                bundle_id=f"group_a_bundle_{index:03d}",
                base_filter=base_filter,
                window_days=window_days,
                group_by_fields=first.group_by_fields,
                param_names=first.param_names,
                templates=tuple(sorted(members, key=lambda x: x.template_id)),
            )
        )
    return bundles


def parse_split_spec(raw_specs: list[str] | None) -> dict[str, int]:
    """Parse split specs like group_a_bundle_006:3."""
    parsed: dict[str, int] = {}
    for raw in raw_specs or []:
        if ":" not in raw:
            raise ValueError(f"Invalid split spec {raw!r}. Use bundle_id:parts, e.g. group_a_bundle_006:3")
        bundle_id, parts_raw = raw.split(":", 1)
        parts = int(parts_raw)
        if parts < 2:
            raise ValueError(f"Split parts must be >= 2 for {raw!r}")
        parsed[bundle_id] = parts
    return parsed


def split_group_a_bundles(bundles: list[GroupABundleSpec], split_specs: dict[str, int]) -> list[GroupABundleSpec]:
    """Split selected Group A bundles into smaller template subsets.

    This is an experimental tuning lever. It preserves correctness because each
    original template ID still appears in exactly one output bundle.
    """
    if not split_specs:
        return bundles

    output: list[GroupABundleSpec] = []
    found: set[str] = set()
    for bundle in bundles:
        parts = split_specs.get(bundle.bundle_id)
        if not parts:
            output.append(bundle)
            continue

        found.add(bundle.bundle_id)
        members = list(bundle.templates)
        chunk_size = (len(members) + parts - 1) // parts
        for idx in range(parts):
            chunk = tuple(members[idx * chunk_size : (idx + 1) * chunk_size])
            if not chunk:
                continue
            output.append(
                GroupABundleSpec(
                    bundle_id=f"{bundle.bundle_id}_split{idx + 1}",
                    base_filter=bundle.base_filter,
                    window_days=bundle.window_days,
                    group_by_fields=bundle.group_by_fields,
                    param_names=bundle.param_names,
                    templates=chunk,
                )
            )

    missing = sorted(set(split_specs) - found)
    if missing:
        raise ValueError(f"Cannot split unknown Group A bundle(s): {', '.join(missing)}")
    return output


def run_group_a_bundle(
    pool: ConnectionPool,
    bundle: GroupABundleSpec,
    bindings: dict[str, Any],
    reference_time: datetime,
    hinted_group_a_bundles: set[str] | None = None,
) -> dict[str, Any]:
    params = tuple(bindings[name] for name in bundle.param_names)
    base_bundle_id = bundle.bundle_id.split("_split", 1)[0]
    hint_set = hinted_group_a_bundles or DEFAULT_HINTED_GROUP_A_BUNDLES
    hint_applied = base_bundle_id in hint_set
    sql = bundle.render_sql(reference_time, hinted=hint_applied)
    conn = pool.connection()
    started = time.perf_counter()
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] if cursor.description else []
    return {
        "bundle_id": bundle.bundle_id,
        "group": "A",
        "window_days": bundle.window_days,
        "base_filter": bundle.base_filter,
        "template_ids": [t.template_id for t in bundle.templates],
        "param_names": list(bundle.param_names),
        "params": [_json_safe(v) for v in params],
        "sql": sql,
        "hint_applied": hint_applied,
        "elapsed_ms": (time.perf_counter() - started) * 1000.0,
        "columns": columns,
        "rows": [[_json_safe(v) for v in row] for row in rows],
    }


# ---------------------------------------------------------------------------
# Group B bundling
# ---------------------------------------------------------------------------

B_SELECT_RE = re.compile(r"^SELECT\s+(.*?)\s+FROM\s+deviceprofile_fact", re.I)
B_WHERE_RE = re.compile(
    r"WHERE\s+(.*?)\s+AND\s+d\.jms_timestamp\s*>=\s*(NOW\(\)\s*-\s*INTERVAL\s+\d+\s+DAY)"
    r"(?:\s+AND\s+(.*?))?\s+GROUP\s+BY",
    re.I,
)
B_WINDOW_RE = re.compile(r"INTERVAL\s+(\d+)\s+DAY", re.I)
B_GROUP_FIELD_RE = re.compile(r"d\.([a-z0-9_]+)\s*=\s*%s", re.I)


@dataclass(frozen=True)
class GroupBTemplateSpec:
    template_id: str
    sql: str
    param_names: tuple[str, ...]
    select_expr: str
    base_filter: str
    window_days: int
    extra_predicate: str | None
    group_by_fields: tuple[str, ...]


@dataclass(frozen=True)
class GroupBBundleSpec:
    bundle_id: str
    base_filter: str
    window_days: int
    group_by_fields: tuple[str, ...]
    param_names: tuple[str, ...]
    templates: tuple[GroupBTemplateSpec, ...]

    def render_sql(self, reference_time: datetime) -> str:
        select_parts: list[str] = []
        for tmpl in self.templates:
            select_parts.append(f"{build_group_b_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
            if tmpl.extra_predicate:
                select_parts.append(f"{build_presence_expr(tmpl.extra_predicate)} AS `{presence_column(tmpl.template_id)}`")
        cutoff_literal = (reference_time - timedelta(days=self.window_days)).strftime("%Y-%m-%d %H:%M:%S.%f")
        reference_literal = reference_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        window_predicate = f"d.jms_timestamp >= '{cutoff_literal}'"
        if RUNTIME_WINDOW_UPPER_BOUND:
            window_predicate += f" AND d.jms_timestamp < '{reference_literal}'"
        sql = (
            "SELECT\n  "
            + ",\n  ".join(select_parts)
            + "\nFROM deviceprofile_fact d\nWHERE "
            + f"{self.base_filter} AND {window_predicate}"
        )
        if has_redundant_group_by(self.bundle_id, self.base_filter, self.group_by_fields):
            return sql + "\nHAVING COUNT(*) > 0;"
        return sql + "\nGROUP BY " + ", ".join(self.group_by_fields) + ";"


def parse_group_b_template(template) -> GroupBTemplateSpec:
    select_match = B_SELECT_RE.search(template.sql)
    where_match = B_WHERE_RE.search(template.sql)
    window_match = B_WINDOW_RE.search(template.sql)
    if not (select_match and where_match and window_match):
        raise ValueError(f"Could not parse Group B template {template.template_id}")
    group_fields = tuple(f"d.{name}" for name in B_GROUP_FIELD_RE.findall(where_match.group(1).strip()))
    if not group_fields:
        raise ValueError(f"No GROUP BY fields detected for Group B template {template.template_id}")
    return GroupBTemplateSpec(
        template_id=template.template_id,
        sql=template.sql,
        param_names=template.param_names,
        select_expr=select_match.group(1).strip(),
        base_filter=where_match.group(1).strip(),
        window_days=int(window_match.group(1)),
        extra_predicate=where_match.group(3).strip() if where_match.group(3) else None,
        group_by_fields=group_fields,
    )


def build_group_b_metric_expr(tmpl: GroupBTemplateSpec) -> str:
    expr = tmpl.select_expr
    cond = tmpl.extra_predicate
    if cond is None:
        return expr
    if expr == "COUNT(*)":
        return f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END)"
    distinct_match = re.fullmatch(r"COUNT\(DISTINCT\((.*?)\)\)", expr, re.I)
    if distinct_match:
        return f"COUNT(DISTINCT CASE WHEN {cond} THEN {distinct_match.group(1).strip()} END)"
    cast_agg_match = re.fullmatch(r"(MIN|MAX|AVG)\((CAST\(.*?\))\)", expr, re.I)
    if cast_agg_match:
        return f"{cast_agg_match.group(1).upper()}(CASE WHEN {cond} THEN {cast_agg_match.group(2)} END)"
    raise ValueError(f"Unsupported Group B aggregate expression: {expr}")


def cluster_group_b_templates() -> list[GroupBBundleSpec]:
    templates = [parse_group_b_template(t) for t in load_query_templates() if t.group == "B"]
    grouped: dict[tuple[str, int], list[GroupBTemplateSpec]] = defaultdict(list)
    for tmpl in templates:
        grouped[(tmpl.base_filter, tmpl.window_days)].append(tmpl)
    bundles: list[GroupBBundleSpec] = []
    for index, ((base_filter, window_days), members) in enumerate(
        sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])),
        start=1,
    ):
        first = members[0]
        bundles.append(
            GroupBBundleSpec(
                bundle_id=f"group_b_bundle_{index:03d}",
                base_filter=base_filter,
                window_days=window_days,
                group_by_fields=first.group_by_fields,
                param_names=first.param_names,
                templates=tuple(sorted(members, key=lambda x: x.template_id)),
            )
        )
    return bundles


def run_group_b_bundle(pool: ConnectionPool, bundle: GroupBBundleSpec, bindings: dict[str, Any], reference_time: datetime) -> dict[str, Any]:
    params = tuple(bindings[name] for name in bundle.param_names)
    sql = bundle.render_sql(reference_time)
    conn = pool.connection()
    started = time.perf_counter()
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] if cursor.description else []
    return {
        "bundle_id": bundle.bundle_id,
        "group": "B",
        "window_days": bundle.window_days,
        "base_filter": bundle.base_filter,
        "template_ids": [t.template_id for t in bundle.templates],
        "param_names": list(bundle.param_names),
        "params": [_json_safe(v) for v in params],
        "sql": sql,
        "hint_applied": None,
        "elapsed_ms": (time.perf_counter() - started) * 1000.0,
        "columns": columns,
        "rows": [[_json_safe(v) for v in row] for row in rows],
    }


# ---------------------------------------------------------------------------
# Group C bundling
# ---------------------------------------------------------------------------

C_SELECT_RE = re.compile(r"^SELECT\s+(.*?)\s+FROM\s+pmt_txn_fact", re.I | re.S)
C_WHERE_RE = re.compile(
    r"WHERE\s+(.*?)\s+AND\s+p\.event_date\s*>=\s*(unix_timestamp\(current_timestamp\s*-\s*interval\s+\d+\s+day\)\s*\*\s*1000)"
    r"(?:\s+AND\s+(.*?))?\s+GROUP\s+BY\s+(.*?);?\s*$",
    re.I | re.S,
)
C_WINDOW_RE = re.compile(r"INTERVAL\s+(\d+)\s+DAY", re.I)


@dataclass(frozen=True)
class GroupCTemplateSpec:
    template_id: str
    sql: str
    param_names: tuple[str, ...]
    select_expr: str
    base_filter: str
    window_days: int
    extra_predicate: str | None
    group_by_fields: tuple[str, ...]


@dataclass(frozen=True)
class GroupCBundleSpec:
    bundle_id: str
    family_label: str
    base_filter: str
    window_days: int
    group_by_fields: tuple[str, ...]
    param_names: tuple[str, ...]
    templates: tuple[GroupCTemplateSpec, ...]

    def render_sql(self, reference_time: datetime, hinted: bool = False) -> str:
        if self.bundle_id in GROUP_C_DEVICE_FIRST_JOIN_BUNDLES:
            return render_group_c_device_first_join_sql(self, reference_time, hinted=hinted)
        cutoff_ms = int(reference_time.timestamp() * 1000) - (self.window_days * 86400 * 1000)
        reference_ms = int(reference_time.timestamp() * 1000)
        cutoff_dt = datetime.fromtimestamp(cutoff_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
        reference_dt = reference_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        p_window_predicate = f"p.event_date >= {cutoff_ms}"
        d_window_predicate = f"d.jms_timestamp >= '{cutoff_dt}'"
        if RUNTIME_WINDOW_UPPER_BOUND:
            p_window_predicate += f" AND p.event_date < {reference_ms}"
            d_window_predicate += f" AND d.jms_timestamp < '{reference_dt}'"
        select_parts: list[str] = []
        for tmpl in self.templates:
            select_parts.append(f"{build_group_c_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
            if tmpl.extra_predicate:
                select_parts.append(f"{build_presence_expr(tmpl.extra_predicate)} AS `{presence_column(tmpl.template_id)}`")
        select_prefix = "SELECT /*+ READ_FROM_STORAGE(TIFLASH[p,d]) */" if hinted else "SELECT"
        join_keyword = "JOIN" if self.bundle_id in GROUP_C_INNER_JOIN_BUNDLES else "LEFT OUTER JOIN"
        sql = (
            select_prefix
            + "\n  "
            + ",\n  ".join(select_parts)
            + f"\nFROM pmt_txn_fact p\n{join_keyword} deviceprofile_fact d"
            + "\n  ON p.parsed_interaction_id = d.interaction_id"
            + f"\nWHERE {self.base_filter} AND {p_window_predicate}"
            + f"\n  AND {d_window_predicate}"
        )
        if has_redundant_group_by(self.bundle_id, self.base_filter, self.group_by_fields):
            return sql + "\nHAVING COUNT(*) > 0;"
        return sql + "\nGROUP BY " + ", ".join(self.group_by_fields) + ";"


def parse_group_c_template(template) -> GroupCTemplateSpec:
    select_match = C_SELECT_RE.search(template.sql)
    where_match = C_WHERE_RE.search(template.sql)
    window_match = C_WINDOW_RE.search(template.sql)
    if not (select_match and where_match and window_match):
        raise ValueError(f"Could not parse Group C template {template.template_id}")
    return GroupCTemplateSpec(
        template_id=template.template_id,
        sql=template.sql,
        param_names=template.param_names,
        select_expr=select_match.group(1).strip(),
        base_filter=where_match.group(1).strip(),
        window_days=int(window_match.group(1)),
        extra_predicate=where_match.group(3).strip() if where_match.group(3) else None,
        group_by_fields=tuple(field.strip() for field in where_match.group(4).split(",")),
    )


def build_group_c_metric_expr(tmpl: GroupCTemplateSpec) -> str:
    expr = tmpl.select_expr
    cond = tmpl.extra_predicate
    if cond is None:
        return expr
    if expr == "COUNT(*)":
        return f"SUM(CASE WHEN {cond} THEN 1 ELSE 0 END)"
    if expr == "SUM(p.amount)":
        return f"SUM(CASE WHEN {cond} THEN p.amount END)"
    if expr == "MIN(p.amount)":
        return f"MIN(CASE WHEN {cond} THEN p.amount END)"
    if expr == "MAX(p.amount)":
        return f"MAX(CASE WHEN {cond} THEN p.amount END)"
    distinct_match = re.fullmatch(r"COUNT\(DISTINCT\((.*?)\)\)", expr, re.I)
    if distinct_match:
        return f"COUNT(DISTINCT CASE WHEN {cond} THEN {distinct_match.group(1).strip()} END)"
    cast_agg_match = re.fullmatch(r"(MIN|MAX)\((CAST\(.*?\))\)", expr, re.I)
    if cast_agg_match:
        return f"{cast_agg_match.group(1).upper()}(CASE WHEN {cond} THEN {cast_agg_match.group(2)} END)"
    raise ValueError(f"Unsupported Group C aggregate expression: {expr}")


def build_presence_expr(condition: str) -> str:
    return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"


def render_group_c_device_first_join_sql(bundle: GroupCBundleSpec, reference_time: datetime, hinted: bool = False) -> str:
    cutoff_ms = int(reference_time.timestamp() * 1000) - (bundle.window_days * 86400 * 1000)
    reference_ms = int(reference_time.timestamp() * 1000)
    cutoff_dt = datetime.fromtimestamp(cutoff_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
    reference_dt = reference_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    p_window_predicate = f"p.event_date >= {cutoff_ms}"
    d_window_predicate = f"d.jms_timestamp >= '{cutoff_dt}'"
    if RUNTIME_WINDOW_UPPER_BOUND:
        p_window_predicate += f" AND p.event_date < {reference_ms}"
        d_window_predicate += f" AND d.jms_timestamp < '{reference_dt}'"
    select_parts: list[str] = []
    for tmpl in bundle.templates:
        select_parts.append(f"{build_group_c_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        if tmpl.extra_predicate:
            select_parts.append(f"{build_presence_expr(tmpl.extra_predicate)} AS `{presence_column(tmpl.template_id)}`")
    select_prefix = "SELECT /*+ READ_FROM_STORAGE(TIFLASH[p,d]) */" if hinted else "SELECT"
    sql = (
        select_prefix
        + "\n  "
        + ",\n  ".join(select_parts)
        + "\nFROM deviceprofile_fact d\nJOIN pmt_txn_fact p"
        + "\n  ON p.parsed_interaction_id = d.interaction_id"
        + f"\nWHERE {bundle.base_filter} AND {d_window_predicate}"
        + f"\n  AND {p_window_predicate}"
    )
    if has_redundant_group_by(bundle.bundle_id, bundle.base_filter, bundle.group_by_fields):
        return sql + "\nHAVING COUNT(*) > 0;"
    return sql + "\nGROUP BY " + ", ".join(bundle.group_by_fields) + ";"


def cluster_group_c_templates() -> list[GroupCBundleSpec]:
    templates = [parse_group_c_template(t) for t in load_query_templates() if t.group == "C"]
    grouped: dict[tuple[str, int], list[GroupCTemplateSpec]] = defaultdict(list)
    for tmpl in templates:
        grouped[(tmpl.base_filter, tmpl.window_days)].append(tmpl)
    bundles: list[GroupCBundleSpec] = []
    for index, ((base_filter, window_days), members) in enumerate(
        sorted(grouped.items(), key=lambda item: (item[0][1], item[0][0])),
        start=1,
    ):
        first = members[0]
        filter_label = re.sub(r"[^a-z0-9]+", "_", base_filter.lower()).strip("_")[:48]
        bundles.append(
            GroupCBundleSpec(
                bundle_id=f"group_c_bundle_{index:03d}",
                family_label=f"{filter_label}_{window_days}d",
                base_filter=base_filter,
                window_days=window_days,
                group_by_fields=first.group_by_fields,
                param_names=first.param_names,
                templates=tuple(sorted(members, key=lambda x: x.template_id)),
            )
        )
    return bundles


def run_group_c_bundle(
    pool: ConnectionPool,
    bundle: GroupCBundleSpec,
    bindings: dict[str, Any],
    reference_time: datetime,
    hinted_group_c_bundles: set[str] | None = None,
) -> dict[str, Any]:
    params = tuple(bindings[name] for name in bundle.param_names)
    hint_applied = bundle.bundle_id in (hinted_group_c_bundles or set())
    sql = bundle.render_sql(reference_time, hinted=hint_applied)
    conn = pool.connection()
    started = time.perf_counter()
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] if cursor.description else []
    return {
        "bundle_id": bundle.bundle_id,
        "group": "C",
        "family_label": bundle.family_label,
        "window_days": bundle.window_days,
        "base_filter": bundle.base_filter,
        "template_ids": [t.template_id for t in bundle.templates],
        "param_names": list(bundle.param_names),
        "params": [_json_safe(v) for v in params],
        "sql": sql,
        "hint_applied": hint_applied,
        "elapsed_ms": (time.perf_counter() - started) * 1000.0,
        "columns": columns,
        "rows": [[_json_safe(v) for v in row] for row in rows],
    }


# ---------------------------------------------------------------------------
# Diff and optimized runner
# ---------------------------------------------------------------------------


def extract_original_result(bundle_result: dict[str, Any], template_id: str, has_extra_predicate: bool) -> list[list[Any]]:
    rows = bundle_result["rows"]
    if not rows:
        return []
    columns = bundle_result["columns"]
    row = rows[0]
    metric_value = row[columns.index(metric_column(template_id))]
    if has_extra_predicate:
        present_value = row[columns.index(presence_column(template_id))]
        if present_value in (0, "0", 0.0, "0.0", None):
            return []
    return [[metric_value]]


def compare_against_baseline(
    baseline: dict[str, Any],
    bundle_results: list[dict[str, Any]],
    bundles: list[Any],
    group: str,
) -> list[dict[str, Any]]:
    baseline_by_template = {
        result["template_id"]: result
        for result in baseline["results"]
        if result["group"] == group
    }
    bundle_map = {result["bundle_id"]: result for result in bundle_results}
    mismatches: list[dict[str, Any]] = []
    for bundle in bundles:
        result = bundle_map[bundle.bundle_id]
        for tmpl in bundle.templates:
            baseline_rows = baseline_by_template[tmpl.template_id]["rows"]
            optimized_rows = extract_original_result(result, tmpl.template_id, bool(tmpl.extra_predicate))
            if normalize_rows(baseline_rows) != normalize_rows(optimized_rows):
                mismatches.append(
                    {
                        "template_id": tmpl.template_id,
                        "bundle_id": bundle.bundle_id,
                        "baseline_rows": baseline_rows,
                        "optimized_rows": optimized_rows,
                    }
                )
    return mismatches


def summarize_group_c_coverage(bundle_results: list[dict[str, Any]]) -> dict[str, Any]:
    nonempty = []
    empty = []
    for result in bundle_results:
        entry = {
            "bundle_id": result["bundle_id"],
            "family_label": result["family_label"],
            "window_days": result["window_days"],
            "query_count": len(result["template_ids"]),
            "rows_returned": len(result["rows"]),
            "template_ids": result["template_ids"],
        }
        (nonempty if result["rows"] else empty).append(entry)
    return {
        "nonempty_bundle_count": len(nonempty),
        "empty_bundle_count": len(empty),
        "nonempty_bundles": nonempty,
        "empty_bundles": empty,
    }


def run_optimized_benchmark(
    baseline_path: Path,
    output_path: Path | None,
    max_workers: int,
    split_group_a: dict[str, int] | None = None,
    extra_group_a_hints: set[str] | None = None,
    extra_group_c_hints: set[str] | None = None,
) -> int:
    baseline = load_baseline(baseline_path)
    bindings = baseline["event"]["bindings"]
    reference_time = datetime.fromisoformat(baseline["event"].get("reference_time", baseline["run_at"]))

    db_config = get_db_config(save_msg="optimized benchmark")
    pool = ConnectionPool(db_config)
    group_a_bundles = split_group_a_bundles(cluster_group_a_templates(), split_group_a or {})
    group_b_bundles = cluster_group_b_templates()
    group_c_bundles = cluster_group_c_templates()
    hinted_group_a_bundles = set(DEFAULT_HINTED_GROUP_A_BUNDLES)
    hinted_group_a_bundles.update(extra_group_a_hints or set())
    hinted_group_c_bundles = set(extra_group_c_hints or set())

    group_a_results: list[dict[str, Any]] = []
    group_b_results: list[dict[str, Any]] = []
    group_c_results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    started = time.perf_counter()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map: dict[Any, tuple[str, Any]] = {}
            for bundle in group_a_bundles:
                future_map[
                    executor.submit(run_group_a_bundle, pool, bundle, bindings, reference_time, hinted_group_a_bundles)
                ] = ("A", bundle)
            for bundle in group_b_bundles:
                future_map[executor.submit(run_group_b_bundle, pool, bundle, bindings, reference_time)] = ("B", bundle)
            for bundle in group_c_bundles:
                future_map[
                    executor.submit(run_group_c_bundle, pool, bundle, bindings, reference_time, hinted_group_c_bundles)
                ] = ("C", bundle)
            for future in as_completed(future_map):
                group, bundle = future_map[future]
                try:
                    result = future.result()
                    if group == "A":
                        group_a_results.append(result)
                    elif group == "B":
                        group_b_results.append(result)
                    else:
                        group_c_results.append(result)
                except Exception as exc:
                    errors.append({"group": group, "bundle_id": bundle.bundle_id, "error": str(exc)})
    finally:
        pool.close()

    event_wall_clock_ms = (time.perf_counter() - started) * 1000.0
    group_a_results.sort(key=lambda item: item["bundle_id"])
    group_b_results.sort(key=lambda item: item["bundle_id"])
    group_c_results.sort(key=lambda item: item["bundle_id"])
    errors.sort(key=lambda item: (item["group"], item["bundle_id"]))
    timings = [result["elapsed_ms"] for result in (group_a_results + group_b_results + group_c_results)]

    mismatches: list[dict[str, Any]] = []
    if not errors:
        mismatches.extend(compare_against_baseline(baseline, group_a_results, group_a_bundles, "A"))
        mismatches.extend(compare_against_baseline(baseline, group_b_results, group_b_bundles, "B"))
        mismatches.extend(compare_against_baseline(baseline, group_c_results, group_c_bundles, "C"))

    baseline_summary = baseline["summary"]
    coverage = summarize_group_c_coverage(group_c_results)
    bundled_count = len(group_a_results) + len(group_b_results) + len(group_c_results)
    output_payload = {
        "run_at": datetime.now().isoformat(),
        "baseline_path": str(baseline_path),
        "event": baseline["event"],
        "summary": {
            "bundled_query_count": bundled_count,
            "event_wall_clock_ms": event_wall_clock_ms,
            "per_bundle_p50_ms": statistics.median(timings) if timings else 0.0,
            "per_bundle_p95_ms": safe_percentile(timings, 95),
            "per_bundle_p99_ms": safe_percentile(timings, 99),
            "per_bundle_max_ms": max(timings) if timings else 0.0,
            "critical_path_bundle_ms": max(timings) if timings else 0.0,
            "error_count": len(errors),
            "mismatch_count": len(mismatches),
            "max_workers": max_workers,
            "group_a_split_specs": split_group_a or {},
            "group_a_hint_bundles": sorted(hinted_group_a_bundles),
            "group_c_hint_bundles": sorted(hinted_group_c_bundles),
        },
        "deltas_vs_baseline": {
            "event_wall_clock_ms": {
                "before": baseline_summary["event_elapsed_ms"],
                "after": event_wall_clock_ms,
                "reduction_pct": (
                    ((baseline_summary["event_elapsed_ms"] - event_wall_clock_ms) / baseline_summary["event_elapsed_ms"]) * 100.0
                    if baseline_summary["event_elapsed_ms"]
                    else 0.0
                ),
            },
            "p95_ms": {
                "before": baseline_summary["per_query_ms"]["p95"],
                "after": safe_percentile(timings, 95),
            },
            "p99_ms": {
                "before": baseline_summary["per_query_ms"]["p99"],
                "after": safe_percentile(timings, 99),
            },
            "max_ms": {
                "before": baseline_summary["per_query_ms"]["max"],
                "after": max(timings) if timings else 0.0,
            },
            "query_count": {"before": baseline_summary["completed"], "after": bundled_count},
        },
        "group_counts": {
            "group_a_bundles": len(group_a_results),
            "group_b_bundles": len(group_b_results),
            "group_c_bundles": len(group_c_results),
        },
        "group_c_coverage": coverage,
        "results": {"group_a": group_a_results, "group_b": group_b_results, "group_c": group_c_results},
        "errors": errors,
        "mismatches": mismatches[:200],
    }

    if output_path is None:
        output_path = DEFAULT_OUTPUT_DIR / f"optimized_hinted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print("Optimized demo")
    print(f"  Bundles:  {bundled_count}")
    print(f"  Event ms: {event_wall_clock_ms:.2f}")
    print(f"  P95:      {safe_percentile(timings, 95):.2f}")
    print(f"  P99:      {safe_percentile(timings, 99):.2f}")
    print(f"  Errors:   {len(errors)}")
    print(f"  Diffs:    {len(mismatches)}")
    print(f"  Group C coverage: {coverage['nonempty_bundle_count']} non-empty / {coverage['empty_bundle_count']} empty")
    print(f"  Saved:    {output_path}")
    return 0 if not errors and not mismatches else 1


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


def run_schema(_: argparse.Namespace) -> int:
    return run_command([sys.executable, "setup_schema.py"])


def run_load(args: argparse.Namespace) -> int:
    env = dict(os.environ)
    if args.scale:
        env["INTUIT_DEMO_SCALE"] = args.scale
        print(f"+ INTUIT_DEMO_SCALE={args.scale} {sys.executable} -u load_data.py", flush=True)
    return subprocess.call([sys.executable, "-u", "load_data.py"], env=env)


def run_baseline(args: argparse.Namespace) -> int:
    command = [sys.executable, "lib/event_benchmark.py", "--max-workers", str(args.max_workers)]
    if args.output:
        command.extend(["--output", args.output])
    if args.invoice_number:
        command.extend(["--invoice-number", args.invoice_number])
    return run_command(command)


def run_optimized(args: argparse.Namespace) -> int:
    return run_optimized_benchmark(
        baseline_path=Path(args.baseline),
        output_path=Path(args.output) if args.output else None,
        max_workers=args.max_workers,
        split_group_a=parse_split_spec(args.split_group_a),
        extra_group_a_hints=set(args.hint_group_a or []),
        extra_group_c_hints=set(args.hint_group_c or []),
    )


def run_full(args: argparse.Namespace) -> int:
    rc = run_command(
        [
            sys.executable,
            "lib/event_benchmark.py",
            "--max-workers",
            str(args.max_workers),
            "--output",
            args.baseline_output,
        ]
    )
    if rc != 0:
        return rc
    return run_optimized_benchmark(
        baseline_path=Path(args.baseline_output),
        output_path=Path(args.optimized_output),
        max_workers=args.max_workers,
        split_group_a=parse_split_spec(args.split_group_a),
        extra_group_a_hints=set(args.hint_group_a or []),
        extra_group_c_hints=set(args.hint_group_c or []),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Single entrypoint for the Intuit TiDB demo.")
    sub = parser.add_subparsers(dest="command", required=True)

    schema = sub.add_parser("schema", help="Create database schema.")
    schema.set_defaults(func=run_schema)

    load = sub.add_parser("load", help="Load synthetic data.")
    load.add_argument("--scale", choices=["tiny", "small", "full"], default=None, help="Data scale. Defaults to load_data.py default.")
    load.set_defaults(func=run_load)

    baseline = sub.add_parser("baseline", help="Run the 2,738-query naive baseline.")
    baseline.add_argument("--max-workers", type=int, default=65)
    baseline.add_argument("--output", default="benchmark_results/naive_baseline_new.json")
    baseline.add_argument("--invoice-number", default=None)
    baseline.set_defaults(func=run_baseline)

    optimized = sub.add_parser("optimized", help="Run the final 65-query optimized/hinted demo.")
    optimized.add_argument("--baseline", required=True, help="Path to naive baseline JSON.")
    optimized.add_argument("--max-workers", type=int, default=65)
    optimized.add_argument("--output", default=None)
    optimized.add_argument(
        "--split-group-a",
        action="append",
        default=[],
        help="Experimentally split a Group A bundle, e.g. --split-group-a group_a_bundle_006:3",
    )
    optimized.add_argument(
        "--hint-group-a",
        action="append",
        default=[],
        help="Experimentally add a TiFlash hint for a Group A bundle, e.g. --hint-group-a group_a_bundle_010",
    )
    optimized.add_argument(
        "--hint-group-c",
        action="append",
        default=[],
        help="Experimentally add a TiFlash hint for a Group C bundle, e.g. --hint-group-c group_c_bundle_018",
    )
    optimized.set_defaults(func=run_optimized)

    full = sub.add_parser("full", help="Run baseline and optimized benchmark back-to-back.")
    full.add_argument("--max-workers", type=int, default=65)
    full.add_argument("--baseline-output", default="benchmark_results/naive_baseline_new.json")
    full.add_argument("--optimized-output", default="benchmark_results/optimized_hinted_new.json")
    full.add_argument(
        "--split-group-a",
        action="append",
        default=[],
        help="Experimentally split a Group A bundle during optimized run, e.g. group_a_bundle_006:3",
    )
    full.add_argument(
        "--hint-group-a",
        action="append",
        default=[],
        help="Experimentally add a TiFlash hint for a Group A bundle during optimized run.",
    )
    full.add_argument(
        "--hint-group-c",
        action="append",
        default=[],
        help="Experimentally add a TiFlash hint for a Group C bundle during optimized run.",
    )
    full.set_defaults(func=run_full)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    DEFAULT_OUTPUT_DIR.mkdir(exist_ok=True)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
