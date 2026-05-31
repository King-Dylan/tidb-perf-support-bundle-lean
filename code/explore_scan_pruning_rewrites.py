#!/usr/bin/env python3
"""Try scan/CASE-pruning rewrites one bundle at a time.

The script intentionally does not change benchmark behavior. It renders the
current optimized SQL plus candidate rewrites, verifies the candidate returns
the same one-row result for the representative slow event, then records full
EXPLAIN ANALYZE evidence.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql

from demo import (
    GroupABundleSpec,
    GroupBBundleSpec,
    build_group_b_metric_expr,
    build_presence_expr,
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
    metric_column,
    presence_column,
)
from explain_problem_bundles import PREAGG_BUNDLES, choose_record, summarize_bundle, tabular
from final_compare_optimization_report import OPTIMIZED_VARIANTS, apply_session, load_records
from lib.db_config import get_db_config
from mixed_traffic_test import bundle_params, render_bundle_sql


ROOT = Path(__file__).resolve().parent


GROUP_A_ROLLUP_BUNDLES = {
    "group_a_bundle_001",
    "group_a_bundle_002",
    "group_a_bundle_003",
    "group_a_bundle_004",
    "group_a_bundle_005",
    "group_a_bundle_006",
    "group_a_bundle_007",
    "group_a_bundle_008",
    "group_a_bundle_009",
    "group_a_bundle_010",
    "group_a_bundle_011",
    "group_a_bundle_012",
    "group_a_bundle_014",
    "group_a_bundle_016",
}


NUMERIC_SCORE_COLUMNS = (
    "device_score",
    "device_fingerprint_score",
    "device_worst_score",
    "true_ip_score",
    "input_ip_score",
)


def resolve_path(raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else ROOT / path


def normalize_value(value: Any) -> Any:
    if isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?(?:E[+-]?\d+)?", value, flags=re.I):
        return str(Decimal(value).normalize())
    if isinstance(value, Decimal):
        return str(value.normalize())
    if isinstance(value, (int, float)):
        return str(Decimal(str(value)).normalize())
    return value


def normalize_rows(rows: tuple[tuple[Any, ...], ...]) -> list[list[Any]]:
    return [[normalize_value(cell) for cell in row] for row in rows]


def extract_plan_metrics(plan_text: str) -> dict[str, Any]:
    keys = [int(value) for value in re.findall(r"total_process_keys: (\d+)", plan_text)]
    indexes = []
    for match in re.finditer(r"table:([^,\t\s]+), index:([^\t\n]+)", plan_text):
        item = {"table": match.group(1), "index": match.group(2).strip()}
        if item not in indexes:
            indexes.append(item)
    return {
        "total_process_keys_sum": sum(keys),
        "total_process_keys_max": max(keys) if keys else 0,
        "total_process_keys": keys,
        "indexes": indexes,
    }


def explain_analyze(cur, sql: str, params: tuple[Any, ...], settings: dict[str, Any], max_execution_time: int) -> dict[str, Any]:
    apply_session(cur, settings, max_execution_time)
    started = time.perf_counter()
    try:
        cur.execute("EXPLAIN ANALYZE " + sql.rstrip().rstrip(";"), params)
        rows = cur.fetchall()
        columns = tuple(desc[0] for desc in cur.description)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        plan = tabular(rows, columns)
        return {
            "ok": True,
            "elapsed_ms": elapsed_ms,
            "plan": plan,
            **extract_plan_metrics(plan),
        }
    except Exception as exc:
        return {
            "ok": False,
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
            "error": repr(exc),
            "plan": "",
            "total_process_keys_sum": 0,
            "total_process_keys_max": 0,
            "total_process_keys": [],
            "indexes": [],
        }


def query_rows(cur, sql: str, params: tuple[Any, ...], max_execution_time: int) -> dict[str, Any]:
    apply_session(cur, {}, max_execution_time)
    started = time.perf_counter()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = tuple(desc[0] for desc in cur.description)
        return {
            "ok": True,
            "elapsed_ms": (time.perf_counter() - started) * 1000.0,
            "columns": columns,
            "rows": normalize_rows(rows),
        }
    except Exception as exc:
        return {"ok": False, "elapsed_ms": (time.perf_counter() - started) * 1000.0, "error": repr(exc)}


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


def group_a_rollup_metric_expr(tmpl, rollup_alias: str = "b") -> str:
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
        distinct_match = re.fullmatch(r"COUNT\(DISTINCT\(p\.([a-z0-9_]+)\)\)", expr, flags=re.I)
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
    raise ValueError(f"Unsupported Group A rollup expression for {tmpl.template_id}: {expr} / {cond}")


def group_a_rollup_presence_expr(tmpl, rollup_alias: str = "b") -> str:
    if not tmpl.extra_predicate:
        raise ValueError(f"{tmpl.template_id} has no presence predicate")
    outer_cond = re.sub(r"\bp\.", f"{rollup_alias}.", tmpl.extra_predicate)
    return f"SUM(CASE WHEN {outer_cond} THEN {rollup_alias}.row_count ELSE 0 END)"


def render_group_a_dimension_rollup_sql(bundle: GroupABundleSpec, reference_time: datetime) -> str:
    columns = group_a_rollup_columns(bundle)
    if not columns:
        raise ValueError(f"{bundle.bundle_id} has no rollup columns")
    cutoff_ms = int((reference_time.timestamp() - (bundle.window_days * 86400)) * 1000)
    select_parts: list[str] = []
    for tmpl in bundle.templates:
        select_parts.append(f"{group_a_rollup_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        if tmpl.extra_predicate:
            select_parts.append(f"{group_a_rollup_presence_expr(tmpl)} AS `{presence_column(tmpl.template_id)}`")
    inner_columns = ", ".join(f"p.{column}" for column in columns)
    group_columns = ", ".join(f"p.{column}" for column in columns)
    return (
        "SELECT\n  "
        + ",\n  ".join(select_parts)
        + "\nFROM (\n"
        + f"  SELECT {inner_columns}, COUNT(*) AS row_count, SUM(p.amount) AS amount_sum, "
        + "MIN(p.amount) AS amount_min, MAX(p.amount) AS amount_max\n"
        + "  FROM pmt_txn_fact p\n"
        + f"  WHERE {bundle.base_filter} AND p.event_date >= {cutoff_ms}\n"
        + f"  GROUP BY {group_columns}\n"
        + ") b\n"
        + "HAVING SUM(b.row_count) > 0;"
    )


def group_b_projected_metric_expr(tmpl) -> str:
    expr = tmpl.select_expr
    cond = tmpl.extra_predicate
    for column in NUMERIC_SCORE_COLUMNS:
        cast_expr = f"CAST(d.{column} AS DECIMAL(10,2))"
        if cast_expr in expr:
            projected = f"d.{column}__num"
            if expr.startswith("MIN("):
                return f"MIN({projected})"
            if expr.startswith("MAX("):
                return f"MAX({projected})"
            if expr.startswith("AVG("):
                return f"AVG({projected})"
        if cond == f"d.{column} IS NOT NULL AND d.{column} != ''":
            return f"SUM(CASE WHEN d.{column}__num IS NOT NULL THEN 1 ELSE 0 END)"
    return build_group_b_metric_expr(tmpl)


def group_b_projected_presence_expr(tmpl) -> str:
    cond = tmpl.extra_predicate
    if not cond:
        raise ValueError(f"{tmpl.template_id} has no presence predicate")
    for column in NUMERIC_SCORE_COLUMNS:
        if cond == f"d.{column} IS NOT NULL AND d.{column} != ''":
            return f"SUM(CASE WHEN d.{column}__num IS NOT NULL THEN 1 ELSE 0 END)"
    return build_presence_expr(cond)


def render_group_b_numeric_projection_sql(bundle: GroupBBundleSpec, reference_time: datetime) -> str:
    cutoff_literal = (reference_time.replace(tzinfo=None) if reference_time.tzinfo else reference_time)
    cutoff_literal = cutoff_literal.strftime("%Y-%m-%d %H:%M:%S.%f")
    # group_b reference_time is the event timestamp; window is encoded by the
    # bundle, so compute the cutoff here rather than reusing current time.
    from datetime import timedelta

    cutoff_literal = (reference_time - timedelta(days=bundle.window_days)).strftime("%Y-%m-%d %H:%M:%S.%f")
    select_parts: list[str] = []
    for tmpl in bundle.templates:
        select_parts.append(f"{group_b_projected_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        if tmpl.extra_predicate:
            select_parts.append(f"{group_b_projected_presence_expr(tmpl)} AS `{presence_column(tmpl.template_id)}`")
    projected_columns = [
        "d.agent_type",
        "d.exact_id",
        "d.smart_id",
        "d.input_ip",
        "d.proxy_ip",
    ]
    projected_columns.extend(
        f"CASE WHEN d.{column} IS NOT NULL AND d.{column} != '' THEN CAST(d.{column} AS DECIMAL(10,2)) END AS `{column}__num`"
        for column in NUMERIC_SCORE_COLUMNS
    )
    return (
        "SELECT\n  "
        + ",\n  ".join(select_parts)
        + "\nFROM (\n"
        + "  SELECT "
        + ", ".join(projected_columns)
        + "\n  FROM deviceprofile_fact d\n"
        + f"  WHERE {bundle.base_filter} AND d.jms_timestamp >= '{cutoff_literal}'\n"
        + ") d\n"
        + "HAVING COUNT(*) > 0;"
    )


def build_bundle_catalog() -> dict[str, tuple[Any, str]]:
    catalog: dict[str, tuple[Any, str]] = {}
    for bundle in cluster_group_a_templates():
        catalog[bundle.bundle_id] = (bundle, "A")
    for bundle in cluster_group_b_templates():
        catalog[bundle.bundle_id] = (bundle, "B")
    for bundle in cluster_group_c_templates():
        catalog[bundle.bundle_id] = (bundle, "C")
    return catalog


def candidate_sqls(bundle, group: str, reference_time: datetime) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    if group == "A" and bundle.bundle_id in GROUP_A_ROLLUP_BUNDLES:
        candidates.append(("group_a_dimension_rollup", render_group_a_dimension_rollup_sql(bundle, reference_time)))
    if group == "B" and bundle.bundle_id == "group_b_bundle_012":
        candidates.append(("group_b_numeric_projection", render_group_b_numeric_projection_sql(bundle, reference_time)))
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mixed-json", default="results/mixed_traffic_1780029697.json")
    parser.add_argument("--slow-csv", default="results/slow_bundles_post_index_3eps_60s.csv")
    parser.add_argument("--output", default="results/scan_pruning_rewrite_attempts.md")
    parser.add_argument("--summary-json", default="results/scan_pruning_rewrite_attempts.json")
    parser.add_argument("--bundle-id", action="append", default=[])
    parser.add_argument("--explain-timeout-ms", type=int, default=30000)
    args = parser.parse_args()

    mixed_path = resolve_path(args.mixed_json)
    slow_csv_path = resolve_path(args.slow_csv)
    output_path = resolve_path(args.output)
    summary_path = resolve_path(args.summary_json)
    mixed = json.loads(mixed_path.read_text(encoding="utf-8"))
    slow_rows = list(csv.DictReader(slow_csv_path.open()))
    bundle_ids = args.bundle_id or [row["bundle_id"] for row in slow_rows]
    events_by_invoice = {
        event["invoice_number"]: event
        for event in mixed.get("sampled_normal_events", []) + mixed.get("sampled_hot_events", [])
    }
    records_by_bundle = load_records(mixed, bundle_ids)
    catalog = build_bundle_catalog()
    preagg_layout = mixed.get("preagg_layout", "prod180")

    cfg = get_db_config(save_msg="scan pruning rewrite exploration")
    conn = pymysql.connect(**cfg)
    conn.autocommit(True)

    lines: list[str] = []
    summaries: list[dict[str, Any]] = []
    lines.append("# Scan/CASE-Pruning Rewrite Attempts")
    lines.append("")
    lines.append(f"- Generated: `{datetime.now().isoformat(timespec='seconds')}`")
    lines.append(f"- Mixed JSON: `{mixed_path}`")
    lines.append(f"- Slow CSV: `{slow_csv_path}`")
    lines.append("- Session baseline for all tests: TiKV/TiDB only, CTE force-inline off, distinct pushdown off unless candidate timing variant enables it.")
    lines.append("")

    try:
        with conn.cursor() as cur:
            for bundle_id in bundle_ids:
                records = records_by_bundle.get(bundle_id) or []
                if not records or bundle_id not in catalog:
                    continue
                chosen = choose_record(records)
                event = events_by_invoice[chosen["event"]]
                reference_time = datetime.fromisoformat(event["reference_time"])
                bundle, group = catalog[bundle_id]
                current_sql = render_bundle_sql(
                    bundle,
                    group,
                    reference_time,
                    hinted_a=set(),
                    preagg_bundles=PREAGG_BUNDLES,
                    preagg_layout=preagg_layout,
                )
                params = bundle_params(bundle, reference_time, event["bindings"], PREAGG_BUNDLES, preagg_layout)
                candidates = candidate_sqls(bundle, group, reference_time)
                if not candidates:
                    continue

                lines.append(f"## {bundle_id}")
                lines.append("")
                lines.append(f"- Group/window/filter: `{group}` / `{getattr(bundle, 'window_days', None)}d` / `{getattr(bundle, 'base_filter', None)}`")
                lines.append(f"- Chosen event: `{chosen['event']}` kind=`{chosen['kind']}` bundle_ms=`{float(chosen['ms']):.1f}` event_ms=`{float(chosen['event_ms']):.1f}`")
                stats = summarize_bundle(records)
                lines.append(f"- Bundle stats: n=`{stats['n']}` >350=`{stats['over350']}` >500=`{stats['over500']}` p95=`{stats['p95']:.1f}ms` max=`{stats['max']:.1f}ms`")
                lines.append("")

                current_rows = query_rows(cur, current_sql, params, args.explain_timeout_ms)
                current_plan = explain_analyze(cur, current_sql, params, {}, args.explain_timeout_ms)
                lines.append("### Current Optimized")
                lines.append("")
                lines.append(f"- SELECT time: `{current_rows['elapsed_ms']:.1f}ms` result=`{'ok' if current_rows['ok'] else current_rows.get('error')}`")
                lines.append(f"- EXPLAIN ANALYZE: `{current_plan['elapsed_ms']:.1f}ms`, scan_sum=`{current_plan['total_process_keys_sum']}`, scan_max=`{current_plan['total_process_keys_max']}`")
                lines.append("")

                for candidate_name, candidate_sql in candidates:
                    candidate_rows = query_rows(cur, candidate_sql, params, args.explain_timeout_ms)
                    same_result = (
                        current_rows.get("ok")
                        and candidate_rows.get("ok")
                        and current_rows.get("columns") == candidate_rows.get("columns")
                        and current_rows.get("rows") == candidate_rows.get("rows")
                    )
                    results: list[dict[str, Any]] = []
                    for variant_name, settings in [("candidate_default", {})] + [
                        (name, settings) for name, settings in OPTIMIZED_VARIANTS if settings
                    ]:
                        result = explain_analyze(cur, candidate_sql, params, settings, args.explain_timeout_ms)
                        result.update({"variant": variant_name, "settings": settings})
                        results.append(result)
                    ok_results = [item for item in results if item["ok"]]
                    best = min(ok_results, key=lambda item: item["elapsed_ms"]) if ok_results else results[0]
                    accepted = bool(same_result and current_plan["ok"] and best["ok"] and best["elapsed_ms"] < current_plan["elapsed_ms"] * 0.95)
                    summary = {
                        "bundle_id": bundle_id,
                        "candidate": candidate_name,
                        "same_result": same_result,
                        "current_ms": current_plan["elapsed_ms"],
                        "current_scan_sum": current_plan["total_process_keys_sum"],
                        "best_ms": best["elapsed_ms"],
                        "best_variant": best["variant"],
                        "best_scan_sum": best["total_process_keys_sum"],
                        "accepted": accepted,
                    }
                    summaries.append(summary)

                    lines.append(f"### Candidate: {candidate_name}")
                    lines.append("")
                    lines.append(f"- Result check: `{'same' if same_result else 'DIFF'}`; SELECT time: `{candidate_rows['elapsed_ms']:.1f}ms`")
                    lines.append(f"- Best EXPLAIN ANALYZE: `{best['elapsed_ms']:.1f}ms` variant=`{best['variant']}` scan_sum=`{best['total_process_keys_sum']}` scan_max=`{best['total_process_keys_max']}` accepted=`{accepted}`")
                    lines.append("")
                    lines.append("| Variant | Time | Scan Sum | Scan Max | Result |")
                    lines.append("| --- | ---: | ---: | ---: | --- |")
                    for result in results:
                        status = "ok" if result["ok"] else result["error"]
                        lines.append(
                            f"| `{result['variant']}` | {result['elapsed_ms']:.1f} ms | {result['total_process_keys_sum']} | {result['total_process_keys_max']} | {status} |"
                        )
                    lines.append("")
                    lines.append("#### Candidate SQL")
                    lines.append("")
                    lines.append("```sql")
                    lines.append(candidate_sql)
                    lines.append("```")
                    lines.append("")
                    lines.append("#### Best Candidate EXPLAIN ANALYZE")
                    lines.append("")
                    lines.append("```text")
                    if best["ok"]:
                        lines.append(f"-- variant={best['variant']}")
                        lines.append(f"-- explain_analyze_elapsed_ms={best['elapsed_ms']:.1f}")
                        lines.append(best["plan"])
                    else:
                        lines.append(best["error"])
                    lines.append("```")
                    lines.append("")
    finally:
        conn.close()

    output_path.write_text("\n".join(lines), encoding="utf-8")
    summary_path.write_text(json.dumps(summaries, indent=2, default=str), encoding="utf-8")
    print(output_path)
    print(summary_path)


if __name__ == "__main__":
    main()
