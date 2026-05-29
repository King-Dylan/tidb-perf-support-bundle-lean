#!/usr/bin/env python3
"""Generate before/after EXPLAIN ANALYZE evidence for optimized slow bundles."""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pymysql

from demo import (
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
from explain_problem_bundles import PREAGG_BUNDLES, build_bundle_catalog, choose_record, summarize_bundle, tabular
from lib.db_config import get_db_config
from mixed_traffic_test import bundle_params, render_bundle_sql
from preagg_rollups import (
    bundle_rollup_metrics,
    is_avg_helper,
    key_fields,
    prod180_cutoff_parts,
    prod180_distinct_table,
    prod180_full_day_predicate,
    prod180_key_predicates,
    prod180_rollup_table,
    quote_ident,
    raw_key_predicates,
    raw_not_null_predicates,
    raw_window_predicate,
    source_parts,
    sql_literal,
)


ROOT = Path(__file__).resolve().parent

BASELINE_SESSION = {
    "tidb_isolation_read_engines": "tikv,tidb",
    "tidb_opt_force_inline_cte": 0,
    "tidb_opt_distinct_agg_push_down": 0,
    "tidb_hashagg_final_concurrency": 4,
    "tidb_hashagg_partial_concurrency": 4,
}

OPTIMIZED_VARIANTS = [
    ("optimized_default", {}),
    ("optimized_hashagg_16_8", {"tidb_hashagg_final_concurrency": 16, "tidb_hashagg_partial_concurrency": 8}),
    ("optimized_hashagg_32_8", {"tidb_hashagg_final_concurrency": 32, "tidb_hashagg_partial_concurrency": 8}),
    ("optimized_distinct_pushdown", {"tidb_opt_distinct_agg_push_down": 1}),
    (
        "optimized_distinct_pushdown_hashagg_16_8",
        {
            "tidb_opt_distinct_agg_push_down": 1,
            "tidb_hashagg_final_concurrency": 16,
            "tidb_hashagg_partial_concurrency": 8,
        },
    ),
]


def resolve_path(raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else ROOT / path


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    lo = math.floor(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def render_original_runtime_sql(bundle, group: str, reference_time: datetime) -> str:
    select_parts: list[str] = []
    for tmpl in bundle.templates:
        if group == "A":
            select_parts.append(f"{build_group_a_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        elif group == "B":
            select_parts.append(f"{build_group_b_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        else:
            select_parts.append(f"{build_group_c_metric_expr(tmpl)} AS `{metric_column(tmpl.template_id)}`")
        if tmpl.extra_predicate:
            select_parts.append(f"{build_presence_expr(tmpl.extra_predicate)} AS `{presence_column(tmpl.template_id)}`")

    if group == "A":
        cutoff_ms = int((reference_time.timestamp() - (bundle.window_days * 86400)) * 1000)
        return (
            "SELECT\n  "
            + ",\n  ".join(select_parts)
            + "\nFROM pmt_txn_fact p\nWHERE "
            + f"{bundle.base_filter} AND p.event_date >= {cutoff_ms}"
            + "\nGROUP BY "
            + ", ".join(bundle.group_by_fields)
            + ";"
        )
    if group == "B":
        cutoff_literal = (reference_time - timedelta(days=bundle.window_days)).strftime("%Y-%m-%d %H:%M:%S.%f")
        return (
            "SELECT\n  "
            + ",\n  ".join(select_parts)
            + "\nFROM deviceprofile_fact d\nWHERE "
            + f"{bundle.base_filter} AND d.jms_timestamp >= '{cutoff_literal}'"
            + "\nGROUP BY "
            + ", ".join(bundle.group_by_fields)
            + ";"
        )

    cutoff_ms = int(reference_time.timestamp() * 1000) - (bundle.window_days * 86400 * 1000)
    cutoff_dt = datetime.fromtimestamp(cutoff_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
    return (
        "SELECT\n  "
        + ",\n  ".join(select_parts)
        + "\nFROM pmt_txn_fact p\nLEFT OUTER JOIN deviceprofile_fact d"
        + "\n  ON p.parsed_interaction_id = d.interaction_id"
        + f"\nWHERE {bundle.base_filter} AND p.event_date >= {cutoff_ms}"
        + f"\n  AND d.jms_timestamp >= '{cutoff_dt}'"
        + "\nGROUP BY "
        + ", ".join(bundle.group_by_fields)
        + ";"
    )


def render_original_prod180_sql(group: str, bundle, reference_time: datetime) -> str:
    rollups, distincts = bundle_rollup_metrics(group, bundle)
    keys = key_fields(bundle)
    cutoff_parts = prod180_cutoff_parts(reference_time)

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
        raw_where_parts = raw_key_predicates(bundle) + raw_not_null_predicates(group) + [raw_window_predicate(group, cutoff_parts)]
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


def original_params(bundle, group: str, reference_time: datetime, bindings: dict[str, Any], preagg_layout: str) -> tuple[Any, ...]:
    if bundle.bundle_id not in PREAGG_BUNDLES:
        return tuple(bindings.get(name) for name in bundle.param_names)
    if preagg_layout != "prod180":
        return bundle_params(bundle, reference_time, bindings, PREAGG_BUNDLES, preagg_layout)
    key_values = tuple(None if bindings.get(k) is None else str(bindings.get(k)) for k in key_fields(bundle))
    rollups, _ = bundle_rollup_metrics(group, bundle)
    params: list[Any] = []
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


def render_original_sql(bundle, group: str, reference_time: datetime, preagg_layout: str) -> str:
    if bundle.bundle_id in PREAGG_BUNDLES and preagg_layout == "prod180":
        return render_original_prod180_sql(group, bundle, reference_time)
    return render_original_runtime_sql(bundle, group, reference_time)


def apply_session(cur, settings: dict[str, Any], max_execution_time: int) -> None:
    combined = dict(BASELINE_SESSION)
    combined.update(settings)
    cur.execute("SET SESSION tidb_isolation_read_engines = %s", (combined["tidb_isolation_read_engines"],))
    cur.execute(f"SET SESSION tidb_opt_force_inline_cte = {int(combined['tidb_opt_force_inline_cte'])}")
    cur.execute(f"SET SESSION tidb_opt_distinct_agg_push_down = {int(combined['tidb_opt_distinct_agg_push_down'])}")
    cur.execute(f"SET SESSION tidb_hashagg_final_concurrency = {int(combined['tidb_hashagg_final_concurrency'])}")
    cur.execute(f"SET SESSION tidb_hashagg_partial_concurrency = {int(combined['tidb_hashagg_partial_concurrency'])}")
    cur.execute("SET SESSION max_execution_time = %s", (max_execution_time,))


def explain_analyze(cur, sql: str, params: tuple[Any, ...], settings: dict[str, Any], max_execution_time: int) -> dict[str, Any]:
    apply_session(cur, settings, max_execution_time)
    started = time.perf_counter()
    try:
        cur.execute("EXPLAIN ANALYZE " + sql.rstrip().rstrip(";"), params)
        rows = cur.fetchall()
        columns = tuple(desc[0] for desc in cur.description)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {
            "ok": True,
            "elapsed_ms": elapsed_ms,
            "plan": tabular(rows, columns),
            "top_operator": rows[0][0] if rows else "",
            "top_info": rows[0][5] if rows and len(rows[0]) > 5 else "",
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {"ok": False, "elapsed_ms": elapsed_ms, "error": repr(exc), "plan": ""}


def method_for(bundle_id: str, original_sql: str, optimized_sql: str, best_variant: str) -> str:
    methods: list[str] = []
    if "GROUP BY" in original_sql and "HAVING COUNT(*) > 0" in optimized_sql:
        methods.append("SQL rewrite: remove redundant GROUP BY on constant key, keep empty-result semantics with HAVING COUNT(*) > 0")
    if "COUNT(*) AS row_count" in optimized_sql and "amount_sum" in optimized_sql and ") b\nHAVING SUM(b.row_count) > 0" in optimized_sql:
        methods.append("SQL rewrite: pre-aggregate low-cardinality payment dimensions, then pivot CASE metrics from the compact rollup")
    if "raw_boundary AS" in optimized_sql and "raw_boundary AS" not in original_sql:
        methods.append("SQL rewrite: materialize raw cutoff boundary once via CTE and share it across distinct metrics")
    if "idx_pmt_" in optimized_sql or bundle_id.startswith("group_c_bundle_"):
        if bundle_id in {"group_c_bundle_007", "group_c_bundle_014", "group_c_bundle_021", "group_c_bundle_024", "group_c_bundle_025"}:
            methods.append("Index: use payment-side covering join index to avoid p table row lookup")
    if best_variant != "optimized_default":
        methods.append(f"Session tuning: {best_variant}")
    if not methods:
        methods.append("No material improvement found; retained current covering-index/CTE shape")
    return "; ".join(methods)


def load_records(mixed: dict[str, Any], bundle_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    records_by_bundle: dict[str, list[dict[str, Any]]] = {bundle_id: [] for bundle_id in bundle_ids}
    for event_result in mixed["read_results"]:
        for bundle_result in event_result.get("bundle_results", []):
            bundle_id = bundle_result["bundle_id"]
            if bundle_id not in records_by_bundle:
                continue
            records_by_bundle[bundle_id].append(
                {
                    **bundle_result,
                    "event": event_result["event"],
                    "event_ms": event_result["ms"],
                    "kind": event_result["kind"],
                    "hot_field": event_result.get("hot_field"),
                }
            )
    return records_by_bundle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mixed-json", default="results/mixed_traffic_1780029697.json")
    parser.add_argument("--slow-csv", default="results/slow_bundles_post_index_3eps_60s.csv")
    parser.add_argument("--output", default="results/final_before_after_optimization_report.md")
    parser.add_argument("--summary-json", default="results/final_before_after_optimization_summary.json")
    parser.add_argument("--explain-timeout-ms", type=int, default=30000)
    args = parser.parse_args()

    mixed_path = resolve_path(args.mixed_json)
    slow_csv_path = resolve_path(args.slow_csv)
    output_path = resolve_path(args.output)
    summary_json_path = resolve_path(args.summary_json)

    mixed = json.loads(mixed_path.read_text(encoding="utf-8"))
    slow_rows = list(csv.DictReader(slow_csv_path.open()))
    bundle_ids = [row["bundle_id"] for row in slow_rows]
    records_by_bundle = load_records(mixed, bundle_ids)
    events_by_invoice = {
        event["invoice_number"]: event
        for event in mixed.get("sampled_normal_events", []) + mixed.get("sampled_hot_events", [])
    }
    catalog = build_bundle_catalog()
    preagg_layout = mixed.get("preagg_layout", "prod180")

    cfg = get_db_config(save_msg="final before/after optimization report")
    conn = pymysql.connect(**cfg)
    conn.autocommit(True)

    details: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            for index, bundle_id in enumerate(bundle_ids, start=1):
                records = records_by_bundle[bundle_id]
                if not records:
                    continue
                chosen = choose_record(records)
                event = events_by_invoice[chosen["event"]]
                reference_time = datetime.fromisoformat(event["reference_time"])
                bundle, group = catalog[bundle_id]
                original_sql = render_original_sql(bundle, group, reference_time, preagg_layout)
                original_param_values = original_params(bundle, group, reference_time, event["bindings"], preagg_layout)
                optimized_sql = render_bundle_sql(
                    bundle,
                    group,
                    reference_time,
                    hinted_a=set(),
                    preagg_bundles=PREAGG_BUNDLES,
                    preagg_layout=preagg_layout,
                )
                optimized_param_values = bundle_params(
                    bundle,
                    reference_time,
                    event["bindings"],
                    PREAGG_BUNDLES,
                    preagg_layout,
                )
                print(f"[{index}/{len(bundle_ids)}] baseline {bundle_id}", flush=True)
                baseline = explain_analyze(cur, original_sql, original_param_values, {}, args.explain_timeout_ms)

                candidates: list[dict[str, Any]] = []
                for variant_name, variant_settings in OPTIMIZED_VARIANTS:
                    print(f"[{index}/{len(bundle_ids)}] {variant_name} {bundle_id}", flush=True)
                    result = explain_analyze(cur, optimized_sql, optimized_param_values, variant_settings, args.explain_timeout_ms)
                    candidates.append({"name": variant_name, "settings": variant_settings, **result})
                ok_candidates = [candidate for candidate in candidates if candidate["ok"]]
                best = min(ok_candidates, key=lambda item: item["elapsed_ms"]) if ok_candidates else candidates[0]
                method = method_for(bundle_id, original_sql, optimized_sql, best["name"])
                stats = summarize_bundle(records)
                before_ms = baseline["elapsed_ms"] if baseline["ok"] else None
                after_ms = best["elapsed_ms"] if best["ok"] else None
                speedup_pct = ((before_ms - after_ms) / before_ms * 100.0) if before_ms and after_ms else None
                summary = {
                    "bundle_id": bundle_id,
                    "group": group,
                    "window_days": getattr(bundle, "window_days", None),
                    "filter": getattr(bundle, "base_filter", None),
                    "preagg": bundle_id in PREAGG_BUNDLES,
                    "chosen_event": chosen["event"],
                    "chosen_kind": chosen["kind"],
                    "chosen_error": chosen.get("error"),
                    "baseline_ms": before_ms,
                    "optimized_ms": after_ms,
                    "speedup_pct": speedup_pct,
                    "best_variant": best["name"],
                    "method": method,
                    "status": "unresolved" if after_ms and after_ms > 500 else "optimized",
                }
                summary_rows.append(summary)
                details.append(
                    {
                        "index": index,
                        "summary": summary,
                        "stats": stats,
                        "original_sql": original_sql,
                        "original_params": original_param_values,
                        "baseline": baseline,
                        "optimized_sql": optimized_sql,
                        "optimized_params": optimized_param_values,
                        "candidates": candidates,
                        "best": best,
                    }
                )
    finally:
        conn.close()

    lines: list[str] = []
    lines.append("# Final Slow SQL Optimization Report")
    lines.append("")
    lines.append(f"- Generated: `{datetime.now().isoformat(timespec='seconds')}`")
    lines.append(f"- Source mixed JSON: `{mixed_path}`")
    lines.append(f"- Source slow CSV: `{slow_csv_path}`")
    lines.append("- Scope: only the 15 bundles still appearing in the post-index slow/error list")
    lines.append("- Baseline session: TiKV/TiDB only, `tidb_opt_distinct_agg_push_down=0`, `tidb_hashagg_final_concurrency=4`, `tidb_hashagg_partial_concurrency=4`")
    lines.append("- Optimized candidates tested per SQL: default optimized SQL, hashagg 16/8, hashagg 32/8, distinct pushdown, distinct pushdown + hashagg 16/8")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Bundle | Filter | Optimization | Before | Best After | Delta | Best Variant | Status |")
    lines.append("| --- | --- | --- | ---: | ---: | ---: | --- | --- |")
    for row in summary_rows:
        before = f"{row['baseline_ms']:.1f} ms" if row["baseline_ms"] is not None else "failed"
        after = f"{row['optimized_ms']:.1f} ms" if row["optimized_ms"] is not None else "failed"
        delta = f"{row['speedup_pct']:.1f}%" if row["speedup_pct"] is not None else "N/A"
        lines.append(
            f"| `{row['bundle_id']}` | `{row['filter']}` | {row['method']} | {before} | {after} | {delta} | `{row['best_variant']}` | `{row['status']}` |"
        )
    lines.append("")
    lines.append("## Detailed Evidence")
    lines.append("")

    for item in details:
        summary = item["summary"]
        lines.append(f"### {item['index']}. {summary['bundle_id']}")
        lines.append("")
        lines.append(f"- Filter/window: `{summary['filter']}` / `{summary['window_days']}d`")
        lines.append(f"- Chosen event: `{summary['chosen_event']}` kind=`{summary['chosen_kind']}` error=`{summary['chosen_error']}`")
        lines.append(f"- Optimization: {summary['method']}")
        lines.append("")
        lines.append("#### Candidate Timings")
        lines.append("")
        lines.append("| Candidate | Settings | Time | Result |")
        lines.append("| --- | --- | ---: | --- |")
        baseline = item["baseline"]
        baseline_time = f"{baseline['elapsed_ms']:.1f} ms" if baseline["ok"] else f"failed after {baseline['elapsed_ms']:.1f} ms"
        baseline_result = "ok" if baseline["ok"] else baseline["error"]
        lines.append(f"| `original_baseline` | `{BASELINE_SESSION}` | {baseline_time} | {baseline_result} |")
        for candidate in item["candidates"]:
            time_text = f"{candidate['elapsed_ms']:.1f} ms" if candidate["ok"] else f"failed after {candidate['elapsed_ms']:.1f} ms"
            result_text = "ok" if candidate["ok"] else candidate["error"]
            lines.append(f"| `{candidate['name']}` | `{candidate['settings']}` | {time_text} | {result_text} |")
        lines.append("")
        lines.append("#### Original SQL")
        lines.append("")
        lines.append("```sql")
        lines.append(item["original_sql"])
        lines.append("```")
        lines.append("")
        lines.append("#### Original Params")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(item["original_params"], indent=2, default=str))
        lines.append("```")
        lines.append("")
        lines.append("#### Original EXPLAIN ANALYZE")
        lines.append("")
        lines.append("```text")
        if item["baseline"]["ok"]:
            lines.append(f"-- explain_analyze_elapsed_ms={item['baseline']['elapsed_ms']:.1f}")
            lines.append(item["baseline"]["plan"])
        else:
            lines.append(f"-- failed_after_ms={item['baseline']['elapsed_ms']:.1f}")
            lines.append(item["baseline"]["error"])
        lines.append("```")
        lines.append("")
        lines.append("#### Optimized SQL")
        lines.append("")
        lines.append("```sql")
        lines.append(item["optimized_sql"])
        lines.append("```")
        lines.append("")
        lines.append("#### Optimized Params")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(item["optimized_params"], indent=2, default=str))
        lines.append("```")
        lines.append("")
        lines.append("#### Optimized EXPLAIN ANALYZE")
        lines.append("")
        lines.append("```text")
        best = item["best"]
        if best["ok"]:
            lines.append(f"-- best_variant={best['name']}")
            lines.append(f"-- explain_analyze_elapsed_ms={best['elapsed_ms']:.1f}")
            lines.append(best["plan"])
        else:
            lines.append(f"-- best_variant={best['name']}")
            lines.append(f"-- failed_after_ms={best['elapsed_ms']:.1f}")
            lines.append(best["error"])
        lines.append("```")
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    summary_json_path.write_text(json.dumps(summary_rows, indent=2, default=str), encoding="utf-8")
    print(output_path)
    print(summary_json_path)


if __name__ == "__main__":
    main()
