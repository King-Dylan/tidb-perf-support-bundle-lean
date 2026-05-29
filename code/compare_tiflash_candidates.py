#!/usr/bin/env python3
"""Compare TiKV and TiFlash plans for selected bundle SQLs.

This helper is intentionally narrow: it renders the same bundle SQL used by
the mixed traffic test, picks representative sampled events from a prior run,
and runs EXPLAIN ANALYZE under a few storage-engine variants.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pymysql

from demo import cluster_group_a_templates, cluster_group_b_templates, cluster_group_c_templates
from lib.db_config import get_db_config
from mixed_traffic_test import bundle_params, render_bundle_sql
from optimized_config import PROD180_PREAGG_BUNDLES
from preagg_rollups import render_prod180_runtime_query, render_runtime_query


ROOT = Path(__file__).resolve().parent

DEFAULT_BUNDLES = [
    "group_b_bundle_012",
    "group_b_bundle_018",
    "group_b_bundle_020",
    "group_c_bundle_018",
]

VARIANTS = {
    "tikv": "tikv,tidb",
    "cost": "tikv,tiflash,tidb",
    "tiflash_hint": "tikv,tiflash,tidb",
    "tiflash_hint_mpp_off": "tikv,tiflash,tidb",
    "tiflash_only": "tiflash,tidb",
}


def resolve_path(raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else ROOT / path


def tabular(rows: tuple[tuple[Any, ...], ...], columns: tuple[str, ...]) -> str:
    out = ["\t".join(columns)]
    for row in rows:
        out.append("\t".join("" if value is None else str(value) for value in row))
    return "\n".join(out)


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


def build_bundle_catalog() -> dict[str, tuple[Any, str]]:
    catalog: dict[str, tuple[Any, str]] = {}
    for bundle in cluster_group_a_templates():
        catalog[bundle.bundle_id] = (bundle, "A")
    for bundle in cluster_group_b_templates():
        catalog[bundle.bundle_id] = (bundle, "B")
    for bundle in cluster_group_c_templates():
        catalog[bundle.bundle_id] = (bundle, "C")
    return catalog


def filter_binding_names(bundle: Any) -> list[str]:
    names: list[str] = []
    for match in re.finditer(r"\b([pd])\.([a-z0-9_]+)\s*=\s*%s\b", bundle.base_filter, re.I):
        names.append(match.group(2))
    return names


def load_candidate_events(mixed_json: Path, bundle: Any, max_events: int) -> list[dict[str, Any]]:
    mixed = json.loads(mixed_json.read_text(encoding="utf-8"))
    normal_events = mixed.get("sampled_normal_events", [])
    hot_events = mixed.get("sampled_hot_events", [])
    binding_names = filter_binding_names(bundle)
    primary = binding_names[0] if binding_names else None

    selected: list[dict[str, Any]] = []
    if primary:
        matching_hot = [
            event for event in hot_events
            if event.get("hot_field") == primary and event.get("bindings", {}).get(primary) is not None
        ]
        matching_hot.sort(key=lambda event: int(event.get("hot_count") or 0), reverse=True)
        selected.extend(matching_hot[:max_events])

    if len(selected) < max_events and normal_events:
        selected.extend(normal_events[: max_events - len(selected)])

    if not selected:
        raise RuntimeError(f"No sampled event found for {bundle.bundle_id} from {mixed_json}")
    return selected[:max_events]


def render_candidate_sql(
    bundle: Any,
    group: str,
    reference_time: datetime,
    variant: str,
    preagg_bundles: set[str],
    preagg_layout: str,
) -> str:
    preagg_applied = bundle.bundle_id in preagg_bundles
    is_tiflash_hint = variant.startswith("tiflash_hint")
    if preagg_applied:
        if preagg_layout == "prod180":
            sql = render_prod180_runtime_query(group, bundle, reference_time)
        else:
            sql = render_runtime_query(group, bundle, reference_time)
    elif group == "A":
        base_bundle_id = bundle.bundle_id.split("_split", 1)[0]
        return bundle.render_sql(reference_time, hinted=(is_tiflash_hint and base_bundle_id == bundle.bundle_id))
    elif group == "C":
        return bundle.render_sql(reference_time, hinted=is_tiflash_hint)
    else:
        sql = bundle.render_sql(reference_time)

    if not is_tiflash_hint:
        return sql
    if group == "B":
        if preagg_applied:
            return sql.replace(
                "UNION ALL SELECT ",
                "UNION ALL SELECT /*+ READ_FROM_STORAGE(TIFLASH[d]) */ ",
            )
        return sql.replace("SELECT\n", "SELECT /*+ READ_FROM_STORAGE(TIFLASH[d]) */\n", 1)
    if group == "C":
        return sql.replace("SELECT\n", "SELECT /*+ READ_FROM_STORAGE(TIFLASH[p,d]) */\n", 1)
    if group == "A":
        return sql.replace("SELECT\n", "SELECT /*+ READ_FROM_STORAGE(TIFLASH[p]) */\n", 1)
    return sql


def configure_session(
    cur,
    engines: str,
    max_execution_time_ms: int,
    distinct_pushdown: bool,
    force_inline_cte: str | None,
    hashagg_final_concurrency: int | None,
    hashagg_partial_concurrency: int | None,
    allow_mpp: bool | None,
    session_vars: list[str],
) -> None:
    cur.execute("SET SESSION tidb_isolation_read_engines = %s", (engines,))
    cur.execute("SET SESSION max_execution_time = %s", (max_execution_time_ms,))
    if allow_mpp is not None:
        cur.execute(f"SET SESSION tidb_allow_mpp = {1 if allow_mpp else 0}")
    if distinct_pushdown:
        cur.execute("SET SESSION tidb_opt_distinct_agg_push_down = 1")
    if force_inline_cte is not None:
        cur.execute(f"SET SESSION tidb_opt_force_inline_cte = {int(force_inline_cte)}")
    if hashagg_final_concurrency is not None:
        cur.execute(f"SET SESSION tidb_hashagg_final_concurrency = {int(hashagg_final_concurrency)}")
    if hashagg_partial_concurrency is not None:
        cur.execute(f"SET SESSION tidb_hashagg_partial_concurrency = {int(hashagg_partial_concurrency)}")
    for raw in session_vars:
        if "=" not in raw:
            raise ValueError(f"--session-var must use name=value, got {raw!r}")
        name, value = raw.split("=", 1)
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            raise ValueError(f"Unsafe session variable name {name!r}")
        cur.execute(f"SET SESSION {name} = %s", (value,))


def summarize_plan(plan_rows: tuple[tuple[Any, ...], ...], columns: tuple[str, ...]) -> dict[str, Any]:
    task_idx = columns.index("task") if "task" in columns else -1
    access_idx = columns.index("access object") if "access object" in columns else -1
    info_idx = columns.index("execution info") if "execution info" in columns else -1
    tasks: set[str] = set()
    access_objects: list[str] = []
    execution_infos: list[str] = []
    for row in plan_rows:
        if task_idx >= 0 and row[task_idx]:
            tasks.add(str(row[task_idx]))
        if access_idx >= 0 and row[access_idx]:
            access_objects.append(str(row[access_idx]))
        if info_idx >= 0 and row[info_idx]:
            execution_infos.append(str(row[info_idx]))
    return {
        "tasks": sorted(tasks),
        "access_objects": access_objects[:20],
        "execution_info_prefix": execution_infos[:8],
    }


def explain_analyze(
    conn,
    sql: str,
    params: tuple[Any, ...],
    variant: str,
    max_execution_time_ms: int,
    distinct_pushdown: bool,
    force_inline_cte: str | None,
    hashagg_final_concurrency: int | None,
    hashagg_partial_concurrency: int | None,
    session_vars: list[str],
) -> dict[str, Any]:
    engines = VARIANTS[variant]
    allow_mpp = False if variant.endswith("_mpp_off") else None
    with conn.cursor() as cur:
        configure_session(
            cur,
            engines,
            max_execution_time_ms,
            distinct_pushdown,
            force_inline_cte,
            hashagg_final_concurrency,
            hashagg_partial_concurrency,
            allow_mpp,
            session_vars,
        )
        started = time.perf_counter()
        try:
            cur.execute("EXPLAIN ANALYZE " + sql.rstrip().rstrip(";"), params)
            rows = cur.fetchall()
            columns = tuple(desc[0] for desc in cur.description)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return {
                "ok": True,
                "elapsed_ms": elapsed_ms,
                "engines": engines,
                "columns": columns,
                "plan_rows": rows,
                "plan_text": tabular(rows, columns),
                "plan_summary": summarize_plan(rows, columns),
            }
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return {
                "ok": False,
                "elapsed_ms": elapsed_ms,
                "engines": engines,
                "error": str(exc),
            }


def write_markdown(output: Path, payload: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# TiKV vs TiFlash Candidate A/B")
    lines.append("")
    lines.append(f"- Generated: `{payload['generated_at']}`")
    lines.append(f"- Mixed JSON: `{payload['mixed_json']}`")
    lines.append(f"- Pre-agg layout: `{payload['preagg_layout']}`")
    lines.append(f"- Pre-agg bundle count: `{len(payload['preagg_bundles'])}`")
    lines.append(f"- Session knobs: distinct_pushdown=`{payload['distinct_pushdown']}`, force_inline_cte=`{payload['force_inline_cte']}`, hashagg_final=`{payload['hashagg_final_concurrency']}`, hashagg_partial=`{payload['hashagg_partial_concurrency']}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| bundle | group | event | variant | engines | elapsed | storage tasks | result |")
    lines.append("|---|---:|---|---|---|---:|---|---|")
    for item in payload["results"]:
        event = item["event"]
        for variant in item["variants"]:
            if variant["ok"]:
                elapsed = f"{variant['elapsed_ms']:.1f} ms"
                tasks = ", ".join(variant["plan_summary"]["tasks"])
                result = "ok"
            else:
                elapsed = f"{variant['elapsed_ms']:.1f} ms"
                tasks = ""
                result = variant["error"].replace("|", "\\|")[:120]
            lines.append(
                "| "
                + " | ".join(
                    [
                        item["bundle_id"],
                        item["group"],
                        f"{event['kind']}:{event.get('hot_field') or '-'}",
                        variant["variant"],
                        variant["engines"],
                        elapsed,
                        tasks,
                        result,
                    ]
                )
                + " |"
            )
    lines.append("")

    for index, item in enumerate(payload["results"], start=1):
        lines.append(f"## {index}. {item['bundle_id']}")
        lines.append("")
        event = item["event"]
        lines.append(f"- Group/window/filter: `{item['group']}` / `{item['window_days']}d` / `{item['base_filter']}`")
        lines.append(f"- Preagg applied: `{item['preagg_applied']}`")
        lines.append(f"- Event: invoice=`{event['invoice_number']}` kind=`{event['kind']}` hot_field=`{event.get('hot_field')}` hot_count=`{event.get('hot_count')}` ref=`{event['reference_time']}`")
        lines.append("")
        lines.append("### Params")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(item["params"], indent=2, default=str))
        lines.append("```")
        lines.append("")
        for variant in item["variants"]:
            lines.append(f"### {variant['variant']}")
            lines.append("")
            lines.append(f"- Engines: `{variant['engines']}`")
            lines.append(f"- Elapsed: `{variant['elapsed_ms']:.1f} ms`")
            if not variant["ok"]:
                lines.append(f"- Error: `{variant['error']}`")
                lines.append("")
                continue
            lines.append(f"- Storage tasks: `{', '.join(variant['plan_summary']['tasks'])}`")
            lines.append("")
            lines.append("```sql")
            lines.append(variant["sql"])
            lines.append("```")
            lines.append("")
            lines.append("```text")
            lines.append(f"-- explain_analyze_elapsed_ms={variant['elapsed_ms']:.1f}")
            lines.append(variant["plan_text"])
            lines.append("```")
            lines.append("")

    output.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mixed-json", default="results/mixed_traffic_1780091850.json")
    parser.add_argument("--output-json", default="results/tiflash_candidate_ab.json")
    parser.add_argument("--output-md", default="results/tiflash_candidate_ab.md")
    parser.add_argument("--bundle-id", action="append", default=[])
    parser.add_argument("--max-events-per-bundle", type=int, default=1)
    parser.add_argument("--variant", action="append", choices=sorted(VARIANTS), default=[])
    parser.add_argument("--preagg-mode", choices=["runtime-only", "hybrid"], default="hybrid")
    parser.add_argument("--preagg-layout", choices=["bundle", "prod180"], default="prod180")
    parser.add_argument("--max-execution-time-ms", type=int, default=10000)
    parser.add_argument("--distinct-agg-pushdown", action="store_true", default=True)
    parser.add_argument("--no-distinct-agg-pushdown", dest="distinct_agg_pushdown", action="store_false")
    parser.add_argument("--force-inline-cte", choices=["0", "1"], default="0")
    parser.add_argument("--hashagg-final-concurrency", type=int, default=16)
    parser.add_argument("--hashagg-partial-concurrency", type=int, default=8)
    parser.add_argument("--session-var", action="append", default=[], help="Extra SET SESSION knob as name=value. Repeatable.")
    args = parser.parse_args()

    mixed_json = resolve_path(args.mixed_json)
    output_json = resolve_path(args.output_json)
    output_md = resolve_path(args.output_md)
    bundle_ids = args.bundle_id or DEFAULT_BUNDLES
    variants = args.variant or ["tikv", "cost", "tiflash_hint", "tiflash_only"]
    preagg_bundles = set(PROD180_PREAGG_BUNDLES) if args.preagg_mode == "hybrid" else set()

    catalog = build_bundle_catalog()
    unknown = sorted(set(bundle_ids) - set(catalog))
    if unknown:
        raise ValueError(f"Unknown bundle ids: {', '.join(unknown)}")

    cfg = get_db_config(save_msg="compare tiflash candidates")
    cfg.setdefault("connect_timeout", 10)
    cfg.setdefault("read_timeout", max(30, math.ceil(args.max_execution_time_ms / 1000) + 10))
    cfg.setdefault("write_timeout", max(30, math.ceil(args.max_execution_time_ms / 1000) + 10))
    conn = pymysql.connect(**cfg)
    conn.autocommit(True)

    payload: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mixed_json": str(mixed_json),
        "preagg_mode": args.preagg_mode,
        "preagg_layout": args.preagg_layout,
        "preagg_bundles": sorted(preagg_bundles),
        "distinct_pushdown": args.distinct_agg_pushdown,
        "force_inline_cte": args.force_inline_cte,
        "hashagg_final_concurrency": args.hashagg_final_concurrency,
        "hashagg_partial_concurrency": args.hashagg_partial_concurrency,
        "session_vars": args.session_var,
        "results": [],
    }

    try:
        for bundle_id in bundle_ids:
            bundle, group = catalog[bundle_id]
            for event in load_candidate_events(mixed_json, bundle, args.max_events_per_bundle):
                reference_time = datetime.fromisoformat(event["reference_time"])
                params = bundle_params(bundle, reference_time, event["bindings"], preagg_bundles, args.preagg_layout)
                item = {
                    "bundle_id": bundle_id,
                    "group": group,
                    "window_days": getattr(bundle, "window_days", None),
                    "base_filter": getattr(bundle, "base_filter", None),
                    "preagg_applied": bundle_id in preagg_bundles,
                    "event": {
                        "invoice_number": event.get("invoice_number"),
                        "kind": event.get("kind"),
                        "hot_field": event.get("hot_field"),
                        "hot_count": event.get("hot_count"),
                        "reference_time": event.get("reference_time"),
                    },
                    "params": params,
                    "variants": [],
                }
                for variant in variants:
                    sql = render_candidate_sql(bundle, group, reference_time, variant, preagg_bundles, args.preagg_layout)
                    result = explain_analyze(
                        conn,
                        sql,
                        params,
                        variant,
                        args.max_execution_time_ms,
                        args.distinct_agg_pushdown,
                        args.force_inline_cte,
                        args.hashagg_final_concurrency,
                        args.hashagg_partial_concurrency,
                        args.session_var,
                    )
                    result["variant"] = variant
                    result["sql"] = sql
                    item["variants"].append(result)
                    status = "ok" if result["ok"] else "ERR"
                    print(f"{bundle_id} {event.get('hot_field') or event.get('kind')} {variant}: {status} {result['elapsed_ms']:.1f} ms", flush=True)
                payload["results"].append(item)
    finally:
        conn.close()

    output_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_markdown(output_md, payload)
    print(output_json)
    print(output_md)


if __name__ == "__main__":
    main()
