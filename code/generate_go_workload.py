#!/usr/bin/env python3
"""Generate a static workload file for the Go load generator.

The Python benchmark code already knows how to render the 65 bundle SQLs and
their event-specific parameters.  The Go load generator should not spend CPU on
template rendering, so this script materializes that boundary into JSON once.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from demo import cluster_group_a_templates, cluster_group_b_templates, cluster_group_c_templates
from mixed_traffic_test import bundle_params, render_bundle_sql, should_skip_null_binding
from optimized_config import EXACT_SERVING_BUNDLES, PROD180_PREAGG_BUNDLES


ROOT = Path(__file__).resolve().parent


def all_bundle_pairs() -> list[tuple[Any, str]]:
    return (
        [(bundle, "A") for bundle in cluster_group_a_templates()]
        + [(bundle, "B") for bundle in cluster_group_b_templates()]
        + [(bundle, "C") for bundle in cluster_group_c_templates()]
    )


def select_events(payload: dict[str, Any], event_count: int, hot_event_pct: float) -> list[dict[str, Any]]:
    normal = list(payload.get("sampled_normal_events", []))
    hot = list(payload.get("sampled_hot_events", []))
    if not normal and not hot:
        raise ValueError("input JSON does not contain sampled_normal_events or sampled_hot_events")

    selected: list[dict[str, Any]] = []
    normal_idx = 0
    hot_idx = 0
    hot_stride = int(round(1 / hot_event_pct)) if hot_event_pct > 0 else 0
    for idx in range(event_count):
        use_hot = bool(hot) and hot_stride > 0 and idx % hot_stride == 0
        if use_hot:
            selected.append(hot[hot_idx % len(hot)])
            hot_idx += 1
        else:
            selected.append(normal[normal_idx % len(normal)])
            normal_idx += 1
    return selected


def json_param(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    return str(value)


def is_runtime_bundle(bundle: Any, preagg_bundles: set[str], serving_bundles: set[str]) -> bool:
    return bundle.bundle_id not in preagg_bundles and bundle.bundle_id not in serving_bundles


def runtime_window_values(bundle: Any, group: str, reference_time: datetime) -> dict[str, Any]:
    start_ms = int(reference_time.timestamp() * 1000) - (bundle.window_days * 86400 * 1000)
    if group == "B":
        start_dt = (reference_time - timedelta(days=bundle.window_days)).strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        start_dt = datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")
    return {
        "start_ms": start_ms,
        "end_ms": int(reference_time.timestamp() * 1000),
        "start_dt": start_dt,
        "end_dt": reference_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
    }


def parameterize_runtime_window_sql(sql: str, bundle: Any, group: str, reference_time: datetime) -> str:
    values = runtime_window_values(bundle, group, reference_time)
    output = sql
    if group in {"A", "C"}:
        needle = f"p.event_date >= {values['start_ms']} AND p.event_date < {values['end_ms']}"
        replacement = "p.event_date >= %s AND p.event_date < %s"
        if needle in output:
            output = output.replace(needle, replacement)
        else:
            needle = f"p.event_date >= {values['start_ms']}"
            if needle not in output:
                raise ValueError(f"Could not find p.event_date runtime window in {bundle.bundle_id}")
            output = output.replace(needle, replacement)
    if group in {"B", "C"}:
        needle = f"d.jms_timestamp >= '{values['start_dt']}' AND d.jms_timestamp < '{values['end_dt']}'"
        replacement = "d.jms_timestamp >= %s AND d.jms_timestamp < %s"
        if needle in output:
            output = output.replace(needle, replacement)
        else:
            needle = f"d.jms_timestamp >= '{values['start_dt']}'"
            if needle not in output:
                raise ValueError(f"Could not find d.jms_timestamp runtime window in {bundle.bundle_id}")
            output = output.replace(needle, replacement)
    return output


def runtime_window_params(bundle: Any, group: str, reference_time: datetime, parameterized_sql: str) -> tuple[Any, ...]:
    values = runtime_window_values(bundle, group, reference_time)
    if group == "A":
        return (values["start_ms"], values["end_ms"])
    if group == "B":
        return (values["start_dt"], values["end_dt"])
    if group == "C":
        p_pos = parameterized_sql.find("p.event_date >= %s")
        d_pos = parameterized_sql.find("d.jms_timestamp >= %s")
        if p_pos < 0 or d_pos < 0:
            raise ValueError(f"Could not find parameterized Group C windows in {bundle.bundle_id}")
        if p_pos < d_pos:
            return (values["start_ms"], values["end_ms"], values["start_dt"], values["end_dt"])
        return (values["start_dt"], values["end_dt"], values["start_ms"], values["end_ms"])
    return ()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reuse-events-json", required=True, help="Prior mixed_traffic JSON with sampled events.")
    ap.add_argument("--output", required=True, help="Output workload JSON path.")
    ap.add_argument("--events", type=int, default=1000)
    ap.add_argument("--hot-event-pct", type=float, default=0.05)
    ap.add_argument("--preagg-mode", choices=["serving", "hybrid", "runtime-only"], default="serving")
    ap.add_argument("--preagg-layout", choices=["prod180", "bundle"], default=os.getenv("PREAGG_LAYOUT", "prod180"))
    ap.add_argument("--serving-as-of-grain", choices=["day", "timestamp"], default=os.getenv("INTUIT_SERVING_AS_OF_GRAIN", "day"))
    ap.add_argument("--serving-bundle", action="append", default=[])
    ap.add_argument("--exclude-bundle", action="append", default=[])
    ap.add_argument(
        "--tiflash-mpp-bundle",
        action="append",
        default=[],
        help="Runtime bundle id to force through TiFlash MPP in the generated SQL template. Repeatable.",
    )
    ap.add_argument(
        "--runtime-window-params",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Parameterize non-preagg runtime start/end windows per event so prepared templates do not freeze the first event reference time.",
    )
    args = ap.parse_args()

    source_path = ROOT / args.reuse_events_json
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    events = select_events(payload, args.events, args.hot_event_pct)

    bundle_pairs = all_bundle_pairs()
    excluded = set(args.exclude_bundle)
    if excluded:
        bundle_pairs = [(bundle, group) for bundle, group in bundle_pairs if bundle.bundle_id not in excluded]

    serving_bundles = set(args.serving_bundle)
    tiflash_mpp_bundles = set(args.tiflash_mpp_bundle)
    if args.preagg_mode == "serving" and not serving_bundles:
        serving_bundles = set(EXACT_SERVING_BUNDLES)
    if args.preagg_mode in {"hybrid", "serving"}:
        preagg_bundles = set(PROD180_PREAGG_BUNDLES) - serving_bundles
    else:
        preagg_bundles = set()
    tiflash_mpp_bundles -= preagg_bundles | serving_bundles

    templates = []
    template_sql_by_bundle: dict[str, str] = {}
    runtime_window_param_bundles: set[str] = set()
    for bundle, group in bundle_pairs:
        reference_time = datetime.fromisoformat(events[0]["reference_time"])
        sql = render_bundle_sql(
            bundle,
            group,
            reference_time,
            hinted_a=set(),
            preagg_bundles=preagg_bundles,
            preagg_layout=args.preagg_layout,
            serving_bundles=serving_bundles,
            serving_as_of_grain=args.serving_as_of_grain,
            tiflash_mpp_bundles=tiflash_mpp_bundles,
        )
        if args.runtime_window_params and is_runtime_bundle(bundle, preagg_bundles, serving_bundles):
            sql = parameterize_runtime_window_sql(sql, bundle, group, reference_time)
            runtime_window_param_bundles.add(bundle.bundle_id)
        template_sql_by_bundle[bundle.bundle_id] = sql
        templates.append(
            {
                "bundle_id": bundle.bundle_id,
                "group": group,
                "sql": sql,
            }
        )

    workload_events = []
    for event_idx, event in enumerate(events):
        reference_time = datetime.fromisoformat(event["reference_time"])
        bundle_runs = []
        for bundle, group in bundle_pairs:
            skip = should_skip_null_binding(bundle, event["bindings"])
            params = []
            if not skip:
                params = [
                    json_param(value)
                    for value in bundle_params(
                        bundle,
                        reference_time,
                        event["bindings"],
                        preagg_bundles=preagg_bundles,
                        preagg_layout=args.preagg_layout,
                        serving_bundles=serving_bundles,
                        serving_as_of_grain=args.serving_as_of_grain,
                    )
                ]
                if bundle.bundle_id in runtime_window_param_bundles:
                    params.extend(
                        json_param(value)
                        for value in runtime_window_params(
                            bundle,
                            group,
                            reference_time,
                            template_sql_by_bundle[bundle.bundle_id],
                        )
                    )
            bundle_runs.append(
                {
                    "bundle_id": bundle.bundle_id,
                    "skip": skip,
                    "params": params,
                }
            )
        workload_events.append(
            {
                "index": event_idx,
                "event": event.get("invoice_number"),
                "kind": event.get("kind"),
                "hot_field": event.get("hot_field"),
                "bundles": bundle_runs,
            }
        )

    output = {
        "generated_at_unix": time.time(),
        "source_events_json": str(source_path),
        "mode": "bundle-serving" if args.preagg_mode == "serving" else args.preagg_mode,
        "event_count": len(workload_events),
        "bundle_count": len(templates),
        "serving_as_of_grain": args.serving_as_of_grain,
        "preagg_layout": args.preagg_layout,
        "runtime_window_params": args.runtime_window_params,
        "runtime_window_param_bundle_count": len(runtime_window_param_bundles),
        "tiflash_mpp_bundles": sorted(tiflash_mpp_bundles),
        "templates": templates,
        "events": workload_events,
    }
    output_path = ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Wrote {output_path} events={len(workload_events)} bundles={len(templates)}")


if __name__ == "__main__":
    main()
