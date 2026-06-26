#!/usr/bin/env python3
"""Generate a static workload file for the Go load generator.

The Python benchmark code already knows how to render the 65 bundle SQLs and
their event-specific parameters.  The Go load generator should not spend CPU on
template rendering, so this script materializes that boundary into JSON once.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from demo import (
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
    group_b_runtime_predicate_repetitions,
)
from mixed_traffic_test import bundle_params, render_bundle_sql, should_skip_null_binding
from optimized_config import EXACT_SERVING_BUNDLES, PROD180_PREAGG_BUNDLES


ROOT = Path(__file__).resolve().parent
KEY_FIELDS = [
    "merchant_account_number",
    "card_holder_number_sha512",
    "check_bank_routing_number",
    "check_bank_account_number_sha512",
    "exact_id",
    "smart_id",
    "input_ip",
    "true_ip",
]


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


def all_bundle_pairs() -> list[tuple[Any, str]]:
    return (
        [(bundle, "A") for bundle in cluster_group_a_templates()]
        + [(bundle, "B") for bundle in cluster_group_b_templates()]
        + [(bundle, "C") for bundle in cluster_group_c_templates()]
    )


def planned_hot_count(event_count: int, hot_event_pct: float) -> int:
    if hot_event_pct <= 0:
        return 0
    hot_stride = int(round(1 / hot_event_pct))
    if hot_stride <= 0:
        return 0
    return sum(1 for idx in range(event_count) if idx % hot_stride == 0)


def validate_source_pool(
    normal: list[dict[str, Any]],
    hot_by_field: dict[str, list[dict[str, Any]]],
    event_count: int,
    hot_event_pct: float,
    allow_event_reuse: bool,
    require_hot_fields: bool,
) -> tuple[int, int]:
    hot_needed = planned_hot_count(event_count, hot_event_pct)
    normal_needed = event_count - hot_needed

    if not allow_event_reuse and len(normal) < normal_needed:
        raise ValueError(
            "source event pool is too small for a no-reuse workload: "
            f"need {normal_needed:,} normal events, found {len(normal):,}. "
            "Rebuild the reuse-events source pool with more normal events, or explicitly pass --allow-event-reuse for a smoke test."
        )

    if hot_needed > 0:
        present_fields = {field for field, events in hot_by_field.items() if events}
        missing_fields = [field for field in KEY_FIELDS if field not in present_fields]
        if require_hot_fields and missing_fields:
            raise ValueError(
                "source event pool is missing hot-key events for required fields: "
                + ", ".join(missing_fields)
            )
        if not present_fields:
            raise ValueError("hot-event percentage was requested, but the source pool contains no hot events")

        fields_used = [field for field in KEY_FIELDS if field in present_fields]
        hot_needed_by_field = {field: 0 for field in fields_used}
        for hot_idx in range(hot_needed):
            hot_needed_by_field[fields_used[hot_idx % len(fields_used)]] += 1
        if not allow_event_reuse:
            short = {
                field: (needed, len(hot_by_field[field]))
                for field, needed in hot_needed_by_field.items()
                if len(hot_by_field[field]) < needed
            }
            if short:
                details = ", ".join(
                    f"{field}: need {needed:,}, found {found:,}"
                    for field, (needed, found) in short.items()
                )
                raise ValueError(
                    "source event pool is too small for no-reuse hot-key coverage: "
                    f"{details}. Rebuild with a higher --hot-events-per-field, or explicitly pass --allow-event-reuse for a smoke test."
                )

    return normal_needed, hot_needed


def select_events(
    payload: dict[str, Any],
    event_count: int,
    hot_event_pct: float,
    allow_event_reuse: bool,
    require_hot_fields: bool,
) -> list[dict[str, Any]]:
    normal = list(payload.get("sampled_normal_events", []))
    hot = list(payload.get("sampled_hot_events", []))
    if not normal and not hot:
        raise ValueError("input JSON does not contain sampled_normal_events or sampled_hot_events")

    hot_by_field: dict[str, list[dict[str, Any]]] = {field: [] for field in KEY_FIELDS}
    for event in hot:
        field = event.get("hot_field")
        if field in hot_by_field:
            hot_by_field[field].append(event)

    validate_source_pool(
        normal,
        hot_by_field,
        event_count,
        hot_event_pct,
        allow_event_reuse=allow_event_reuse,
        require_hot_fields=require_hot_fields,
    )

    selected: list[dict[str, Any]] = []
    normal_idx = 0
    hot_idx_by_field = {field: 0 for field in KEY_FIELDS}
    hot_fields = [field for field in KEY_FIELDS if hot_by_field[field]]
    hot_field_idx = 0
    hot_stride = int(round(1 / hot_event_pct)) if hot_event_pct > 0 else 0
    for idx in range(event_count):
        use_hot = bool(hot) and hot_stride > 0 and idx % hot_stride == 0
        if use_hot:
            field = hot_fields[hot_field_idx % len(hot_fields)]
            field_events = hot_by_field[field]
            source_idx = hot_idx_by_field[field]
            if allow_event_reuse:
                source_idx %= len(field_events)
            selected.append(field_events[source_idx])
            hot_idx_by_field[field] += 1
            hot_field_idx += 1
        else:
            source_idx = normal_idx
            if allow_event_reuse:
                source_idx %= len(normal)
            selected.append(normal[source_idx])
            normal_idx += 1
    return selected


def binding_key(event: dict[str, Any]) -> tuple[Any, ...]:
    bindings = event.get("bindings") or {}
    return tuple(bindings.get(field) for field in KEY_FIELDS)


def json_param(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    return str(value)


def is_runtime_bundle(bundle: Any, preagg_bundles: set[str], serving_bundles: set[str]) -> bool:
    return bundle.bundle_id not in preagg_bundles and bundle.bundle_id not in serving_bundles


def bundle_mode(bundle_id: str, preagg_bundles: set[str], serving_bundles: set[str]) -> str:
    if bundle_id in serving_bundles:
        return "serving"
    if bundle_id in preagg_bundles:
        return "preagg"
    return "runtime"


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
    ap.add_argument("--events", type=int, default=0, help="Total workload events to generate. If omitted with --target-event-eps/--duration, computed as EPS * duration.")
    ap.add_argument("--target-event-eps", type=float, default=0.0, help="Fleet-wide target event EPS used to compute --events.")
    ap.add_argument("--duration", default="", help="Run duration used to compute --events, for example 300s, 5m, or 10m.")
    ap.add_argument("--hot-event-pct", type=float, default=0.05)
    ap.add_argument(
        "--allow-event-reuse",
        action="store_true",
        help="Allow cycling/reusing source events. Keep false for customer-facing final runs; useful only for smoke tests.",
    )
    ap.add_argument(
        "--require-hot-fields",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require hot-key events for all 8 key fields when hot-event-pct is non-zero.",
    )
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

    computed_events = 0
    if args.target_event_eps > 0 or args.duration:
        if not (args.target_event_eps > 0 and args.duration):
            raise ValueError("--target-event-eps and --duration must be provided together")
        computed_events = events_from_rate(args.target_event_eps, args.duration)
        if args.events and args.events != computed_events:
            raise ValueError(
                f"--events ({args.events:,}) does not match --target-event-eps * --duration ({computed_events:,}). "
                "Omit --events to avoid mismatch."
            )
        args.events = computed_events
    elif args.events <= 0:
        args.events = 1000

    source_path = ROOT / args.reuse_events_json
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    events = select_events(
        payload,
        args.events,
        args.hot_event_pct,
        allow_event_reuse=args.allow_event_reuse,
        require_hot_fields=args.require_hot_fields,
    )

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
                "mode": bundle_mode(bundle.bundle_id, preagg_bundles, serving_bundles),
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
                if (
                    bundle.bundle_id in runtime_window_param_bundles
                    and group == "B"
                    and group_b_runtime_predicate_repetitions(bundle) > 1
                ):
                    base_params = [json_param(event["bindings"].get(name)) for name in bundle.param_names]
                    window_params = [
                        json_param(value)
                        for value in runtime_window_params(
                            bundle,
                            group,
                            reference_time,
                            template_sql_by_bundle[bundle.bundle_id],
                        )
                    ]
                    params = []
                    for _ in range(group_b_runtime_predicate_repetitions(bundle)):
                        params.extend(base_params)
                        params.extend(window_params)
                elif bundle.bundle_id in runtime_window_param_bundles:
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
                "bindings": event.get("bindings") or {},
                "bundles": bundle_runs,
            }
        )

    source_event_ids = [event.get("invoice_number") for event in events]
    full_binding_keys = [binding_key(event) for event in events]
    selected_hot_by_field = {
        field: sum(1 for event in events if event.get("hot_field") == field)
        for field in KEY_FIELDS
    }
    output = {
        "generated_at_unix": time.time(),
        "source_events_json": str(source_path),
        "mode": "bundle-serving" if args.preagg_mode == "serving" else args.preagg_mode,
        "event_count": len(workload_events),
        "target_event_eps": args.target_event_eps,
        "duration": args.duration,
        "computed_events_from_rate": computed_events,
        "bundle_count": len(templates),
        "serving_as_of_grain": args.serving_as_of_grain,
        "preagg_layout": args.preagg_layout,
        "runtime_window_params": args.runtime_window_params,
        "runtime_window_param_bundle_count": len(runtime_window_param_bundles),
        "allow_event_reuse": args.allow_event_reuse,
        "require_hot_fields": args.require_hot_fields,
        "runtime_bundles": sorted(bundle.bundle_id for bundle, _ in bundle_pairs if is_runtime_bundle(bundle, preagg_bundles, serving_bundles)),
        "preagg_bundles": sorted(preagg_bundles),
        "serving_bundles": sorted(serving_bundles),
        "tiflash_mpp_bundles": sorted(tiflash_mpp_bundles),
        "hot_key_profile": (payload.get("profile") or {}).get("hot_fields", {}),
        "workload_stats": {
            "generated_workload_rows": len(workload_events),
            "unique_source_events": len(set(source_event_ids)),
            "unique_full_binding_sets": len(set(full_binding_keys)),
            "source_event_reuse_max": max((source_event_ids.count(event_id) for event_id in set(source_event_ids)), default=0),
            "full_binding_reuse_max": max((full_binding_keys.count(key) for key in set(full_binding_keys)), default=0),
            "hot_event_pct": args.hot_event_pct,
            "planned_hot_events": planned_hot_count(args.events, args.hot_event_pct),
            "planned_normal_events": args.events - planned_hot_count(args.events, args.hot_event_pct),
            "selected_hot_events_by_field": selected_hot_by_field,
        },
        "templates": templates,
        "events": workload_events,
    }
    output_path = ROOT / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Wrote {output_path} events={len(workload_events)} bundles={len(templates)}")


if __name__ == "__main__":
    main()
