#!/usr/bin/env python3
"""Mixed-traffic sustained HTAP test for Henry's clarified SLA.

This test is intentionally different from the fixed hot/non-hot demo run:

* samples many real joined events;
* mixes mostly normal events with a small percentage of hot-key events;
* runs the same 65 bundled queries per event;
* keeps the same background write workload;
* reports TP99 plus how many events exceed 350 ms and 500 ms.

The goal is not to prove every worst-case hot key is below 350 ms. Henry
clarified that the SLA is TP99 across traffic, with rare tails tolerated.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pymysql

from demo import (
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
)
from lib.db_config import get_db_config
from optimized_config import PROD180_PREAGG_BUNDLES
from preagg_rollups import bundle_rollup_metrics, key_fields, render_prod180_runtime_query, render_runtime_query
from sustained_test import writer_thread


ROOT = Path(__file__).resolve().parent

FILTER_FIELDS = [
    ("payment", "p", "merchant_account_number", "merchant_account_number"),
    ("payment", "p", "card_holder_number_sha512", "card_holder_number_sha512"),
    ("payment", "p", "check_bank_routing_number", "check_bank_routing_number"),
    ("payment", "p", "check_bank_account_number_sha512", "check_bank_account_number_sha512"),
    ("device", "d", "exact_id", "exact_id"),
    ("device", "d", "smart_id", "smart_id"),
    ("device", "d", "input_ip", "input_ip"),
    ("device", "d", "true_ip", "true_ip"),
]

EVENT_COLUMNS = """
    p.invoice_number,
    p.event_date,
    p.merchant_account_number,
    p.card_holder_number_sha512,
    p.check_bank_routing_number,
    p.check_bank_account_number_sha512,
    d.exact_id,
    d.smart_id,
    d.input_ip,
    d.true_ip,
    p.parsed_interaction_id
"""

JOIN_AND_NON_NULLS = """
FROM pmt_txn_fact p
JOIN deviceprofile_fact d
  ON p.parsed_interaction_id = d.interaction_id
WHERE p.merchant_account_number IS NOT NULL
  AND p.card_holder_number_sha512 IS NOT NULL
  AND p.check_bank_routing_number IS NOT NULL
  AND p.check_bank_account_number_sha512 IS NOT NULL
  AND p.parsed_interaction_id IS NOT NULL
  AND d.exact_id IS NOT NULL
  AND d.smart_id IS NOT NULL
  AND d.input_ip IS NOT NULL
  AND d.true_ip IS NOT NULL
"""


def percentile(vals: list[float], pct: float) -> float:
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    ordered = sorted(vals)
    rank = (len(ordered) - 1) * (pct / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def summarize(vals: list[float]) -> dict[str, float | int]:
    if not vals:
        return {"n": 0}
    return {
        "n": len(vals),
        "p50": percentile(vals, 50),
        "p95": percentile(vals, 95),
        "p99": percentile(vals, 99),
        "min": min(vals),
        "max": max(vals),
        "avg": statistics.mean(vals),
        "over_350": sum(1 for v in vals if v > 350),
        "over_500": sum(1 for v in vals if v > 500),
    }


def configure_read_session(cur, max_execution_time_ms: int | None = None) -> None:
    isolation_read_engines = os.getenv("TIDB_ISOLATION_READ_ENGINES", "tikv,tidb").strip()
    force_inline_cte = os.getenv("INTUIT_FORCE_INLINE_CTE", "0")
    if isolation_read_engines:
        cur.execute("SET SESSION tidb_isolation_read_engines = %s", (isolation_read_engines,))
    if max_execution_time_ms:
        cur.execute("SET SESSION max_execution_time = %s", (max_execution_time_ms,))
    if os.getenv("INTUIT_DISTINCT_AGG_PUSH_DOWN") == "1":
        cur.execute("SET SESSION tidb_opt_distinct_agg_push_down = 1")
    if force_inline_cte in {"0", "1"}:
        cur.execute(f"SET SESSION tidb_opt_force_inline_cte = {int(force_inline_cte)}")
    if os.getenv("INTUIT_HASHAGG_FINAL_CONCURRENCY"):
        cur.execute(f"SET SESSION tidb_hashagg_final_concurrency = {int(os.environ['INTUIT_HASHAGG_FINAL_CONCURRENCY'])}")
    if os.getenv("INTUIT_HASHAGG_PARTIAL_CONCURRENCY"):
        cur.execute(f"SET SESSION tidb_hashagg_partial_concurrency = {int(os.environ['INTUIT_HASHAGG_PARTIAL_CONCURRENCY'])}")


def make_pool(size: int, max_execution_time_ms: int | None = None) -> Queue:
    cfg = get_db_config(save_msg="mixed traffic test")
    pool: Queue = Queue(maxsize=size)
    for _ in range(size):
        conn = pymysql.connect(**cfg)
        conn.autocommit(True)
        with conn.cursor() as cur:
            configure_read_session(cur, max_execution_time_ms=max_execution_time_ms)
        pool.put(conn)
    return pool


def make_event(row: tuple[Any, ...], kind: str, hot_field: str | None = None, hot_count: int | None = None) -> dict[str, Any]:
    event_date_ms = int(row[1])
    return {
        "invoice_number": row[0],
        "kind": kind,
        "hot_field": hot_field,
        "hot_count": hot_count,
        "reference_time": datetime.fromtimestamp(event_date_ms / 1000).isoformat(),
        "bindings": {
            "merchant_account_number": row[2],
            "card_holder_number_sha512": row[3],
            "check_bank_routing_number": row[4],
            "check_bank_account_number_sha512": row[5],
            "exact_id": row[6],
            "smart_id": row[7],
            "input_ip": row[8],
            "true_ip": row[9],
            "parsed_interaction_id": row[10],
        },
    }


def top_value(cur, table: str, column: str) -> tuple[str, int]:
    cur.execute(
        f"""
        SELECT {column}, COUNT(*) AS c
        FROM {table}
        WHERE {column} IS NOT NULL
          AND {column} <> ''
        GROUP BY {column}
        ORDER BY c DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No top value found for {table}.{column}")
    return str(row[0]), int(row[1])


def fetch_events_for_hot_value(cur, alias: str, column: str, value: str, field_name: str, hot_count: int, limit: int) -> list[dict[str, Any]]:
    candidate_limit = max(limit * 200, 1000)
    if alias == "p":
        cur.execute(
            f"""
            SELECT {EVENT_COLUMNS}
            FROM (
                SELECT
                    invoice_number,
                    event_date,
                    merchant_account_number,
                    card_holder_number_sha512,
                    check_bank_routing_number,
                    check_bank_account_number_sha512,
                    parsed_interaction_id
                FROM pmt_txn_fact
                WHERE merchant_account_number IS NOT NULL
                  AND card_holder_number_sha512 IS NOT NULL
                  AND check_bank_routing_number IS NOT NULL
                  AND check_bank_account_number_sha512 IS NOT NULL
                  AND parsed_interaction_id IS NOT NULL
                  AND {column} = %s
                ORDER BY event_date DESC
                LIMIT %s
            ) p
            JOIN deviceprofile_fact d
              ON p.parsed_interaction_id = d.interaction_id
            WHERE d.exact_id IS NOT NULL
              AND d.smart_id IS NOT NULL
              AND d.input_ip IS NOT NULL
              AND d.true_ip IS NOT NULL
            ORDER BY p.event_date DESC
            LIMIT %s
            """,
            (value, candidate_limit, limit),
        )
    else:
        cur.execute(
            f"""
            SELECT {EVENT_COLUMNS}
            FROM pmt_txn_fact p
            JOIN (
                SELECT
                    interaction_id,
                    exact_id,
                    smart_id,
                    input_ip,
                    true_ip,
                    jms_timestamp
                FROM deviceprofile_fact
                WHERE interaction_id IS NOT NULL
                  AND exact_id IS NOT NULL
                  AND smart_id IS NOT NULL
                  AND input_ip IS NOT NULL
                  AND true_ip IS NOT NULL
                  AND {column} = %s
                ORDER BY jms_timestamp DESC
                LIMIT %s
            ) d
              ON p.parsed_interaction_id = d.interaction_id
            WHERE p.merchant_account_number IS NOT NULL
              AND p.card_holder_number_sha512 IS NOT NULL
              AND p.check_bank_routing_number IS NOT NULL
              AND p.check_bank_account_number_sha512 IS NOT NULL
              AND p.parsed_interaction_id IS NOT NULL
            ORDER BY p.event_date DESC
            LIMIT %s
            """,
            (value, candidate_limit, limit),
        )
    return [
        make_event(row, kind=f"hot_{field_name}", hot_field=field_name, hot_count=hot_count)
        for row in cur.fetchall()
    ]


def value_count(cur, table: str, column: str, value: Any) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} = %s", (value,))
    return int(cur.fetchone()[0])


def event_key_counts(cur, event: dict[str, Any]) -> dict[str, int]:
    bindings = event["bindings"]
    counts: dict[str, int] = {}
    for _, alias, column, field_name in FILTER_FIELDS:
        table = "pmt_txn_fact" if alias == "p" else "deviceprofile_fact"
        counts[field_name] = value_count(cur, table, column, bindings[field_name])
    return counts


def is_normal_event(counts: dict[str, int], max_payment_rows: int, max_device_rows: int) -> bool:
    payment_fields = {
        "merchant_account_number",
        "card_holder_number_sha512",
        "check_bank_routing_number",
        "check_bank_account_number_sha512",
    }
    for field, count in counts.items():
        limit = max_payment_rows if field in payment_fields else max_device_rows
        if count > limit:
            return False
    return True


def sample_normal_events(
    cur,
    limit: int,
    excluded_hot_values: dict[str, str],
    max_payment_rows: int,
    max_device_rows: int,
    validate_counts: bool,
) -> list[dict[str, Any]]:
    p_clauses = []
    d_clauses = []
    p_params: list[Any] = []
    d_params: list[Any] = []
    for _, alias, column, field_name in FILTER_FIELDS:
        hot = excluded_hot_values.get(field_name)
        if hot:
            if alias == "p":
                p_clauses.append(f"AND {column} <> %s")
                p_params.append(hot)
            else:
                d_clauses.append(f"AND d.{column} <> %s")
                d_params.append(hot)
    candidate_limit = max(limit * (60 if validate_counts else 25), 10000)
    cur.execute(
        f"""
        SELECT
            invoice_number,
            event_date,
            merchant_account_number,
            card_holder_number_sha512,
            check_bank_routing_number,
            check_bank_account_number_sha512,
            parsed_interaction_id
        FROM pmt_txn_fact
        WHERE merchant_account_number IS NOT NULL
          AND card_holder_number_sha512 IS NOT NULL
          AND check_bank_routing_number IS NOT NULL
          AND check_bank_account_number_sha512 IS NOT NULL
          AND parsed_interaction_id IS NOT NULL
          {' '.join(p_clauses)}
        ORDER BY event_date DESC
        LIMIT %s
        """,
        tuple(p_params + [candidate_limit]),
    )
    payment_rows = list(cur.fetchall())
    random.shuffle(payment_rows)

    rows = []
    batch_size = 500
    for i in range(0, len(payment_rows), batch_size):
        batch = payment_rows[i : i + batch_size]
        interaction_ids = [row[6] for row in batch if row[6]]
        if not interaction_ids:
            continue
        placeholders = ", ".join(["%s"] * len(interaction_ids))
        cur.execute(
            f"""
            SELECT
                d.interaction_id,
                d.exact_id,
                d.smart_id,
                d.input_ip,
                d.true_ip
            FROM deviceprofile_fact d
            WHERE d.interaction_id IN ({placeholders})
              AND d.exact_id IS NOT NULL
              AND d.smart_id IS NOT NULL
              AND d.input_ip IS NOT NULL
              AND d.true_ip IS NOT NULL
              {' '.join(d_clauses)}
            """,
            tuple(interaction_ids + d_params),
        )
        device_by_interaction = {row[0]: row for row in cur.fetchall()}
        for p_row in batch:
            d_row = device_by_interaction.get(p_row[6])
            if not d_row:
                continue
            rows.append(
                (
                    p_row[0],
                    p_row[1],
                    p_row[2],
                    p_row[3],
                    p_row[4],
                    p_row[5],
                    d_row[1],
                    d_row[2],
                    d_row[3],
                    d_row[4],
                    p_row[6],
                )
            )
            if len(rows) >= max(limit * (30 if validate_counts else 2), 300):
                break
        if len(rows) >= max(limit * (30 if validate_counts else 2), 300):
            break

    random.shuffle(rows)
    events: list[dict[str, Any]] = []
    for row in rows:
        event = make_event(row, kind="normal")
        if not validate_counts:
            events.append(event)
        else:
            counts = event_key_counts(cur, event)
            event["key_counts"] = counts
            if is_normal_event(counts, max_payment_rows=max_payment_rows, max_device_rows=max_device_rows):
                events.append(event)
        if len(events) >= limit:
            break
    return events


def sample_mixed_events(
    normal_count: int,
    hot_events_per_field: int,
    max_payment_rows: int,
    max_device_rows: int,
    validate_normal_counts: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    cfg = get_db_config(save_msg="mixed traffic event sampler")
    conn = pymysql.connect(**cfg)
    conn.autocommit(True)
    hot_events: list[dict[str, Any]] = []
    excluded_hot_values: dict[str, str] = {}
    profile: dict[str, Any] = {"hot_fields": {}}
    try:
        with conn.cursor() as cur:
            configure_read_session(cur)
            for table_name, alias, column, field_name in FILTER_FIELDS:
                table = "pmt_txn_fact" if alias == "p" else "deviceprofile_fact"
                value, count = top_value(cur, table, column)
                excluded_hot_values[field_name] = value
                profile["hot_fields"][field_name] = {"table": table_name, "value": value, "count": count}
                hot_events.extend(fetch_events_for_hot_value(cur, alias, column, value, field_name, count, hot_events_per_field))

            normal_events = sample_normal_events(
                cur,
                normal_count,
                excluded_hot_values,
                max_payment_rows=max_payment_rows,
                max_device_rows=max_device_rows,
                validate_counts=validate_normal_counts,
            )
    finally:
        conn.close()

    if not normal_events:
        raise RuntimeError("No normal events sampled")
    if not hot_events:
        raise RuntimeError("No hot events sampled")
    return normal_events, hot_events, profile


def render_bundle_sql(
    bundle,
    group: str,
    reference_time: datetime,
    hinted_a: set[str],
    preagg_bundles: set[str],
    preagg_layout: str,
) -> str:
    if bundle.bundle_id in preagg_bundles:
        if preagg_layout == "prod180":
            return render_prod180_runtime_query(group, bundle, reference_time)
        return render_runtime_query(group, bundle, reference_time)
    if group == "A":
        base_bundle_id = bundle.bundle_id.split("_split", 1)[0]
        return bundle.render_sql(reference_time, hinted=(base_bundle_id in hinted_a))
    return bundle.render_sql(reference_time)


def bundle_params(
    bundle,
    reference_time: datetime,
    bindings: dict[str, Any],
    preagg_bundles: set[str],
    preagg_layout: str = "bundle",
) -> tuple[Any, ...]:
    if bundle.bundle_id not in preagg_bundles:
        return tuple(bindings.get(name) for name in bundle.param_names)

    key_values = tuple(
        None if bindings.get(k) is None else str(bindings.get(k))
        for k in key_fields(bundle)
    )
    reference_value = reference_time.strftime("%Y-%m-%d %H:%M:%S")
    group = bundle.bundle_id.split("_bundle_", 1)[0].split("_", 1)[1].upper()
    rollups, distincts = bundle_rollup_metrics(group, bundle)
    params: list[Any] = []
    if preagg_layout == "prod180":
        # prod180 embeds the exact timestamp cutoff in SQL. Each distinct path
        # has one helper-table key predicate and one raw-boundary key predicate;
        # filtered distincts also emit a companion presence-count path.
        if rollups == [] and all(not tmpl.extra_predicate for tmpl in bundle.templates):
            params.extend(key_values)
            params.extend(key_values)
            return tuple(params)
        if group == "C" and rollups and distincts and all(not tmpl.extra_predicate for tmpl in bundle.templates):
            # Mixed rollup+distinct Group C prod180 SQL uses three shared CTE
            # predicates: raw_boundary, rollup helper, and distinct helper.
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
    distinct_count = sum(
        1 for tmpl in bundle.templates
        if tmpl.select_expr.strip().upper().startswith("COUNT(DISTINCT")
    )
    for _ in range(distinct_count):
        params.extend(key_values)
        params.append(reference_value)
    if rollups:
        params.extend(key_values)
        params.append(reference_value)
    return tuple(params)


def run_one_event_detailed(
    pool: Queue,
    all_bundles,
    hinted_a: set[str],
    preagg_bundles: set[str],
    preagg_layout: str,
    event: dict[str, Any],
    bundle_executor: ThreadPoolExecutor | None = None,
    store_bundle_results: bool = True,
) -> dict[str, Any]:
    bindings = event["bindings"]
    reference_time = datetime.fromisoformat(event["reference_time"])

    def run_bundle(bundle, group: str, queued_at: float | None = None) -> dict[str, Any]:
        worker_started = time.perf_counter()
        task_queue_ms = ((worker_started - queued_at) * 1000.0) if queued_at is not None else 0.0
        sql = render_bundle_sql(bundle, group, reference_time, hinted_a, preagg_bundles, preagg_layout)
        params = bundle_params(bundle, reference_time, bindings, preagg_bundles, preagg_layout)
        conn_wait_started = time.perf_counter()
        conn = pool.get()
        conn_wait_ms = (time.perf_counter() - conn_wait_started) * 1000.0
        started = time.perf_counter()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cur.fetchall()
            return {
                "bundle_id": bundle.bundle_id,
                "group": group,
                "window_days": getattr(bundle, "window_days", None),
                "base_filter": getattr(bundle, "base_filter", None),
                "preagg_applied": bundle.bundle_id in preagg_bundles,
                "ms": (time.perf_counter() - started) * 1000.0,
                "task_queue_ms": task_queue_ms,
                "conn_wait_ms": conn_wait_ms,
            }
        except Exception as exc:
            return {
                "bundle_id": bundle.bundle_id,
                "group": group,
                "window_days": getattr(bundle, "window_days", None),
                "base_filter": getattr(bundle, "base_filter", None),
                "preagg_applied": bundle.bundle_id in preagg_bundles,
                "ms": -1.0,
                "task_queue_ms": task_queue_ms,
                "conn_wait_ms": conn_wait_ms,
                "error": str(exc)[:300],
            }
        finally:
            pool.put(conn)

    event_start = time.perf_counter()
    if bundle_executor is None:
        with ThreadPoolExecutor(max_workers=len(all_bundles)) as ex:
            bundle_results = list(ex.map(lambda item: run_bundle(*item), all_bundles))
    else:
        futures = [
            bundle_executor.submit(run_bundle, bundle, group, time.perf_counter())
            for bundle, group in all_bundles
        ]
        bundle_results = [future.result() for future in futures]
    event_ms = (time.perf_counter() - event_start) * 1000.0
    successful = [b for b in bundle_results if b["ms"] >= 0]
    bundle_ms = [float(b["ms"]) for b in successful]
    task_queue_ms = [float(b.get("task_queue_ms", 0.0)) for b in bundle_results]
    conn_wait_ms = [float(b.get("conn_wait_ms", 0.0)) for b in bundle_results]
    slowest = sorted(successful, key=lambda b: b["ms"], reverse=True)[:8]
    cutoff_counts = {
        str(cutoff): sum(1 for b in successful if float(b["ms"]) <= cutoff)
        for cutoff in (50, 100, 150, 200, 350, 500)
    }
    return {
        "event": event["invoice_number"],
        "kind": event["kind"],
        "hot_field": event.get("hot_field"),
        "hot_count": event.get("hot_count"),
        "ms": event_ms,
        "ts": time.time(),
        "error_count": len(bundle_results) - len(successful),
        "bundle_avg_ms": statistics.mean(bundle_ms) if bundle_ms else 0.0,
        "bundle_max_ms": max(bundle_ms) if bundle_ms else 0.0,
        "bundle_task_queue_avg_ms": statistics.mean(task_queue_ms) if task_queue_ms else 0.0,
        "bundle_task_queue_max_ms": max(task_queue_ms) if task_queue_ms else 0.0,
        "bundle_conn_wait_avg_ms": statistics.mean(conn_wait_ms) if conn_wait_ms else 0.0,
        "bundle_conn_wait_max_ms": max(conn_wait_ms) if conn_wait_ms else 0.0,
        "slowest_bundles": slowest,
        "bundle_counts_by_cutoff": cutoff_counts,
        "bundle_results_omitted": not store_bundle_results,
        "bundle_results": bundle_results if store_bundle_results else [],
    }


def pop_rotating(events: list[dict[str, Any]], queue: list[dict[str, Any]]) -> dict[str, Any]:
    if not queue:
        queue.extend(events)
        random.shuffle(queue)
    return queue.pop()


def pop_unique(queue: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not queue:
        return None
    return queue.pop()


def reader_thread(
    stop_evt,
    pool,
    all_bundles,
    hinted_a,
    preagg_bundles,
    preagg_layout,
    normal_events,
    hot_events,
    hot_event_pct,
    target_rate,
    results,
    event_executor: ThreadPoolExecutor,
    bundle_executor: ThreadPoolExecutor,
    event_semaphore: threading.Semaphore,
    unique_events_required: bool,
    store_bundle_results: bool,
    reader_stats: dict[str, Any],
):
    interval = 1.0 / target_rate
    next_fire = time.monotonic()
    normal_queue: list[dict[str, Any]] = list(normal_events) if unique_events_required else []
    hot_queue: list[dict[str, Any]] = list(hot_events) if unique_events_required else []
    random.shuffle(normal_queue)
    random.shuffle(hot_queue)
    submitted = 0
    while not stop_evt.is_set():
        now = time.monotonic()
        if now >= next_fire:
            prefer_hot = random.random() < hot_event_pct
            if unique_events_required:
                event = pop_unique(hot_queue if prefer_hot else normal_queue)
                if event is None:
                    reader_stats["fallback_events"] = int(reader_stats.get("fallback_events", 0)) + 1
                    event = pop_unique(normal_queue if prefer_hot else hot_queue)
                if event is None:
                    reader_stats["event_sample_exhausted"] = True
                    stop_evt.set()
                    break
            else:
                event = pop_rotating(hot_events, hot_queue) if prefer_hot else pop_rotating(normal_events, normal_queue)

            if not event_semaphore.acquire(timeout=1.0):
                reader_stats["backpressure_skips"] = int(reader_stats.get("backpressure_skips", 0)) + 1
                next_fire += interval
                continue

            def _run(ev=event):
                try:
                    results.append(
                        run_one_event_detailed(
                            pool,
                            all_bundles,
                            hinted_a,
                            preagg_bundles,
                            preagg_layout,
                            ev,
                            bundle_executor=bundle_executor,
                            store_bundle_results=store_bundle_results,
                        )
                    )
                finally:
                    event_semaphore.release()

            event_executor.submit(_run)
            submitted += 1
            next_fire += interval
        else:
            time.sleep(min(0.005, next_fire - now))
    reader_stats["submitted_events"] = submitted


def print_summary(label: str, rows: list[dict[str, Any]]) -> None:
    vals = [float(r["ms"]) for r in rows]
    s = summarize(vals)
    if not s["n"]:
        print(f"{label}: n=0")
        return
    print(
        f"{label}: n={s['n']} p50={s['p50']:.1f} p95={s['p95']:.1f} "
        f"p99={s['p99']:.1f} max={s['max']:.1f} "
        f">350={s['over_350']} >500={s['over_500']}"
    )


def bundle_coverage_summary(rows: list[dict[str, Any]], bundle_count: int) -> dict[str, Any]:
    cutoffs = [50, 100, 150, 200, 350, 500]
    if not rows:
        return {"events": 0, "bundle_count": bundle_count}

    counts_by_cutoff: dict[int, list[int]] = {cutoff: [] for cutoff in cutoffs}
    for row in rows:
        counts = row.get("bundle_counts_by_cutoff", {})
        for cutoff in cutoffs:
            counts_by_cutoff[cutoff].append(int(counts.get(str(cutoff), 0)))

    by_cutoff = {
        str(cutoff): {
            "avg": statistics.mean(vals),
            "median": percentile([float(v) for v in vals], 50),
            "events_ge_60": sum(1 for v in vals if v >= 60),
            "events_65": sum(1 for v in vals if v >= bundle_count),
        }
        for cutoff, vals in counts_by_cutoff.items()
    }

    windows = [
        ("0-50ms", 0, 50),
        ("50-100ms", 50, 100),
        ("100-150ms", 100, 150),
        ("150-200ms", 150, 200),
        ("200-350ms", 200, 350),
        ("350-500ms", 350, 500),
    ]
    histogram = []
    for label, lo, hi in windows:
        hi_vals = counts_by_cutoff[hi]
        if lo == 0:
            window_counts = hi_vals
        else:
            window_counts = [
                hi_count - lo_count
                for hi_count, lo_count in zip(hi_vals, counts_by_cutoff[lo], strict=True)
            ]
        histogram.append(
            {
                "window": label,
                "avg_bundles_per_event": statistics.mean(window_counts),
                "total_bundle_executions": sum(window_counts),
            }
        )

    over_500_or_error = [
        bundle_count - int(row.get("bundle_counts_by_cutoff", {}).get("500", 0))
        for row in rows
    ]
    histogram.append(
        {
            "window": ">500/error",
            "avg_bundles_per_event": statistics.mean(over_500_or_error),
            "total_bundle_executions": sum(over_500_or_error),
        }
    )
    return {
        "events": len(rows),
        "bundle_count": bundle_count,
        "by_cutoff": by_cutoff,
        "histogram": histogram,
    }


def fanout_capacity_summary(
    read_rate: float,
    bundle_count: int,
    pool_size: int,
    bundle_workers: int,
    event_workers: int,
    max_pending_events: int,
) -> dict[str, Any]:
    target_bundle_qps = read_rate * bundle_count
    configured_bundle_slots = min(pool_size, bundle_workers)
    required_slots_350 = math.ceil(target_bundle_qps * 0.350)
    required_slots_500 = math.ceil(target_bundle_qps * 0.500)
    return {
        "model": "independent fan-out/fan-in: all bundle queries for an event share bindings/reference_time and can run in parallel",
        "event_qps": read_rate,
        "bundles_per_event": bundle_count,
        "target_bundle_qps": target_bundle_qps,
        "pool_size": pool_size,
        "bundle_workers": bundle_workers,
        "event_workers": event_workers,
        "max_pending_events": max_pending_events,
        "configured_bundle_slots": configured_bundle_slots,
        "max_events_fully_fanned_out_by_client": configured_bundle_slots // bundle_count if bundle_count else 0,
        "required_bundle_slots_if_queries_run_350ms": required_slots_350,
        "required_bundle_slots_if_queries_run_500ms": required_slots_500,
        "configured_vs_350ms_requirement_pct": (configured_bundle_slots / required_slots_350 * 100.0) if required_slots_350 else 0.0,
        "configured_vs_500ms_requirement_pct": (configured_bundle_slots / required_slots_500 * 100.0) if required_slots_500 else 0.0,
    }


def print_bundle_coverage(summary: dict[str, Any]) -> None:
    events = int(summary.get("events", 0))
    bundle_count = int(summary.get("bundle_count", 65))
    if events <= 0:
        print("Bundle coverage: no events")
        return
    by_cutoff = summary["by_cutoff"]
    print()
    print("Bundle coverage:")
    for cutoff in (350, 500):
        row = by_cutoff[str(cutoff)]
        print(
            f"  >=60/{bundle_count} by {cutoff}ms: {row['events_ge_60']}/{events} "
            f"({row['events_ge_60'] / events:.1%})"
        )
        print(
            f"  {bundle_count}/{bundle_count} by {cutoff}ms: {row['events_65']}/{events} "
            f"({row['events_65'] / events:.1%})"
        )
        print(f"  Avg bundles by {cutoff}ms: {row['avg']:.1f}/{bundle_count}")

    print()
    print("Bundle return-time drop-off:")
    print("  Window       Avg bundles/event   Total bundle executions")
    for item in summary["histogram"]:
        print(
            f"  {item['window']:<11s} {item['avg_bundles_per_event']:>8.2f}"
            f"              {item['total_bundle_executions']:>10,}"
        )


def print_fanout_capacity(summary: dict[str, Any]) -> None:
    bundle_count = int(summary["bundles_per_event"])
    print()
    print("Fan-out capacity model:")
    print(
        f"  Target: {summary['event_qps']:.1f} events/sec * {bundle_count} bundles/event "
        f"= {summary['target_bundle_qps']:.1f} bundle SQL/sec"
    )
    print(
        f"  Client bundle slots: min(pool_size={summary['pool_size']}, "
        f"bundle_workers={summary['bundle_workers']}) = {summary['configured_bundle_slots']}"
    )
    print(
        f"  Fully fanned-out events possible at once by client slots: "
        f"{summary['max_events_fully_fanned_out_by_client']} events"
    )
    print(
        f"  Slots needed if every bundle occupies a slot for 350ms: "
        f"{summary['required_bundle_slots_if_queries_run_350ms']} "
        f"({summary['configured_vs_350ms_requirement_pct']:.1f}% configured)"
    )
    print(
        f"  Slots needed if every bundle occupies a slot for 500ms: "
        f"{summary['required_bundle_slots_if_queries_run_500ms']} "
        f"({summary['configured_vs_500ms_requirement_pct']:.1f}% configured)"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--read-rate", type=float, default=3.0)
    ap.add_argument("--hot-event-pct", type=float, default=0.10)
    ap.add_argument("--normal-events", type=int, default=80)
    ap.add_argument("--hot-events-per-field", type=int, default=1)
    ap.add_argument("--fast-normal-sampling", action="store_true", help="Sample normal events by excluding top hot values, without per-event count validation.")
    ap.add_argument("--max-normal-payment-rows", type=int, default=10000)
    ap.add_argument("--max-normal-device-rows", type=int, default=10000)
    ap.add_argument("--pool-size", type=int, default=300)
    ap.add_argument("--write-pool-size", type=int, default=25)
    ap.add_argument("--event-workers", type=int, default=0, help="Bound concurrent event executions. 0 chooses a rate-based default.")
    ap.add_argument("--bundle-workers", type=int, default=0, help="Bound concurrent bundle SQL executions. 0 defaults to --pool-size.")
    ap.add_argument("--max-pending-events", type=int, default=0, help="Backpressure limit for queued/running events. 0 defaults to event_workers * 2.")
    ap.add_argument("--unique-events-required", action="store_true", help="Do not rotate/reuse sampled events. Fail early if the sample is too small.")
    ap.add_argument("--summary-only", action="store_true", help="Omit per-bundle details from each event to keep high-rate result JSONs small.")
    ap.add_argument("--read-max-execution-time-ms", type=int, default=0, help="Set TiDB max_execution_time on read connections. 0 means unlimited.")
    ap.add_argument("--no-writes", action="store_true")
    ap.add_argument("--preagg-mode", choices=["hybrid", "runtime-only"], default="hybrid", help="hybrid uses --preagg-bundle paths; runtime-only ignores pre-agg bundles.")
    ap.add_argument("--preagg-bundle", action="append", default=[], help="Use daily pre-aggregation for this bundle id. Repeatable.")
    ap.add_argument("--preagg-layout", choices=["bundle", "prod180"], default=os.getenv("PREAGG_LAYOUT", "prod180"), help="Pre-agg physical layout to use for selected bundles.")
    ap.add_argument("--skip-initial-warmup", action="store_true", help="Do not run the one normal + one hot preflight event before timing.")
    ap.add_argument("--reuse-events-json", default=None, help="Reuse sampled_normal_events and sampled_hot_events from a prior mixed_traffic JSON run.")
    args = ap.parse_args()

    random.seed(20260508)
    print(
        f"Mixed traffic test: duration={args.duration}s read_rate={args.read_rate}/s "
        f"hot_event_pct={args.hot_event_pct:.2%}"
    )

    if args.reuse_events_json:
        reused = json.loads((ROOT / args.reuse_events_json).read_text(encoding="utf-8"))
        normal_events = reused.get("sampled_normal_events", [])[: args.normal_events]
        hot_events = reused.get("sampled_hot_events", [])
        profile = reused.get("profile", {"hot_fields": {}})
        print(f"Reusing sampled events from {args.reuse_events_json}")
    else:
        normal_events, hot_events, profile = sample_mixed_events(
            args.normal_events,
            hot_events_per_field=args.hot_events_per_field,
            max_payment_rows=args.max_normal_payment_rows,
            max_device_rows=args.max_normal_device_rows,
            validate_normal_counts=not args.fast_normal_sampling,
        )
    print(f"Sampled {len(normal_events)} normal events and {len(hot_events)} hot-key events")
    target_events = math.ceil(args.duration * args.read_rate)
    expected_hot_events = math.ceil(target_events * args.hot_event_pct)
    expected_normal_events = target_events - expected_hot_events
    if args.unique_events_required:
        if len(normal_events) < expected_normal_events or len(hot_events) < expected_hot_events:
            raise RuntimeError(
                "Unique-event run does not have enough sampled events. "
                f"Need about {expected_normal_events} normal + {expected_hot_events} hot "
                f"for {target_events} target events, sampled {len(normal_events)} normal + {len(hot_events)} hot. "
                "Increase NORMAL_EVENTS/HOT_EVENTS_PER_FIELD or lower READ_RATE/DURATION/HOT_EVENT_PCT."
            )
        print(
            f"Unique-event mode enabled: target_events={target_events}, "
            f"expected_normal={expected_normal_events}, expected_hot={expected_hot_events}"
        )
    for field, info in profile["hot_fields"].items():
        print(f"  hot {field:34s} {info['value']} rows={info['count']}")

    a_bundles = cluster_group_a_templates()
    b_bundles = cluster_group_b_templates()
    c_bundles = cluster_group_c_templates()
    all_bundles = [(b, "A") for b in a_bundles] + [(b, "B") for b in b_bundles] + [(b, "C") for b in c_bundles]
    # Production-style baseline: let TiDB choose TiKV vs TiFlash by cost.
    # Older fixed-event demo runs forced TiFlash for a couple of Group A routing
    # bundles, but rotating-key traffic should not inherit one-off hints.
    hinted_a: set[str] = set()
    if args.preagg_mode == "hybrid":
        preagg_bundles = set(args.preagg_bundle)
        if not preagg_bundles and args.preagg_layout == "prod180":
            preagg_bundles = set(PROD180_PREAGG_BUNDLES)
    else:
        preagg_bundles = set()
    print(f"Bundles per event: {len(all_bundles)}")
    print(f"Pre-agg mode: {args.preagg_mode}")
    print(f"Pre-agg layout: {args.preagg_layout}")
    if preagg_bundles:
        print(f"Pre-agg bundles: {', '.join(sorted(preagg_bundles))}")

    print(f"Building read pool ({args.pool_size})...")
    read_timeout_ms = args.read_max_execution_time_ms or None
    if read_timeout_ms:
        print(f"Read connection max_execution_time={read_timeout_ms}ms")
    read_pool = make_pool(args.pool_size, max_execution_time_ms=read_timeout_ms)
    write_pool = None
    if not args.no_writes:
        print(f"Building write pool ({args.write_pool_size})...")
        write_pool = make_pool(args.write_pool_size)

    if args.skip_initial_warmup:
        print("Skipping initial preflight event.")
    else:
        print("Running one normal and one hot preflight event...")
        run_one_event_detailed(read_pool, all_bundles, hinted_a, preagg_bundles, args.preagg_layout, normal_events[0])
        run_one_event_detailed(read_pool, all_bundles, hinted_a, preagg_bundles, args.preagg_layout, hot_events[0])

    stop_evt = threading.Event()
    read_results: list[dict[str, Any]] = []
    write_results: list[dict[str, Any]] = []
    threads: list[threading.Thread] = []
    reader_stats: dict[str, Any] = {"event_sample_exhausted": False, "fallback_events": 0, "submitted_events": 0, "backpressure_skips": 0}
    event_workers = args.event_workers or max(32, min(4096, math.ceil(args.read_rate * 4) + 64))
    bundle_workers = args.bundle_workers or args.pool_size
    max_pending_events = args.max_pending_events or event_workers * 2
    event_semaphore = threading.Semaphore(max_pending_events)
    print(
        f"Client workers: event_workers={event_workers} bundle_workers={bundle_workers} "
        f"max_pending_events={max_pending_events} summary_only={args.summary_only}"
    )
    fanout_capacity = fanout_capacity_summary(
        args.read_rate,
        len(all_bundles),
        args.pool_size,
        bundle_workers,
        event_workers,
        max_pending_events,
    )
    print_fanout_capacity(fanout_capacity)
    event_executor = ThreadPoolExecutor(max_workers=event_workers)
    bundle_executor = ThreadPoolExecutor(max_workers=bundle_workers)

    reader = threading.Thread(
        target=reader_thread,
        args=(
            stop_evt,
            read_pool,
            all_bundles,
            hinted_a,
            preagg_bundles,
            args.preagg_layout,
            normal_events,
            hot_events,
            args.hot_event_pct,
            args.read_rate,
            read_results,
            event_executor,
            bundle_executor,
            event_semaphore,
            args.unique_events_required,
            not args.summary_only,
            reader_stats,
        ),
    )
    reader.start()
    threads.append(reader)

    if write_pool is not None:
        pmt_sql = (
            "INSERT INTO pmt_txn_fact "
            "(invoice_number, event_date, check_bank_routing_number, transaction_type, amount, parsed_interaction_id) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )

        def pmt_params():
            now_ms = int(time.time() * 1000)
            inv = f"INV_MIX_{int(time.time()*1e6)}_{random.randint(0,9999)}"
            return (inv, now_ms, "999000999", random.choice(["Sale", "Refund", "Capture"]), random.uniform(10, 500), f"mix_{int(time.time()*1e6)}")

        dev_sql = "INSERT INTO deviceprofile_fact (jms_timestamp, exact_id, smart_id, agent_type, interaction_id) VALUES (NOW(), %s, %s, %s, %s)"

        def dev_params():
            return (
                f"mix_e{random.randint(0, 1000000)}",
                f"mix_s{random.randint(0, 1000000)}",
                "browser_computer",
                f"mix_{int(time.time()*1e6)}",
            )

        for table, sql, params_fn, tps in (
            ("pmt_txn_fact", pmt_sql, pmt_params, 50.0),
            ("deviceprofile_fact", dev_sql, dev_params, 40.0),
        ):
            t = threading.Thread(target=writer_thread, args=(stop_evt, write_pool, table, sql, params_fn, tps, write_results))
            t.start()
            threads.append(t)

    test_start = time.time()
    print(f"Running mixed traffic for {args.duration}s...")
    time.sleep(args.duration)
    stop_evt.set()
    for t in threads:
        t.join(timeout=10)
    event_executor.shutdown(wait=True)
    bundle_executor.shutdown(wait=True)
    time.sleep(5)

    cutoff = test_start + args.warmup
    all_reads = list(read_results)
    steady_reads = [r for r in all_reads if r["ts"] >= cutoff]
    hot_reads = [r for r in steady_reads if r["kind"].startswith("hot_")]
    normal_reads = [r for r in steady_reads if r["kind"] == "normal"]

    print()
    print("=" * 80)
    print("MIXED TRAFFIC RESULTS")
    print("=" * 80)
    print_summary("All reads including first window", all_reads)
    print_summary("Steady reads", steady_reads)
    print_summary("Normal steady", normal_reads)
    print_summary("Hot-key steady", hot_reads)
    coverage = bundle_coverage_summary(steady_reads, len(all_bundles))
    print_bundle_coverage(coverage)
    print_fanout_capacity(fanout_capacity)
    print_summary(
        "Bundle task queue avg per event",
        [{"ms": float(r.get("bundle_task_queue_avg_ms", 0.0))} for r in steady_reads],
    )
    print_summary(
        "Bundle task queue max per event",
        [{"ms": float(r.get("bundle_task_queue_max_ms", 0.0))} for r in steady_reads],
    )
    print_summary(
        "DB connection wait avg per event",
        [{"ms": float(r.get("bundle_conn_wait_avg_ms", 0.0))} for r in steady_reads],
    )
    print_summary(
        "DB connection wait max per event",
        [{"ms": float(r.get("bundle_conn_wait_max_ms", 0.0))} for r in steady_reads],
    )
    achieved_rate = len(all_reads) / args.duration if args.duration else 0.0
    print(f"Target read rate: {args.read_rate:.1f}/s, achieved submitted-completed rate: {achieved_rate:.1f}/s")
    if achieved_rate < args.read_rate * 0.95:
        print(
            "WARNING: achieved rate is below 95% of target. "
            "This usually means the client workers/connections or cluster capacity are saturated."
        )
    if reader_stats.get("event_sample_exhausted"):
        print("WARNING: unique event sample was exhausted before duration completed.")
    if reader_stats.get("fallback_events"):
        print(f"Unique-event hot/normal fallback selections: {reader_stats['fallback_events']}")
    if reader_stats.get("backpressure_skips"):
        print(f"Client backpressure skips: {reader_stats['backpressure_skips']}")

    for kind in sorted({r["kind"] for r in steady_reads}):
        print_summary(f"{kind} steady", [r for r in steady_reads if r["kind"] == kind])

    slow_events = sorted(steady_reads, key=lambda r: r["ms"], reverse=True)[:10]
    print()
    print("Top 10 slowest steady events:")
    for idx, row in enumerate(slow_events, 1):
        slow_bundle = row["slowest_bundles"][0] if row.get("slowest_bundles") else {}
        print(
            f"{idx:2d}. {row['ms']:8.1f} ms {row['kind']:36s} "
            f"event={row['event']} slowest={slow_bundle.get('bundle_id')} "
            f"{slow_bundle.get('ms', 0):.1f}ms window={slow_bundle.get('window_days')}"
        )

    bundle_totals: dict[str, list[float]] = {}
    for row in steady_reads:
        for bundle in row.get("slowest_bundles", [])[:3]:
            bundle_totals.setdefault(bundle["bundle_id"], []).append(float(bundle["ms"]))
    print()
    print("Bundles most often appearing in top-3 slowest per event:")
    ranked = sorted(bundle_totals.items(), key=lambda kv: (len(kv[1]), max(kv[1])), reverse=True)[:15]
    for bundle_id, vals in ranked:
        print(f"  {bundle_id:22s} appearances={len(vals):4d} max={max(vals):8.1f} p95={percentile(vals, 95):8.1f}")

    result = {
        "test_start": test_start,
        "duration": args.duration,
        "warmup": args.warmup,
        "read_rate": args.read_rate,
        "achieved_read_rate": achieved_rate,
        "hot_event_pct": args.hot_event_pct,
        "hot_events_per_field": args.hot_events_per_field,
        "fast_normal_sampling": args.fast_normal_sampling,
        "unique_events_required": args.unique_events_required,
        "summary_only": args.summary_only,
        "event_workers": event_workers,
        "bundle_workers": bundle_workers,
        "max_pending_events": max_pending_events,
        "reader_stats": reader_stats,
        "read_max_execution_time_ms": args.read_max_execution_time_ms,
        "preagg_mode": args.preagg_mode,
        "preagg_layout": args.preagg_layout,
        "preagg_bundles": sorted(preagg_bundles),
        "fanout_capacity": fanout_capacity,
        "skip_initial_warmup": args.skip_initial_warmup,
        "query_shape_notes": {
            "event_bundle_dependency": "The 65 bundle queries for one event are independent; they use the same event bindings/reference_time and fan in only when the scoring app needs the combined feature vector.",
            "group_c_join_key": "p.parsed_interaction_id = d.interaction_id",
            "group_c_timestamp_filter": "v15 applies both p.event_date >= cutoff and d.jms_timestamp >= cutoff on runtime Group C joins",
            "payment_join_key_source": "risk_profile_token is modeled as <sessionID>:<interactionID>; parsed_interaction_id contains the suffix used for the join",
            "account_hash_population": "check_bank_account_number_sha512 is populated only for ACH/check rows with routing numbers",
        },
        "profile": profile,
        "sampled_normal_events": normal_events,
        "sampled_hot_events": hot_events,
        "read_results": all_reads,
        "write_results": write_results,
        "summaries": {
            "all_reads": summarize([float(r["ms"]) for r in all_reads]),
            "steady_reads": summarize([float(r["ms"]) for r in steady_reads]),
            "normal_steady": summarize([float(r["ms"]) for r in normal_reads]),
            "hot_steady": summarize([float(r["ms"]) for r in hot_reads]),
            "bundle_task_queue_avg_per_event": summarize(
                [float(r.get("bundle_task_queue_avg_ms", 0.0)) for r in steady_reads]
            ),
            "bundle_task_queue_max_per_event": summarize(
                [float(r.get("bundle_task_queue_max_ms", 0.0)) for r in steady_reads]
            ),
            "bundle_conn_wait_avg_per_event": summarize(
                [float(r.get("bundle_conn_wait_avg_ms", 0.0)) for r in steady_reads]
            ),
            "bundle_conn_wait_max_per_event": summarize(
                [float(r.get("bundle_conn_wait_max_ms", 0.0)) for r in steady_reads]
            ),
        },
        "bundle_coverage": coverage,
    }
    out = ROOT / "results" / f"mixed_traffic_{int(test_start)}.json"
    out.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
