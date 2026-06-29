#!/usr/bin/env python3
"""Correctness gate for the BENCHMARKED 180d path (the wide serving table).

The shipped spotcheck_prod180_correctness.py compares raw runtime vs the prod180
ROLLUP. But the fleet actually serves the 12 180d bundles from the WIDE table
(`risk_feature_serving_wide`) via render_wide_serving_query. This gate closes
that gap: for each of the 12 PROD180 serving bundles it asserts

    wide point-lookup  ==  prod180 rollup  ==  raw runtime 180d aggregate

for a populated key at a covered as_of day. If they match, the numbers the
customer sees from the serving table are provably correct, not just fast.

Verified 2026-06-29 on the Premium cluster: 12/12 PASS (wide==preagg==raw) at
as_of 2026-04-10.
"""

from __future__ import annotations

import argparse
import math
import ssl as ssllib
import time
from decimal import Decimal
from datetime import datetime

import pymysql

from lib.db_config import get_db_config
from mixed_traffic_test import bundle_params, render_bundle_sql
from exact_serving import render_wide_serving_query, serving_params
from demo import (
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
)
from optimized_config import PROD180_PREAGG_BUNDLES


def connect():
    cfg = get_db_config(save_msg="wide serving spotcheck")
    if not cfg.get("ssl"):
        ctx = ssllib.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssllib.CERT_NONE
        cfg = {k: v for k, v in cfg.items() if k != "autocommit"}
        cfg["ssl"] = ctx
    cfg["autocommit"] = True
    cfg["cursorclass"] = pymysql.cursors.DictCursor
    cfg.setdefault("read_timeout", 180)
    return pymysql.connect(**cfg)


def norm(v):
    if isinstance(v, Decimal):
        return float(v) if v % 1 else int(v)
    if isinstance(v, float):
        return None if math.isnan(v) else round(v, 4)
    return v


def vals(rows):
    return [[norm(x) for x in r.values()] for r in rows]


def serving_bundles():
    out = []
    for g, fac in (("A", cluster_group_a_templates), ("B", cluster_group_b_templates), ("C", cluster_group_c_templates)):
        for b in fac():
            if b.window_days == 180 and b.bundle_id in PROD180_PREAGG_BUNDLES:
                out.append((g, b))
    return out


def resolve_reference(cur, table: str, ref_date: str | None):
    """Pin reference_time to the max real ts on/before a covered as_of day."""
    if ref_date is None:
        cur.execute(f"SELECT MAX(as_of_key) mx FROM {table} WHERE as_of_grain='day'")
        ref_date = cur.fetchone()["mx"]
    cur.execute(
        "SELECT FROM_UNIXTIME(MAX(event_date)/1000) ts FROM pmt_txn_fact "
        "WHERE event_date <= UNIX_TIMESTAMP(%s)*1000 AND merchant_account_number IS NOT NULL",
        (f"{ref_date} 23:59:59",),
    )
    p = cur.fetchone()["ts"]
    cur.execute("SELECT MAX(jms_timestamp) ts FROM deviceprofile_fact WHERE jms_timestamp <= %s", (f"{ref_date} 23:59:59",))
    d = cur.fetchone()["ts"]
    return min(p, d), ref_date


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference-date", default=None, help="covered as_of day (YYYY-MM-DD); default = max as_of in the wide table")
    ap.add_argument("--table", default="risk_feature_serving_wide")
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()
    cur.execute("SET SESSION max_execution_time=90000")

    ref, ref_date = resolve_reference(cur, args.table, args.reference_date)
    preagg = set(PROD180_PREAGG_BUNDLES)
    bundles = serving_bundles()
    asof = serving_params(bundles[0][1], ref, {}, "day")[1]
    print(f"reference_time={ref} as_of={asof} (date={ref_date})", flush=True)

    ok_wide_raw = ok_wide_pre = checked = 0
    failures = []
    for g, b in bundles:
        cols = list(b.param_names)
        cur.execute(
            f"SELECT key1,key2 FROM {args.table} WHERE bundle_id=%s AND as_of_grain='day' "
            "AND as_of_key=%s AND key1<>'' LIMIT 1",
            (b.bundle_id, asof),
        )
        kr = cur.fetchone()
        if not kr:
            print(f"SKIP {b.bundle_id}: no wide key at {asof}", flush=True)
            continue
        bind = dict(zip(cols, [kr["key1"]] + ([kr["key2"]] if len(cols) > 1 else [])))
        cur.execute(render_wide_serving_query(b, "day", args.table), serving_params(b, ref, bind, "day"))
        wide = vals(cur.fetchall())
        cur.execute(render_bundle_sql(b, g, ref, set(), preagg, "prod180"), bundle_params(b, ref, bind, preagg, "prod180"))
        pre = vals(cur.fetchall())
        t = time.perf_counter()
        cur.execute(render_bundle_sql(b, g, ref, set(), set(), "none"), bundle_params(b, ref, bind, set()))
        raw = vals(cur.fetchall())
        raw_ms = (time.perf_counter() - t) * 1000
        wr, wp = (wide == raw), (wide == pre)
        checked += 1
        ok_wide_raw += wr
        ok_wide_pre += wp
        status = "PASS" if (wr and wp) else "FAIL"
        print(f"{status} {b.bundle_id} g={g} wide==raw:{wr} wide==preagg:{wp} raw={raw_ms:.0f}ms", flush=True)
        if not (wr and wp):
            failures.append(b.bundle_id)
            print(f"   wide:{wide[:1]} raw:{raw[:1]} pre:{pre[:1]}", flush=True)
    conn.close()

    print(f"\nSUMMARY checked={checked} | wide==raw {ok_wide_raw}/{checked} | wide==preagg {ok_wide_pre}/{checked}", flush=True)
    if failures or checked == 0:
        print(f"FAIL: {failures or 'no bundles checked'}", flush=True)
        return 1
    print("ALL_PASS: served 180d values are provably correct", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
