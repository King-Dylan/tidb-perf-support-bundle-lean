#!/usr/bin/env python3
"""Pre-run guard: verify the wide serving table covers every (bundle, key, as_of)
the event pool will look up.

Why this exists
---------------
The 12 180d bundles are served from `risk_feature_serving_wide` as single-row
point lookups keyed by (as_of_grain, as_of_key, bundle_id, key1, key2). Each
event's as_of_key is that event's OWN date (mixed_traffic_test stamps
reference_time = event_date). If the serving table has no row for an event's
(bundle, key, date), the lookup returns ZERO ROWS. The Go harness currently
treats a zero-row response as a fast success, so an uncovered event silently
reports 12 hollow "instant successes" and inflates the 65/65 headline.

This check reads a generated event pool (reuse-events JSON or go-workload JSON),
replays the serving lookups for the PROD180 serving bundles, and reports the
real coverage. It exits non-zero if coverage is below --min-coverage so a stale
serving table cannot silently inflate a customer-facing run.

Run AFTER building the pool and AFTER (re)building the wide serving table, and
BEFORE the full fleet run. See FINAL_RUN_CHECKLIST.md.
"""

from __future__ import annotations

import argparse
import json
import ssl as ssllib
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pymysql

from lib.db_config import get_db_config
from demo import (
    cluster_group_a_templates,
    cluster_group_b_templates,
    cluster_group_c_templates,
)
from optimized_config import PROD180_PREAGG_BUNDLES


def connect():
    cfg = get_db_config(save_msg="serving coverage check")
    # TiDB Cloud requires TLS; force it (no-verify) when the saved config omits ssl.
    if not cfg.get("ssl"):
        ctx = ssllib.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssllib.CERT_NONE
        cfg = {k: v for k, v in cfg.items() if k != "autocommit"}
        cfg["ssl"] = ctx
    cfg["autocommit"] = True
    cfg["cursorclass"] = pymysql.cursors.DictCursor
    cfg.setdefault("read_timeout", 120)
    return pymysql.connect(**cfg)


def serving_bundles():
    out = []
    for fac in (cluster_group_a_templates, cluster_group_b_templates, cluster_group_c_templates):
        for b in fac():
            if b.window_days == 180 and b.bundle_id in PROD180_PREAGG_BUNDLES:
                out.append(b)
    return out


def load_events(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    # Accept either a reuse-events pool or a go-workload; both carry per-event
    # bindings + reference_time.
    if isinstance(data, dict):
        for key in ("events", "workload", "rows"):
            if key in data and isinstance(data[key], list):
                return data[key]
    if isinstance(data, list):
        return data
    raise SystemExit(f"could not find an event list in {path}")


def as_of_key(ev: dict) -> str:
    ref = ev.get("reference_time")
    if ref:
        return datetime.fromisoformat(ref).date().isoformat()
    # fall back to event_date in ms
    ed = ev.get("event_date") or ev.get("bindings", {}).get("event_date")
    if ed is None:
        raise SystemExit("event missing reference_time/event_date")
    return datetime.fromtimestamp(int(ed) / 1000).date().isoformat()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True, type=Path, help="reuse-events or go-workload JSON")
    ap.add_argument("--sample", type=int, default=2000, help="max events to check (sampled head); 0 = all")
    ap.add_argument("--min-coverage", type=float, default=0.99, help="fail if served-lookup hit rate is below this")
    ap.add_argument("--table", default="risk_feature_serving_wide")
    args = ap.parse_args()

    bundles = serving_bundles()
    if len(bundles) != 12:
        print(f"WARN expected 12 PROD180 serving bundles, found {len(bundles)}", flush=True)

    events = load_events(args.pool)
    if args.sample and len(events) > args.sample:
        events = events[: args.sample]
    print(f"checking {len(events)} events x {len(bundles)} serving bundles against {args.table}", flush=True)

    conn = connect()
    cur = conn.cursor()
    cur.execute("SET SESSION max_execution_time=30000")

    hit = miss = 0
    miss_by_bundle: dict[str, int] = defaultdict(int)
    events_with_miss = 0
    for ev in events:
        bindings = ev.get("bindings", ev)
        a = as_of_key(ev)
        ev_bad = False
        for b in bundles:
            pn = b.param_names
            k1 = "" if bindings.get(pn[0]) is None else str(bindings.get(pn[0]))
            k2 = str(bindings.get(pn[1])) if len(pn) > 1 and bindings.get(pn[1]) is not None else ""
            cur.execute(
                f"SELECT 1 FROM {args.table} WHERE as_of_grain='day' AND as_of_key=%s "
                f"AND bundle_id=%s AND key1=%s AND key2=%s LIMIT 1",
                (a, b.bundle_id, k1, k2),
            )
            if cur.fetchone():
                hit += 1
            else:
                miss += 1
                miss_by_bundle[b.bundle_id] += 1
                ev_bad = True
        if ev_bad:
            events_with_miss += 1
    conn.close()

    total = hit + miss
    cov = hit / total if total else 0.0
    print(f"\nSERVING COVERAGE: {hit}/{total} lookups hit = {cov*100:.2f}%")
    print(f"events with >=1 serving miss: {events_with_miss}/{len(events)} ({100*events_with_miss/max(1,len(events)):.2f}%)")
    if miss_by_bundle:
        print("misses by bundle:", dict(miss_by_bundle))
    print(
        "\nNote: a serving miss = empty point-lookup, which the Go harness scores as a fast\n"
        "success unless main.go has the empty-serving guard. Below threshold means the wide\n"
        "table must be (re)built for this pool's date range (or the run pinned to covered days)."
    )
    if cov < args.min_coverage:
        print(f"\nFAIL: coverage {cov*100:.2f}% < required {args.min_coverage*100:.2f}%", flush=True)
        return 1
    print(f"\nPASS: coverage {cov*100:.2f}% >= required {args.min_coverage*100:.2f}%", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
