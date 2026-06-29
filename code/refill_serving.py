#!/usr/bin/env python3
"""One-shot: (re)build the wide serving table for a run pool.

The wide serving table (`risk_feature_serving_wide`) stores precomputed 180d
aggregates only for the (bundle, key, as_of-date) combos a pool references. So it
must be rebuilt from the SAME pool the benchmark run uses, or a fresh pool hits
dates the table doesn't have (empty serving lookups). This driver does exactly
that for the 12 PROD180 serving bundles.

Forces TLS, pins reads to TiKV (so the 180d aggregation never gets pushed to
TiFlash and OOMs), and builds in parallel.

Measured build rate (single worker, scaled-in): ~14 rows/sec / ~71ms/row. With
--workers 8 that is ~5-6 min for ~30 days of an average pool; faster on a
scaled-up cluster. No OOM (TiKV-pinned).

Usage (run at scale-up, with the run's pool):
    python3 refill_serving.py --pool results/reuse_events_1000eps_5m.json --workers 8
Then verify:
    python3 serving_coverage_check.py --pool results/reuse_events_1000eps_5m.json --min-coverage 0.99
    python3 spotcheck_wide_serving.py

For a bounded end-to-end proof first, add --limit-keys 500 and --table risk_feature_serving_wide_test.
"""

from __future__ import annotations

import argparse
import json
import ssl as ssllib
import time
from pathlib import Path

import pymysql

import exact_serving as ES
from exact_serving import collect_serving_keys, build_serving_rows, configure_session
from optimized_config import PROD180_PREAGG_BUNDLES
from lib.db_config import get_db_config


def tls_conn():
    """Builder connection: force TLS (no-verify) when the saved config omits it,
    and pin reads to TiKV so the 180d aggregation cannot be routed to TiFlash."""
    cfg = get_db_config(save_msg="serving refill")
    if not cfg.get("ssl"):
        ctx = ssllib.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssllib.CERT_NONE
        cfg = {k: v for k, v in cfg.items() if k != "autocommit"}
        cfg["ssl"] = ctx
    conn = pymysql.connect(**cfg)
    conn.autocommit(True)
    configure_session(conn)
    with conn.cursor() as cur:
        cur.execute("SET SESSION tidb_isolation_read_engines='tikv'")
        cur.execute("SET SESSION max_execution_time=120000")
    return conn


# Workers in build_serving_rows open their own connections via connect_for_builder;
# point that at the TLS + TiKV-pinned connection.
ES.connect_for_builder = tls_conn


def load_events(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("events", "workload", "rows"):
            if isinstance(data.get(key), list):
                return data[key]
        # reuse-events pool format (build_reuse_events_from_stats): events are split
        # across sampled_normal_events + sampled_hot_events, each with reference_time+bindings.
        combined = []
        for key in ("sampled_normal_events", "sampled_hot_events"):
            if isinstance(data.get(key), list):
                combined.extend(data[key])
        if combined:
            return combined
    raise SystemExit(f"could not find an event list in {path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True, type=Path, help="run pool JSON (reuse-events or go-workload)")
    ap.add_argument("--table", default="risk_feature_serving_wide")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit-keys", type=int, default=None, help="cap rows (bounded proof run)")
    args = ap.parse_args()

    events = load_events(args.pool)
    bundle_ids = sorted(PROD180_PREAGG_BUNDLES)
    keys = collect_serving_keys(events, bundle_ids, "day", args.limit_keys)
    print(f"building {len(keys):,} serving rows for {len(bundle_ids)} PROD180 bundles "
          f"into {args.table}, workers={args.workers}", flush=True)

    conn = tls_conn()
    started = time.time()
    build_serving_rows(conn, keys, args.table, workers=args.workers, layout="wide")
    elapsed = time.time() - started
    rate = len(keys) / elapsed if elapsed else 0.0
    print(f"\nDONE: {len(keys):,} rows in {elapsed:.0f}s ({rate:.0f} rows/sec). "
          f"Now run serving_coverage_check.py (expect >=99%) and spotcheck_wide_serving.py.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
