#!/usr/bin/env python3
"""Convert a wide serving Go workload to a bucketed wide serving workload."""

from __future__ import annotations

import argparse
import json
import re
import time
import zlib
from pathlib import Path
from typing import Any


FROM_RE = re.compile(r"FROM `risk_feature_serving_wide` s")
WHERE_RE = re.compile(r"WHERE s\.as_of_grain = %s")
KEY2_PARAM_RE = re.compile(r"AND s\.key2 = %s;")


def bucket_for(bundle_id: str, key1: Any, key2: Any, bucket_count: int) -> int:
    raw = f"{bundle_id}#{'' if key1 is None else key1}#{'' if key2 is None else key2}"
    return (zlib.crc32(raw.encode("utf-8")) & 0xFFFFFFFF) % bucket_count


def rewrite_sql(sql: str, source_table: str, target_table: str) -> str:
    sql = sql.replace(f"FROM `{source_table}` s", f"FROM `{target_table}` s")
    if "WHERE s.bucket = %s" not in sql:
        sql = WHERE_RE.sub("WHERE s.bucket = %s\n  AND s.as_of_grain = %s", sql, count=1)
    return sql


def count_placeholders(sql: str) -> int:
    return sql.count("%s")


def bundle_key2(template_sql: str, params: list[Any]) -> Any:
    if KEY2_PARAM_RE.search(template_sql):
        if len(params) < 4:
            raise ValueError(f"template expects key2 param but params has {len(params)} values")
        return params[3]
    return ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-table", default="risk_feature_serving_wide")
    parser.add_argument("--target-table", default="risk_feature_serving_wide_b64")
    parser.add_argument("--bucket-count", type=int, default=64)
    args = parser.parse_args()

    workload = json.loads(args.input.read_text(encoding="utf-8"))
    templates = {t["bundle_id"]: t for t in workload["templates"]}

    for template in workload["templates"]:
        template["sql"] = rewrite_sql(template["sql"], args.source_table, args.target_table)

    for event in workload["events"]:
        for bundle in event["bundles"]:
            if bundle.get("skip"):
                continue
            params = bundle["params"]
            if len(params) < 3:
                raise ValueError(f"{bundle['bundle_id']} has too few params: {params!r}")
            template = templates[bundle["bundle_id"]]
            key1 = params[2]
            key2 = bundle_key2(template["sql"], params)
            bucket = bucket_for(bundle["bundle_id"], key1, key2, args.bucket_count)
            bundle["params"] = [bucket, *params]

    for template in workload["templates"]:
        expected = count_placeholders(template["sql"])
        for event in workload["events"][:10]:
            bundle = next(b for b in event["bundles"] if b["bundle_id"] == template["bundle_id"])
            if not bundle.get("skip") and len(bundle["params"]) != expected:
                raise ValueError(
                    f"{template['bundle_id']} placeholder/param mismatch: "
                    f"{expected} placeholders, {len(bundle['params'])} params"
                )

    workload["generated_at_unix"] = time.time()
    workload["mode"] = f"{workload.get('mode', 'bundle-serving')}-bucketed-wide"
    workload["serving_bucket_table"] = args.target_table
    workload["serving_bucket_count"] = args.bucket_count
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(workload, indent=2), encoding="utf-8")
    print(f"wrote {args.output} events={len(workload['events'])} bundles={len(workload['templates'])}")


if __name__ == "__main__":
    main()
