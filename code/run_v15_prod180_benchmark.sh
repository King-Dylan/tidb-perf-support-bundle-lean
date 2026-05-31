#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p logs results

READ_RATE="${1:-3}"
DURATION="${2:-300}"
LABEL="${LABEL:-v15_monthly_prod180_${READ_RATE}eps}"
EVENT_SAMPLE_MULTIPLIER="${EVENT_SAMPLE_MULTIPLIER:-1.10}"

export PREAGG_LAYOUT="${PREAGG_LAYOUT:-prod180}"
export PREAGG_MODE="${PREAGG_MODE:-hybrid}"
export TIDB_ISOLATION_READ_ENGINES="${TIDB_ISOLATION_READ_ENGINES:-tikv,tidb}"
export INTUIT_FORCE_INLINE_CTE="${INTUIT_FORCE_INLINE_CTE:-0}"
export TIFLASH_MPP_BUNDLE_IDS="${TIFLASH_MPP_BUNDLE_IDS:-group_b_bundle_012 group_c_bundle_018}"
export TIFLASH_MPP_ALL_EVENTS="${TIFLASH_MPP_ALL_EVENTS:-0}"
export READ_RATE
export READ_MAX_EXECUTION_TIME_MS="${READ_MAX_EXECUTION_TIME_MS:-500}"
export HOT_EVENT_PCT="${HOT_EVENT_PCT:-0.05}"
export PREAGG_BUNDLE_IDS="${PREAGG_BUNDLE_IDS:-group_a_bundle_017 group_a_bundle_018 group_a_bundle_019 group_a_bundle_020 group_b_bundle_017 group_b_bundle_018 group_b_bundle_019 group_b_bundle_020 group_c_bundle_022 group_c_bundle_023 group_c_bundle_024 group_c_bundle_025}"
export UNIQUE_EVENTS_REQUIRED="${UNIQUE_EVENTS_REQUIRED:-1}"

if [[ -z "${NORMAL_EVENTS:-}" || -z "${HOT_EVENTS_PER_FIELD:-}" ]]; then
  read -r AUTO_NORMAL_EVENTS AUTO_HOT_EVENTS_PER_FIELD AUTO_TOTAL_EVENTS <<< "$(
    python3 - "$READ_RATE" "$DURATION" "$HOT_EVENT_PCT" "$EVENT_SAMPLE_MULTIPLIER" <<'PY'
import math
import sys
read_rate = float(sys.argv[1])
duration = int(float(sys.argv[2]))
hot_pct = float(sys.argv[3])
mult = float(sys.argv[4])
total = math.ceil(read_rate * duration * mult)
hot = math.ceil(total * hot_pct)
normal = total - hot
hot_per_field = math.ceil(hot / 8)
print(normal, hot_per_field, total)
PY
  )"
  export NORMAL_EVENTS="${NORMAL_EVENTS:-$AUTO_NORMAL_EVENTS}"
  export HOT_EVENTS_PER_FIELD="${HOT_EVENTS_PER_FIELD:-$AUTO_HOT_EVENTS_PER_FIELD}"
  export TARGET_SAMPLE_EVENTS="$AUTO_TOTAL_EVENTS"
fi

if [[ -z "${POOL_SIZE:-}" ]]; then
  POOL_SIZE="$(
    python3 - "$READ_RATE" <<'PY'
import sys
r = float(sys.argv[1])
if r >= 1000:
    print(12000)
elif r >= 500:
    print(8000)
elif r >= 200:
    print(5000)
elif r >= 100:
    print(3000)
elif r >= 50:
    print(1500)
else:
    print(600)
PY
  )"
  export POOL_SIZE
fi

export BUNDLE_WORKERS="${BUNDLE_WORKERS:-$POOL_SIZE}"
export EVENT_WORKERS="${EVENT_WORKERS:-$(
  python3 - "$READ_RATE" <<'PY'
import math
import sys
r = float(sys.argv[1])
print(max(128, min(4096, math.ceil(r * 4) + 64)))
PY
)}"
export MAX_PENDING_EVENTS="${MAX_PENDING_EVENTS:-$(
  python3 - "$EVENT_WORKERS" <<'PY'
import sys
print(int(sys.argv[1]) * 2)
PY
)}"
export WRITE_POOL_SIZE="${WRITE_POOL_SIZE:-50}"

REQUIRED_NOFILE="$(
  python3 - "$POOL_SIZE" "$WRITE_POOL_SIZE" <<'PY'
import sys
print(int(sys.argv[1]) + int(sys.argv[2]) + 2048)
PY
)"
CURRENT_NOFILE="$(ulimit -n)"
if [[ "$CURRENT_NOFILE" -lt "$REQUIRED_NOFILE" ]]; then
  if ulimit -n "$REQUIRED_NOFILE" 2>/dev/null; then
    echo "Raised open-file limit from $CURRENT_NOFILE to $(ulimit -n) for connection pool sizing."
  else
    echo "ERROR: open-file limit is $CURRENT_NOFILE, but benchmark needs at least $REQUIRED_NOFILE for pool_size=$POOL_SIZE write_pool_size=$WRITE_POOL_SIZE." >&2
    echo "Run: ulimit -n $REQUIRED_NOFILE" >&2
    exit 1
  fi
fi

if [[ -z "${SUMMARY_ONLY:-}" ]]; then
  SUMMARY_ONLY="$(
    python3 - "$READ_RATE" <<'PY'
import sys
print(1 if float(sys.argv[1]) >= 100 else 0)
PY
  )"
  export SUMMARY_ONLY
fi

if [[ -n "${REUSE_EVENTS_JSON:-}" ]]; then
  export REUSE_EVENTS_JSON
fi

echo "Starting v15 prod180 benchmark"
echo "label=$LABEL read_rate=$READ_RATE duration=$DURATION preagg_layout=$PREAGG_LAYOUT preagg_mode=$PREAGG_MODE timeout_ms=$READ_MAX_EXECUTION_TIME_MS"
echo "session: tidb_isolation_read_engines=$TIDB_ISOLATION_READ_ENGINES tidb_opt_force_inline_cte=$INTUIT_FORCE_INLINE_CTE"
echo "preagg_bundles=$PREAGG_BUNDLE_IDS"
echo "unique_events_required=$UNIQUE_EVENTS_REQUIRED normal_events=$NORMAL_EVENTS hot_events_per_field=$HOT_EVENTS_PER_FIELD event_sample_multiplier=$EVENT_SAMPLE_MULTIPLIER"
echo "pool_size=$POOL_SIZE bundle_workers=$BUNDLE_WORKERS event_workers=$EVENT_WORKERS max_pending_events=$MAX_PENDING_EVENTS write_pool_size=$WRITE_POOL_SIZE summary_only=$SUMMARY_ONLY"

./run_final_mixed_preagg.sh "$LABEL" "$DURATION"
