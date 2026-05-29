#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p logs results

LABEL="${1:-mixed}"
DURATION="${2:-300}"
READ_RATE="${READ_RATE:-3}"
HOT_EVENT_PCT="${HOT_EVENT_PCT:-0.05}"
READ_MAX_EXECUTION_TIME_MS="${READ_MAX_EXECUTION_TIME_MS:-0}"
PREAGG_MODE="${PREAGG_MODE:-hybrid}"
PREAGG_LAYOUT="${PREAGG_LAYOUT:-prod180}"
REUSE_EVENTS_JSON="${REUSE_EVENTS_JSON:-}"
NORMAL_EVENTS="${NORMAL_EVENTS:-900}"
HOT_EVENTS_PER_FIELD="${HOT_EVENTS_PER_FIELD:-10}"
POOL_SIZE="${POOL_SIZE:-300}"
WRITE_POOL_SIZE="${WRITE_POOL_SIZE:-25}"
EVENT_WORKERS="${EVENT_WORKERS:-0}"
BUNDLE_WORKERS="${BUNDLE_WORKERS:-0}"
MAX_PENDING_EVENTS="${MAX_PENDING_EVENTS:-0}"
UNIQUE_EVENTS_REQUIRED="${UNIQUE_EVENTS_REQUIRED:-0}"
SUMMARY_ONLY="${SUMMARY_ONLY:-0}"
SKIP_REPORTS="${SKIP_REPORTS:-0}"
BENCHMARK_LABEL_PREFIX="${BENCHMARK_LABEL_PREFIX:-v15}"

LOG="logs/mixed_traffic_${LABEL}_$(date +%Y%m%d_%H%M%S).log"

PREAGG_BUNDLES=(
  group_a_bundle_017 group_a_bundle_018 group_a_bundle_019 group_a_bundle_020
  group_b_bundle_017 group_b_bundle_018 group_b_bundle_019 group_b_bundle_020
  group_c_bundle_022 group_c_bundle_023 group_c_bundle_024 group_c_bundle_025
)

if [[ -n "${PREAGG_BUNDLE_IDS:-}" ]]; then
  # Space-separated override, for targeted tests such as 180d-only pre-agg.
  # Example: PREAGG_BUNDLE_IDS="group_b_bundle_017 group_b_bundle_018"
  read -r -a PREAGG_BUNDLES <<< "$PREAGG_BUNDLE_IDS"
elif [[ "$PREAGG_LAYOUT" == "bundle" ]]; then
  PREAGG_BUNDLES=(
    group_a_bundle_010 group_a_bundle_012 group_a_bundle_014 group_a_bundle_016 group_a_bundle_018 group_a_bundle_020
    group_b_bundle_009 group_b_bundle_010 group_b_bundle_011 group_b_bundle_012 group_b_bundle_013 group_b_bundle_014
    group_b_bundle_015 group_b_bundle_016 group_b_bundle_017 group_b_bundle_018 group_b_bundle_019 group_b_bundle_020
    group_c_bundle_015 group_c_bundle_016 group_c_bundle_017 group_c_bundle_018 group_c_bundle_019 group_c_bundle_020
    group_c_bundle_021 group_c_bundle_022 group_c_bundle_023 group_c_bundle_024 group_c_bundle_025
  )
fi

export TIDB_ISOLATION_READ_ENGINES="${TIDB_ISOLATION_READ_ENGINES:-tikv,tidb}"
export INTUIT_FORCE_INLINE_CTE="${INTUIT_FORCE_INLINE_CTE:-0}"

ARGS=(
  --duration "$DURATION"
  --warmup 0
  --read-rate "$READ_RATE"
  --hot-event-pct "$HOT_EVENT_PCT"
  --normal-events "$NORMAL_EVENTS"
  --hot-events-per-field "$HOT_EVENTS_PER_FIELD"
  --fast-normal-sampling
  --pool-size "$POOL_SIZE"
  --write-pool-size "$WRITE_POOL_SIZE"
  --event-workers "$EVENT_WORKERS"
  --bundle-workers "$BUNDLE_WORKERS"
  --max-pending-events "$MAX_PENDING_EVENTS"
  --read-max-execution-time-ms "$READ_MAX_EXECUTION_TIME_MS"
  --preagg-mode "$PREAGG_MODE"
  --preagg-layout "$PREAGG_LAYOUT"
  --skip-initial-warmup
)

if [[ "$UNIQUE_EVENTS_REQUIRED" == "1" ]]; then
  ARGS+=(--unique-events-required)
fi

if [[ "$SUMMARY_ONLY" == "1" ]]; then
  ARGS+=(--summary-only)
fi

if [[ -n "$REUSE_EVENTS_JSON" ]]; then
  ARGS+=(--reuse-events-json "$REUSE_EVENTS_JSON")
fi

if [[ "$PREAGG_MODE" == "hybrid" ]]; then
  for bundle in "${PREAGG_BUNDLES[@]}"; do
    ARGS+=(--preagg-bundle "$bundle")
  done
fi

echo "Starting mixed traffic benchmark"
echo "label=$LABEL duration=$DURATION read_rate=$READ_RATE hot_event_pct=$HOT_EVENT_PCT preagg_mode=$PREAGG_MODE preagg_layout=$PREAGG_LAYOUT read_max_execution_time_ms=$READ_MAX_EXECUTION_TIME_MS log=$LOG"
python3 - "$READ_RATE" <<'PY'
import sys
read_rate = float(sys.argv[1])
print(f"fanout_target={read_rate:.1f} events/sec * 65 bundles/event = {read_rate * 65:.1f} bundle SQL/sec")
PY
echo "normal_events=$NORMAL_EVENTS hot_events_per_field=$HOT_EVENTS_PER_FIELD pool_size=$POOL_SIZE write_pool_size=$WRITE_POOL_SIZE event_workers=$EVENT_WORKERS bundle_workers=$BUNDLE_WORKERS max_pending_events=$MAX_PENDING_EVENTS unique_events_required=$UNIQUE_EVENTS_REQUIRED summary_only=$SUMMARY_ONLY"
echo "session: tidb_isolation_read_engines=$TIDB_ISOLATION_READ_ENGINES tidb_opt_force_inline_cte=$INTUIT_FORCE_INLINE_CTE"

python3 -u mixed_traffic_test.py "${ARGS[@]}" 2>&1 | tee "$LOG"

LATEST_JSON="$(ls -t results/mixed_traffic_*.json | head -n 1)"
STEM="$(basename "$LATEST_JSON" .json)"

if [[ "$SKIP_REPORTS" == "1" || "$SUMMARY_ONLY" == "1" ]]; then
  echo "Summary-only or SKIP_REPORTS enabled; skipping detailed Markdown/CSV report generation."
  echo "Result JSON: $LATEST_JSON"
  exit 0
fi

python3 event_detail_report.py \
  --mixed-json "$LATEST_JSON" \
  --csv-output "results/event_detail_${STEM}.csv" \
  --md-output "results/event_detail_${STEM}.md"

python3 summarize_final_benchmark.py \
  --mixed-json "$LATEST_JSON" \
  --label "${BENCHMARK_LABEL_PREFIX} ${PREAGG_MODE}, ${READ_MAX_EXECUTION_TIME_MS}ms timeout, ${READ_RATE} events/sec mixed traffic" \
  --output "results/final_benchmark_summary_${STEM}_slack.md" \
  --slow-csv "results/slow_bundles_${STEM}.csv"

echo "Reports:"
echo "  results/event_detail_${STEM}.csv"
echo "  results/event_detail_${STEM}.md"
echo "  results/final_benchmark_summary_${STEM}_slack.md"
echo "  results/slow_bundles_${STEM}.csv"
