#!/usr/bin/env bash
# ==============================================================================
# 01_run_local.sh
# Chapter 03: Run Real-Time Streaming & Windowing Pipeline Locally (DirectRunner)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SAMPLE_FILE="${CHAPTER_DIR}/data/sample_events.json"
OUTPUT_DIR="${CHAPTER_DIR}/output"

TUMBLING_PREFIX="${OUTPUT_DIR}/tumbling_metrics"
SLIDING_PREFIX="${OUTPUT_DIR}/sliding_metrics"
SESSION_PREFIX="${OUTPUT_DIR}/session_metrics"
DLQ_PREFIX="${OUTPUT_DIR}/dead_letters"

echo "======================================================================"
echo " Chapter 03: Running Streaming Windowing & Late Data Pipeline Locally"
echo "======================================================================"
echo "Input Events:     ${SAMPLE_FILE}"
echo "Tumbling Output:  ${TUMBLING_PREFIX}*.json"
echo "Sliding Output:   ${SLIDING_PREFIX}*.json"
echo "Session Output:   ${SESSION_PREFIX}*.json"
echo "Dead-Letter DLQ:  ${DLQ_PREFIX}*.json"
echo "----------------------------------------------------------------------"

# Clean previous local outputs
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

uv run python "${CHAPTER_DIR}/pipelines/streaming_windowing_pipeline.py" \
  --runner=DirectRunner \
  --input_file="${SAMPLE_FILE}" \
  --output_prefix_tumbling="${TUMBLING_PREFIX}" \
  --output_prefix_sliding="${SLIDING_PREFIX}" \
  --output_prefix_session="${SESSION_PREFIX}" \
  --output_prefix_dlq="${DLQ_PREFIX}" \
  --fixed_window_sec=60 \
  --slide_window_sec=60 \
  --slide_period_sec=20 \
  --session_gap_sec=30 \
  --allowed_lateness_sec=120

echo "----------------------------------------------------------------------"
echo " Execution finished! Output files in ${OUTPUT_DIR}:"
ls -la "${OUTPUT_DIR}"
echo ""
echo "=== [1] Fixed (Tumbling) Window Aggregations (Sample) ==="
cat "${OUTPUT_DIR}"/tumbling_metrics*.json | head -n 8
echo ""
echo "=== [2] Sliding (Hopping) Window Aggregations (Sample) ==="
cat "${OUTPUT_DIR}"/sliding_metrics*.json | head -n 8
echo ""
echo "=== [3] Session Window Aggregations (Sample) ==="
cat "${OUTPUT_DIR}"/session_metrics*.json | head -n 8
echo ""
echo "=== [4] Dead-Letter Poison Records (Sample) ==="
cat "${OUTPUT_DIR}"/dead_letters*.json | head -n 8
echo ""
echo "======================================================================"
