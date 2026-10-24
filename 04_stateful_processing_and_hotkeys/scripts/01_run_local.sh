#!/usr/bin/env bash
# ==============================================================================
# 01_run_local.sh
# Chapter 04: Run Stateful Processing & Hot Key Fanout Pipeline Locally
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_FILE="${CHAPTER_DIR}/data/skewed_clickstream.json"
OUTPUT_DIR="${CHAPTER_DIR}/output"

STATEFUL_PREFIX="${OUTPUT_DIR}/stateful_sessions"
FANOUT_PREFIX="${OUTPUT_DIR}/fanout_metrics"

echo "======================================================================"
echo " Chapter 04: Running Stateful Processing & Hot Key Fanout Pipeline"
echo "======================================================================"
echo "Input File:      ${INPUT_FILE}"
echo "Stateful Output: ${STATEFUL_PREFIX}*.json"
echo "Fanout Output:   ${FANOUT_PREFIX}*.json"
echo "----------------------------------------------------------------------"

# Clean previous local outputs
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

uv run python "${CHAPTER_DIR}/pipelines/stateful_and_fanout_pipeline.py" \
  --runner=DirectRunner \
  --input_file="${INPUT_FILE}" \
  --output_prefix_stateful="${STATEFUL_PREFIX}" \
  --output_prefix_fanout="${FANOUT_PREFIX}" \
  --fanout_shards=10 \
  --session_inactivity_sec=60

echo "----------------------------------------------------------------------"
echo " Execution finished! Output files in ${OUTPUT_DIR}:"
ls -la "${OUTPUT_DIR}"
echo ""
echo "=== [1] Finalized Stateful User Sessions (Sample) ==="
cat "${OUTPUT_DIR}"/stateful_sessions*.json | head -n 12
echo ""
echo "=== [2] Aggregated Fanout Metrics (Sample) ==="
cat "${OUTPUT_DIR}"/fanout_metrics*.json | head -n 12
echo ""
echo "======================================================================"
