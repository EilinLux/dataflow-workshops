#!/usr/bin/env bash
# ==============================================================================
# 01_test_rolling_update_simulation.sh
# Chapter 06: Test Rolling Update Logic (V1 vs V2 Comparison)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_FILE="${CHAPTER_DIR}/data/replay_telemetry.json"
OUTPUT_DIR="${CHAPTER_DIR}/output"

OUTPUT_V1="${OUTPUT_DIR}/orders_v1"
OUTPUT_V2="${OUTPUT_DIR}/orders_v2"

echo "======================================================================"
echo " Chapter 06: Simulating Rolling Pipeline Update (V1 -> V2)"
echo "======================================================================"
echo "Input Telemetry: ${INPUT_FILE}"
echo "V1 Output:       ${OUTPUT_V1}*.json"
echo "V2 Output:       ${OUTPUT_V2}*.json"
echo "----------------------------------------------------------------------"

# Clean output directory
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# 1. Run Pipeline Version 1
echo "[1/2] Running Pipeline Version 1 (Standard 5% Tax)..."
uv run python "${CHAPTER_DIR}/pipelines/resilient_stream_v1.py" \
  --runner=DirectRunner \
  --input_file="${INPUT_FILE}" \
  --output_prefix="${OUTPUT_V1}"

# 2. Run Pipeline Version 2
echo "[2/2] Running Pipeline Version 2 (10% Loyalty Discount on >=\$100)..."
uv run python "${CHAPTER_DIR}/pipelines/resilient_stream_v2.py" \
  --runner=DirectRunner \
  --input_file="${INPUT_FILE}" \
  --output_prefix="${OUTPUT_V2}"

echo "----------------------------------------------------------------------"
echo " Both versions executed successfully! Generated files in ${OUTPUT_DIR}:"
ls -la "${OUTPUT_DIR}"
echo ""
echo "=== Sample Output: Version 1 ==="
head -n 4 "${OUTPUT_DIR}"/orders_v1*.json
echo ""
echo "=== Sample Output: Version 2 (Notice loyalty_discount and net_amount) ==="
head -n 4 "${OUTPUT_DIR}"/orders_v2*.json
echo ""
echo "======================================================================"
