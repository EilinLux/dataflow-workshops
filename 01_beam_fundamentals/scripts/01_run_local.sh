#!/usr/bin/env bash
# ==============================================================================
# 01_run_local.sh
# Execute the Chapter 01 Sales Pipeline locally using Apache Beam DirectRunner
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSHOP_DIR="$(cd "${CHAPTER_DIR}/.." && pwd)"

INPUT_FILE="${CHAPTER_DIR}/data/sales_data.csv"
OUTPUT_DIR="${CHAPTER_DIR}/output"
OUTPUT_PREFIX="${OUTPUT_DIR}/revenue_summary"

echo "======================================================================"
echo " Running Apache Beam Pipeline Locally (DirectRunner)"
echo "======================================================================"
echo "Input file:    ${INPUT_FILE}"
echo "Output prefix: ${OUTPUT_PREFIX}"
echo "----------------------------------------------------------------------"

# Clean up previous local outputs
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# Execute with uv run and DirectRunner
uv run python "${CHAPTER_DIR}/pipelines/sales_pipeline.py" \
  --runner=DirectRunner \
  --input="${INPUT_FILE}" \
  --output="${OUTPUT_PREFIX}" \
  --target-date="2025-06-12"

echo "----------------------------------------------------------------------"
echo " Execution finished! Generated output files:"
ls -la "${OUTPUT_DIR}"
echo ""
echo " Contents of output:"
cat "${OUTPUT_DIR}"/revenue_summary*.txt
echo ""
echo "======================================================================"
