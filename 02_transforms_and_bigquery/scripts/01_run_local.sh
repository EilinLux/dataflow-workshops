#!/usr/bin/env bash
# ==============================================================================
# 01_run_local.sh
# Chapter 02: Run Side Inputs & DLQ Pipeline Locally (DirectRunner)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SALES_FILE="${CHAPTER_DIR}/data/sales_data_with_errors.csv"
CATALOG_FILE="${CHAPTER_DIR}/data/product_catalog.json"
OUTPUT_DIR="${CHAPTER_DIR}/output"

VALID_PREFIX="${OUTPUT_DIR}/valid_sales"
DLQ_PREFIX="${OUTPUT_DIR}/dead_letter_records"

echo "======================================================================"
echo " Chapter 02: Running Side Input & Tagged Multi-Output Pipeline Locally"
echo "======================================================================"
echo "Sales Input:   ${SALES_FILE}"
echo "Catalog Input: ${CATALOG_FILE}"
echo "Valid Output:  ${VALID_PREFIX}"
echo "DLQ Output:    ${DLQ_PREFIX}"
echo "----------------------------------------------------------------------"

# Clean previous local outputs
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

uv run python "${CHAPTER_DIR}/pipelines/enrich_and_load_bq.py" \
  --runner=DirectRunner \
  --input-sales="${SALES_FILE}" \
  --input-catalog="${CATALOG_FILE}" \
  --output-local-valid="${VALID_PREFIX}" \
  --output-dlq="${DLQ_PREFIX}"

echo "----------------------------------------------------------------------"
echo " Execution finished! Generated files in ${OUTPUT_DIR}:"
ls -la "${OUTPUT_DIR}"
echo ""
echo "=== [1] Valid Enriched Sales Records (Sample) ==="
cat "${OUTPUT_DIR}"/valid_sales*.json
echo ""
echo "=== [2] Dead-Letter Poison Records Routed to DLQ ==="
cat "${OUTPUT_DIR}"/dead_letter_records*.json
echo ""
echo "======================================================================"
