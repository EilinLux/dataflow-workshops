#!/usr/bin/env bash
# ==============================================================================
# 01_run_local_benchmark.sh
# Chapter 05: Run Local Benchmark & Measure Step Latency Metrics (DirectRunner)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

INPUT_FILE="${CHAPTER_DIR}/data/benchmark_telemetry.json"
OUTPUT_DIR="${CHAPTER_DIR}/output"
METRICS_PREFIX="${OUTPUT_DIR}/category_metrics"

echo "======================================================================"
echo " Chapter 05: Running Local Pipeline Benchmark & Step Latency Metrics"
echo "======================================================================"
echo "Input File:      ${INPUT_FILE}"
echo "Output Prefix:   ${METRICS_PREFIX}*.json"
echo "Simulated Delay: 0.5 ms / record"
echo "----------------------------------------------------------------------"

# Clean previous local outputs
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

uv run python "${CHAPTER_DIR}/pipelines/benchmarked_streaming_pipeline.py" \
  --runner=DirectRunner \
  --input_file="${INPUT_FILE}" \
  --output_prefix_metrics="${METRICS_PREFIX}" \
  --simulated_latency_ms=0.5

echo "----------------------------------------------------------------------"
echo " Execution finished! Generated files in ${OUTPUT_DIR}:"
ls -la "${OUTPUT_DIR}"
echo ""
echo "=== Aggregated Category Performance Metrics ==="
cat "${OUTPUT_DIR}"/category_metrics*.json
echo ""
echo "======================================================================"
