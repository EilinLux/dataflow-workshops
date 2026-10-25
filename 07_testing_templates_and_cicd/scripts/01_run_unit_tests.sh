#!/usr/bin/env bash
# ==============================================================================
# 01_run_unit_tests.sh
# Chapter 07: Run Apache Beam Unit Tests (TestPipeline & TestStream)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "======================================================================"
echo " Chapter 07: Running Unit Tests (TestPipeline, PAssert & TestStream)"
echo "======================================================================"

cd "${CHAPTER_DIR}"
uv run pytest -v tests/

echo "======================================================================"
echo "✅ All Apache Beam unit tests passed successfully!"
echo "======================================================================"
