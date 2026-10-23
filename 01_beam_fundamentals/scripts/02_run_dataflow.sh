#!/usr/bin/env bash
# ==============================================================================
# 02_run_dataflow.sh
# Submit the Chapter 01 Sales Pipeline to Google Cloud Dataflow (DataflowRunner)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSHOP_DIR="$(cd "${CHAPTER_DIR}/.." && pwd)"

# Load environment configuration if available
if [[ -f "${WORKSHOP_DIR}/.env" ]]; then
  echo "Loading environment variables from ${WORKSHOP_DIR}/.env"
  # shellcheck disable=SC1091
  source "${WORKSHOP_DIR}/.env"
fi

PROJECT_ID="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${GCP_REGION:-europe-west1}"
BUCKET="${DATAFLOW_BUCKET:-${PROJECT_ID}-dataflow-storage}"
JOB_NAME="ch01-sales-summary-$(date +%Y%m%d-%H%M%S)"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

echo "======================================================================"
echo " Submitting Apache Beam Pipeline to Google Cloud Dataflow"
echo "======================================================================"
echo "Project:      ${PROJECT_ID}"
echo "Region:       ${REGION}"
echo "Bucket:       ${BUCKET}"
echo "Job Name:     ${JOB_NAME}"
echo "----------------------------------------------------------------------"

# 1. Upload sample sales_data.csv to Cloud Storage
GCS_INPUT="gs://${BUCKET}/data/sales_data.csv"
GCS_OUTPUT="gs://${BUCKET}/output/ch01_revenue_summary"
GCS_TEMP="gs://${BUCKET}/temp"
GCS_STAGING="gs://${BUCKET}/staging"

echo "[1/3] Uploading sample dataset to ${GCS_INPUT}..."
gcloud storage cp "${CHAPTER_DIR}/data/sales_data.csv" "${GCS_INPUT}"

# 2. Submit the Dataflow Job
echo "[2/3] Submitting Dataflow job..."
uv run python "${CHAPTER_DIR}/pipelines/sales_pipeline.py" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --staging_location="${GCS_STAGING}" \
  --temp_location="${GCS_TEMP}" \
  --input="${GCS_INPUT}" \
  --output="${GCS_OUTPUT}" \
  --target-date="2025-06-12" \
  --max_num_workers=3 \
  --save_main_session

echo "----------------------------------------------------------------------"
echo "[3/3] Job submitted successfully!"
echo "View job in Google Cloud Console:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo "======================================================================"
