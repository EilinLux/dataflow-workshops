#!/usr/bin/env bash
# ==============================================================================
# 02_run_dataflow.sh
# Chapter 04: Submit Stateful & Hot Key Fanout Pipeline to Google Cloud Dataflow
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
BQ_DATASET="${BQ_DATASET_ID:-dataflow_workshop}"

JOB_NAME="ch04-stateful-fanout-$(date +%Y%m%d-%H%M%S)"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

echo "======================================================================"
echo " Submitting Chapter 04 Stateful & Fanout Pipeline to Cloud Dataflow"
echo "======================================================================"
echo "Project:            ${PROJECT_ID}"
echo "Region:             ${REGION}"
echo "Bucket:             ${BUCKET}"
echo "BigQuery Dataset:   ${BQ_DATASET}"
echo "Job Name:           ${JOB_NAME}"
echo "Hot Key Fanout:     10 shards"
echo "----------------------------------------------------------------------"

# 1. Ensure BigQuery Dataset exists
echo "[1/3] Ensuring BigQuery dataset '${BQ_DATASET}' exists..."
bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1 || \
  bq --location="${BQ_LOCATION:-EU}" mk --dataset --description="Dataflow Workshop Dataset" "${PROJECT_ID}:${BQ_DATASET}"

# 2. Upload skewed clickstream data to Cloud Storage
GCS_INPUT="gs://${BUCKET}/data/ch04_skewed_clickstream.json"
GCS_TEMP="gs://${BUCKET}/temp"
GCS_STAGING="gs://${BUCKET}/staging"

echo "[2/3] Uploading skewed clickstream data to Cloud Storage..."
gcloud storage cp "${CHAPTER_DIR}/data/skewed_clickstream.json" "${GCS_INPUT}"

# 3. Submit Dataflow job
echo "[3/3] Launching Dataflow job with Hot Key Fanout and Stateful Processing..."
uv run python "${CHAPTER_DIR}/pipelines/stateful_and_fanout_pipeline.py" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --staging_location="${GCS_STAGING}" \
  --temp_location="${GCS_TEMP}" \
  --input_file="${GCS_INPUT}" \
  --output_dataset="${BQ_DATASET}" \
  --fanout_shards=10 \
  --session_inactivity_sec=60 \
  --max_num_workers=4 \
  --save_main_session

echo "----------------------------------------------------------------------"
echo "Dataflow job submitted successfully!"
echo "Dataflow Job Console URL:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "BigQuery Output Tables:"
echo "  1. User Session Profiles: https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BQ_DATASET}!3suser_session_profiles"
echo "  2. Fanout User Metrics:   https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BQ_DATASET}!3sfanout_user_metrics"
echo "======================================================================"
