#!/usr/bin/env bash
# ==============================================================================
# 02_run_dataflow.sh
# Chapter 02: Submit Side Inputs & BigQuery Pipeline to Google Cloud Dataflow
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
BQ_TABLE="${PROJECT_ID}:${BQ_DATASET}.curated_sales"
JOB_NAME="ch02-enrich-bq-$(date +%Y%m%d-%H%M%S)"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

echo "======================================================================"
echo " Submitting Chapter 02 Pipeline to Google Cloud Dataflow"
echo "======================================================================"
echo "Project:      ${PROJECT_ID}"
echo "Region:       ${REGION}"
echo "Bucket:       ${BUCKET}"
echo "BigQuery:     ${BQ_TABLE}"
echo "Job Name:     ${JOB_NAME}"
echo "----------------------------------------------------------------------"

# 1. Ensure BigQuery Dataset exists
echo "[1/4] Ensuring BigQuery dataset '${BQ_DATASET}' exists..."
bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1 || \
  bq --location="${BQ_LOCATION:-EU}" mk --dataset --description="Dataflow Workshop Dataset" "${PROJECT_ID}:${BQ_DATASET}"

# 2. Upload sales data and catalog to Cloud Storage
GCS_SALES="gs://${BUCKET}/data/ch02_sales_data_with_errors.csv"
GCS_CATALOG="gs://${BUCKET}/data/ch02_product_catalog.json"
GCS_DLQ="gs://${BUCKET}/dlq/dead_letter_records"
GCS_TEMP="gs://${BUCKET}/temp"
GCS_STAGING="gs://${BUCKET}/staging"

echo "[2/4] Uploading raw sales data and product catalog to Cloud Storage..."
gcloud storage cp "${CHAPTER_DIR}/data/sales_data_with_errors.csv" "${GCS_SALES}"
gcloud storage cp "${CHAPTER_DIR}/data/product_catalog.json" "${GCS_CATALOG}"

# 3. Submit Dataflow job
echo "[3/4] Launching DataflowRunner job..."
uv run python "${CHAPTER_DIR}/pipelines/enrich_and_load_bq.py" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --staging_location="${GCS_STAGING}" \
  --temp_location="${GCS_TEMP}" \
  --input-sales="${GCS_SALES}" \
  --input-catalog="${GCS_CATALOG}" \
  --output-bq-table="${BQ_TABLE}" \
  --output-dlq="${GCS_DLQ}" \
  --max_num_workers=3 \
  --save_main_session

echo "----------------------------------------------------------------------"
echo "[4/4] Dataflow job submitted successfully!"
echo "Dataflow Job Console URL:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "BigQuery Table Destination:"
echo "https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BQ_DATASET}!3scurated_sales"
echo "======================================================================"
