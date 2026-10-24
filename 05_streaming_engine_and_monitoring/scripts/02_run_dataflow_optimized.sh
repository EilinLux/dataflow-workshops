#!/usr/bin/env bash
# ==============================================================================
# 02_run_dataflow_optimized.sh
# Chapter 05: Submit Production-Optimized Dataflow Pipeline with Monitoring
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

JOB_NAME="ch05-perf-monitoring-$(date +%Y%m%d-%H%M%S)"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

echo "======================================================================"
echo " Submitting Chapter 05 Optimized Pipeline with Observability"
echo "======================================================================"
echo "Project:            ${PROJECT_ID}"
echo "Region:             ${REGION}"
echo "Bucket:             ${BUCKET}"
echo "BigQuery Dataset:   ${BQ_DATASET}"
echo "Job Name:           ${JOB_NAME}"
echo "Machine Type:       n2-standard-4"
echo "Autoscaling:        min=2, max=8 workers"
echo "Streaming Engine:   Enabled (true)"
echo "----------------------------------------------------------------------"

# 1. Ensure BigQuery Dataset exists
echo "[1/4] Ensuring BigQuery dataset '${BQ_DATASET}' exists..."
bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1 || \
  bq --location="${BQ_LOCATION:-EU}" mk --dataset --description="Dataflow Workshop Dataset" "${PROJECT_ID}:${BQ_DATASET}"

# 2. Upload benchmark telemetry to Cloud Storage
GCS_INPUT="gs://${BUCKET}/data/ch05_benchmark_telemetry.json"
GCS_TEMP="gs://${BUCKET}/temp"
GCS_STAGING="gs://${BUCKET}/staging"

echo "[2/4] Uploading benchmark data to Cloud Storage..."
gcloud storage cp "${CHAPTER_DIR}/data/benchmark_telemetry.json" "${GCS_INPUT}"

# 3. Submit Dataflow job with performance optimization flags
echo "[3/4] Launching DataflowRunner with autoscaling and Streaming Engine..."
uv run python "${CHAPTER_DIR}/pipelines/benchmarked_streaming_pipeline.py" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --staging_location="${GCS_STAGING}" \
  --temp_location="${GCS_TEMP}" \
  --input_file="${GCS_INPUT}" \
  --output_dataset="${BQ_DATASET}" \
  --worker_machine_type="n2-standard-4" \
  --num_workers=2 \
  --max_num_workers=8 \
  --save_main_session

echo "----------------------------------------------------------------------"
echo "[4/4] Dataflow job submitted successfully!"
echo "Dataflow Job Console URL:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "📊 Cloud Monitoring Alert Policy Deployment Commands:"
echo "   gcloud alpha monitoring policies create \\"
echo "     --policy-from-file=${CHAPTER_DIR}/monitoring/alert_policy_system_lag.json \\"
echo "     --project=${PROJECT_ID}"
echo ""
echo "   gcloud alpha monitoring policies create \\"
echo "     --policy-from-file=${CHAPTER_DIR}/monitoring/alert_policy_data_freshness.json \\"
echo "     --project=${PROJECT_ID}"
echo ""
echo "📈 Cloud Monitoring SLA Dashboard Import Command:"
echo "   gcloud monitoring dashboards create \\"
echo "     --config-from-file=${CHAPTER_DIR}/monitoring/dashboard_dataflow_sla.json \\"
echo "     --project=${PROJECT_ID}"
echo "======================================================================"
