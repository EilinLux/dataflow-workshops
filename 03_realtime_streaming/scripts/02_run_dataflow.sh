#!/usr/bin/env bash
# ==============================================================================
# 02_run_dataflow.sh
# Chapter 03: Submit Real-Time Streaming Pipeline to Google Cloud Dataflow
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
BQ_DATASET="${BQ_DATASET_ID:-dataflow_streaming_db}"
TOPIC_ID="${PUBSUB_TOPIC_ID:-telemetry-events-topic}"
SUB_ID="${PUBSUB_SUBSCRIPTION_ID:-telemetry-events-sub}"

JOB_NAME="ch03-streaming-windowing-$(date +%Y%m%d-%H%M%S)"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

TOPIC_PATH="projects/${PROJECT_ID}/topics/${TOPIC_ID}"
SUB_PATH="projects/${PROJECT_ID}/subscriptions/${SUB_ID}"

echo "======================================================================"
echo " Submitting Chapter 03 Streaming Pipeline to Google Cloud Dataflow"
echo "======================================================================"
echo "Project:            ${PROJECT_ID}"
echo "Region:             ${REGION}"
echo "Bucket:             ${BUCKET}"
echo "Pub/Sub Topic:      ${TOPIC_PATH}"
echo "Pub/Sub Sub:        ${SUB_PATH}"
echo "BigQuery Dataset:   ${BQ_DATASET}"
echo "Job Name:           ${JOB_NAME}"
echo "Streaming Engine:   Enabled (true)"
echo "----------------------------------------------------------------------"

# 1. Ensure Pub/Sub Topic exists
echo "[1/5] Ensuring Pub/Sub topic '${TOPIC_ID}' exists..."
gcloud pubsub topics describe "${TOPIC_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${TOPIC_ID}" --project="${PROJECT_ID}"

# 2. Ensure Pub/Sub Subscription exists with Message Ordering & Dead-Letter enabled
echo "[2/5] Ensuring Pub/Sub subscription '${SUB_ID}' exists with message ordering..."
gcloud pubsub subscriptions describe "${SUB_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "${SUB_ID}" \
    --topic="${TOPIC_ID}" \
    --project="${PROJECT_ID}" \
    --enable-message-ordering \
    --ack-deadline=60

# 3. Ensure BigQuery Dataset exists
echo "[3/5] Ensuring BigQuery dataset '${BQ_DATASET}' exists..."
bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1 || \
  bq --location="${BQ_LOCATION:-EU}" mk --dataset --description="Dataflow Streaming Dataset" "${PROJECT_ID}:${BQ_DATASET}"

GCS_TEMP="gs://${BUCKET}/temp"
GCS_STAGING="gs://${BUCKET}/staging"

# 4. Submit Dataflow Streaming Job
echo "[4/5] Launching Streaming Dataflow job with Streaming Engine..."
uv run python "${CHAPTER_DIR}/pipelines/streaming_windowing_pipeline.py" \
  --runner=DataflowRunner \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --job_name="${JOB_NAME}" \
  --staging_location="${GCS_STAGING}" \
  --temp_location="${GCS_TEMP}" \
  --input_subscription="${SUB_PATH}" \
  --output_dataset="${BQ_DATASET}" \
  --streaming \
  --enable_streaming_engine \
  --max_num_workers=4 \
  --save_main_session

echo "----------------------------------------------------------------------"
echo "[5/5] Streaming job submitted successfully!"
echo ""
echo "🚀 To publish live test events to the Pub/Sub topic, run:"
echo "   uv run python ${CHAPTER_DIR}/scripts/generate_streaming_data.py \\"
echo "     --project=${PROJECT_ID} \\"
echo "     --topic=${TOPIC_ID} \\"
echo "     --total-events=100 \\"
echo "     --rate=5.0"
echo ""
echo "Dataflow Job Console URL:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "BigQuery Streaming Tables:"
echo "https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BQ_DATASET}!3skpi_tumbling_metrics"
echo "======================================================================"
