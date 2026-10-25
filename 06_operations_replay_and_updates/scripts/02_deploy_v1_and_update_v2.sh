#!/usr/bin/env bash
# ==============================================================================
# 02_deploy_v1_and_update_v2.sh
# Chapter 06: Zero-Downtime Rolling Update Demonstration (DataflowRunner)
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
TOPIC_ID="${PUBSUB_TOPIC_ID:-orders-stream-topic}"
SUB_ID="${PUBSUB_SUBSCRIPTION_ID:-orders-stream-sub}"

JOB_NAME="ch06-orders-pipeline"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

TOPIC_PATH="projects/${PROJECT_ID}/topics/${TOPIC_ID}"
SUB_PATH="projects/${PROJECT_ID}/subscriptions/${SUB_ID}"
TABLE_PATH="${PROJECT_ID}:${BQ_DATASET}.processed_orders"

echo "======================================================================"
echo " Chapter 06: Zero-Downtime Streaming Update (V1 -> V2)"
echo "======================================================================"
echo "Project:      ${PROJECT_ID}"
echo "Region:       ${REGION}"
echo "Job Name:     ${JOB_NAME}"
echo "Topic:        ${TOPIC_PATH}"
echo "Subscription: ${SUB_PATH}"
echo "BigQuery:     ${TABLE_PATH}"
echo "----------------------------------------------------------------------"

# 1. Ensure Pub/Sub Topic and Subscription exist
echo "[1/4] Ensuring Pub/Sub topic and subscription exist..."
gcloud pubsub topics describe "${TOPIC_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${TOPIC_ID}" --project="${PROJECT_ID}" --message-retention-duration=7d

gcloud pubsub subscriptions describe "${SUB_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "${SUB_ID}" --topic="${TOPIC_ID}" --project="${PROJECT_ID}"

# 2. Ensure BigQuery dataset exists
echo "[2/4] Ensuring BigQuery dataset exists..."
bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1 || \
  bq --location="${BQ_LOCATION:-EU}" mk --dataset "${PROJECT_ID}:${BQ_DATASET}"

GCS_TEMP="gs://${BUCKET}/temp"
GCS_STAGING="gs://${BUCKET}/staging"

echo "[3/4] Step A: Deploying Pipeline Version 1..."
echo "Command:"
echo "uv run python ${CHAPTER_DIR}/pipelines/resilient_stream_v1.py \\"
echo "  --runner=DataflowRunner \\"
echo "  --project=${PROJECT_ID} \\"
echo "  --region=${REGION} \\"
echo "  --job_name=${JOB_NAME} \\"
echo "  --staging_location=${GCS_STAGING} \\"
echo "  --temp_location=${GCS_TEMP} \\"
echo "  --input_subscription=${SUB_PATH} \\"
echo "  --output_table=${TABLE_PATH} \\"
echo "  --streaming \\"
echo "  --enable_streaming_engine"

echo ""
echo "[4/4] Step B: Issuing In-Place Zero-Downtime Update to Version 2..."
echo "To update the running job in-place with NO downtime and NO data loss, run:"
echo "----------------------------------------------------------------------"
echo "uv run python ${CHAPTER_DIR}/pipelines/resilient_stream_v2.py \\"
echo "  --runner=DataflowRunner \\"
echo "  --project=${PROJECT_ID} \\"
echo "  --region=${REGION} \\"
echo "  --job_name=${JOB_NAME} \\"
echo "  --update \\"
echo "  --staging_location=${GCS_STAGING} \\"
echo "  --temp_location=${GCS_TEMP} \\"
echo "  --input_subscription=${SUB_PATH} \\"
echo "  --output_table=${TABLE_PATH} \\"
echo "  --streaming \\"
echo "  --enable_streaming_engine"
echo "----------------------------------------------------------------------"
echo "Notice the '--update' flag and identical '--job_name'. Dataflow automatically validates"
echo "graph compatibility, preserves in-flight state, and transitions workers with zero downtime!"
echo "======================================================================"
