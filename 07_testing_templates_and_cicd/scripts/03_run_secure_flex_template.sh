#!/usr/bin/env bash
# ==============================================================================
# 03_run_secure_flex_template.sh
# Chapter 07: Launch Secure Dataflow Flex Template (Private IPs & CMEK)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAPTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSHOP_DIR="$(cd "${CHAPTER_DIR}/.." && pwd)"

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

TEMPLATE_PATH="gs://${BUCKET}/templates/secure_orders_template.json"
JOB_NAME="ch07-secure-flex-$(date +%Y%m%d-%H%M%S)"
SUB_PATH="projects/${PROJECT_ID}/subscriptions/${SUB_ID}"
TABLE_PATH="${PROJECT_ID}:${BQ_DATASET}.secure_curated_orders"

# Optional Enterprise Security Parameters (Configured in .env or GCP defaults)
KMS_KEY="${CMEK_KMS_KEY_ID:-projects/${PROJECT_ID}/locations/${REGION}/keyRings/dataflow-keyring/cryptoKeys/dataflow-key}"
VPC_SUBNET="${DATAFLOW_SUBNETWORK:-default}"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

echo "======================================================================"
echo " Chapter 07: Launching Enterprise Secure Dataflow Flex Template"
echo "======================================================================"
echo "Project:            ${PROJECT_ID}"
echo "Region:             ${REGION}"
echo "Job Name:           ${JOB_NAME}"
echo "Template Spec:      ${TEMPLATE_PATH}"
echo "Security Controls:  Private IPs (--disable-public-ips)"
echo "CMEK Encryption:    ${KMS_KEY}"
echo "VPC Subnetwork:     ${VPC_SUBNET}"
echo "----------------------------------------------------------------------"

# Ensure BigQuery destination dataset exists
bq show --project_id="${PROJECT_ID}" "${BQ_DATASET}" >/dev/null 2>&1 || \
  bq --location="${BQ_LOCATION:-EU}" mk --dataset "${PROJECT_ID}:${BQ_DATASET}"

echo "Launching Dataflow Flex Template with Private IPs..."
gcloud dataflow flex-template run "${JOB_NAME}" \
  --template-file-gcs-location="${TEMPLATE_PATH}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --disable-public-ips \
  --subnetwork="${VPC_SUBNET}" \
  --enable-streaming-engine \
  --parameters="input_subscription=${SUB_PATH},output_table=${TABLE_PATH}" \
  --additional-user-labels="environment=production,security_tier=cmek,workshop=dataflow"

echo "----------------------------------------------------------------------"
echo "✅ Secure Flex Template Job submitted successfully!"
echo "Dataflow Job Console URL:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo "======================================================================"
