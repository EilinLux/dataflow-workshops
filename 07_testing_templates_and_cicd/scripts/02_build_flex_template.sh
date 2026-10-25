#!/usr/bin/env bash
# ==============================================================================
# 02_build_flex_template.sh
# Chapter 07: Build & Publish Containerized Dataflow Flex Template
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
REPO_NAME="dataflow-templates"
IMAGE_NAME="dataflow-secure-orders"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "Error: GCP_PROJECT_ID is not set and no active gcloud project found."
  echo "Please set GCP_PROJECT_ID in .env or run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:latest"
TEMPLATE_PATH="gs://${BUCKET}/templates/secure_orders_template.json"

echo "======================================================================"
echo " Chapter 07: Building Dataflow Flex Template & Container Image"
echo "======================================================================"
echo "Project:           ${PROJECT_ID}"
echo "Region:            ${REGION}"
echo "Artifact Registry: ${IMAGE_TAG}"
echo "Template Spec GCS: ${TEMPLATE_PATH}"
echo "----------------------------------------------------------------------"

# 1. Ensure Artifact Registry repository exists
echo "[1/3] Ensuring Artifact Registry Docker repository '${REPO_NAME}' exists..."
gcloud artifacts repositories describe "${REPO_NAME}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud artifacts repositories create "${REPO_NAME}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Dataflow Flex Templates Repository" \
    --project="${PROJECT_ID}"

# 2. Build and push container image using Cloud Build
echo "[2/3] Building and pushing Docker container image via Google Cloud Build..."
gcloud builds submit "${CHAPTER_DIR}" \
  --tag="${IMAGE_TAG}" \
  --project="${PROJECT_ID}"

# 3. Build Dataflow Flex Template Specification in Cloud Storage
echo "[3/3] Generating Flex Template specification JSON in Cloud Storage..."
gcloud dataflow flex-template build "${TEMPLATE_PATH}" \
  --image="${IMAGE_TAG}" \
  --sdk-language="PYTHON" \
  --metadata-file="${CHAPTER_DIR}/template/metadata.json" \
  --project="${PROJECT_ID}"

echo "----------------------------------------------------------------------"
echo "✅ Flex Template built and registered successfully!"
echo "Template GCS URL: ${TEMPLATE_PATH}"
echo "To run this template with enterprise security flags, execute:"
echo "bash ${CHAPTER_DIR}/scripts/03_run_secure_flex_template.sh"
echo "======================================================================"
