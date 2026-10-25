#!/usr/bin/env bash
# ==============================================================================
# 03_disaster_recovery_seek_replay.sh
# Chapter 06: Disaster Recovery via Pub/Sub Message Retention & Subscription Seek
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
PROD_SUB_ID="${PUBSUB_SUBSCRIPTION_ID:-orders-stream-sub}"
RECOVERY_SUB_ID="orders-recovery-backfill-sub"
DLQ_TOPIC_ID="orders-dead-letter-topic"

echo "======================================================================"
echo " Chapter 06: Disaster Recovery via Pub/Sub Seek Replay"
echo "======================================================================"
echo "Project:               ${PROJECT_ID}"
echo "Live Production Topic: ${TOPIC_ID}"
echo "Live Production Sub:   ${PROD_SUB_ID}"
echo "Recovery Backfill Sub: ${RECOVERY_SUB_ID}"
echo "Dead-Letter Topic:     ${DLQ_TOPIC_ID}"
echo "----------------------------------------------------------------------"

# Step 1: Ensure 7-Day Message Retention on Pub/Sub Topic
echo "[1/5] Ensuring Topic has 7-day message retention configured..."
gcloud pubsub topics describe "${TOPIC_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${TOPIC_ID}" --project="${PROJECT_ID}" --message-retention-duration=7d

# Step 2: Ensure Dead-Letter Topic and Max Delivery Attempts Policy
echo "[2/5] Configuring Dead-Letter Queue with max delivery attempts = 5..."
gcloud pubsub topics describe "${DLQ_TOPIC_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${DLQ_TOPIC_ID}" --project="${PROJECT_ID}"

# Step 3: Create Dedicated Recovery Subscription
echo "[3/5] Creating dedicated recovery subscription '${RECOVERY_SUB_ID}'..."
gcloud pubsub subscriptions describe "${RECOVERY_SUB_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create "${RECOVERY_SUB_ID}" \
    --topic="${TOPIC_ID}" \
    --project="${PROJECT_ID}" \
    --ack-deadline=60

# Step 4: Seek Subscription to 24 Hours Ago
REPLAY_TIMESTAMP=$(date -u -v-24H +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d "24 hours ago" +"%Y-%m-%dT%H:%M:%SZ")
echo "[4/5] Seeking recovery subscription to 24 hours in the past (${REPLAY_TIMESTAMP})..."
echo "Command:"
echo "gcloud pubsub subscriptions seek ${RECOVERY_SUB_ID} --time='${REPLAY_TIMESTAMP}' --project=${PROJECT_ID}"

# Step 5: Run Backfill Pipeline to Temporary BigQuery Table
BACKFILL_TABLE="${PROJECT_ID}:${BQ_DATASET}.temp_orders_backfill_$(date +%Y%m%d)"
echo "[5/5] Launching Recovery Backfill Dataflow Job..."
echo "Destination Table (Temp Validation): ${BACKFILL_TABLE}"
echo "Command:"
echo "uv run python ${CHAPTER_DIR}/pipelines/resilient_stream_v2.py \\"
echo "  --runner=DataflowRunner \\"
echo "  --project=${PROJECT_ID} \\"
echo "  --region=${REGION} \\"
echo "  --job_name=ch06-disaster-recovery-backfill \\"
echo "  --input_subscription=projects/${PROJECT_ID}/subscriptions/${RECOVERY_SUB_ID} \\"
echo "  --output_table=${BACKFILL_TABLE} \\"
echo "  --streaming \\"
echo "  --enable_streaming_engine"
echo ""
echo "💡 Architectural Principle: Seeking an isolated recovery subscription leaves the"
echo "production pipeline unaffected, ensuring continuous operations while safely backfilling!"
echo "======================================================================"
