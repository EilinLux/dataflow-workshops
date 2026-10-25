# Chapter 06 — Resilient Operations, Disaster Recovery & Rolling Updates

[![Apache Beam](https://img.shields.io/badge/Apache_Beam-2.56+-FF6F00?logo=apachebeam&logoColor=white)](https://beam.apache.org/)
[![Google Cloud Dataflow](https://img.shields.io/badge/Google_Cloud-Dataflow-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/dataflow)
[![Google Cloud Pub/Sub](https://img.shields.io/badge/Google_Cloud-Pub%2FSub-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/pubsub)
[![Google BigQuery](https://img.shields.io/badge/Google_Cloud-BigQuery-669DF6?logo=googlecloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)

Streaming data pipelines are long-running services intended to operate 24 hours a day, 365 days a year. However, code evolves, business logic changes, bugs are discovered, and downstream corruptions occasionally require historical reprocessing.

Stopping a production pipeline creates processing gaps, while naive restarts can corrupt in-flight state or generate duplicate transactions.

This chapter covers the operational discipline required to run enterprise streaming pipelines on Google Cloud:
1. **In-Place Zero-Downtime Streaming Updates (`--update`)**: Upgrading running Dataflow pipelines while preserving state and processing continuity.
2. **Disaster Recovery via Pub/Sub Subscription Seek**: Safely replaying 24 hours of data to backfill corrupted tables without disrupting live production traffic.
3. **Dead-Letter Topics & Max Delivery Attempts**: Isolating poison records at the messaging layer to prevent infinite worker crash loops.

---

## Table of Contents

- [Real-World Production Problem & Storyline](#real-world-production-problem--storyline)
- [Architecture Overview](#architecture-overview)
- [1. Zero-Downtime In-Place Updates (`--update`)](#1-zero-downtime-in-place-updates---update)
  - [The Solution: Dataflow Update Command](#the-solution-dataflow-update-command)
  - [Graph Compatibility Rules](#graph-compatibility-rules)
- [2. Disaster Recovery via Pub/Sub Subscription Seek](#2-disaster-recovery-via-pubsub-subscription-seek)
  - [The Production Flaw: Seeking the Live Subscription](#the-production-flaw-seeking-the-live-subscription)
  - [The Production Gold Standard: Dedicated Recovery Subscription](#the-production-gold-standard-dedicated-recovery-subscription)
- [3. Pub/Sub Dead-Letter Topics & Poison Pill Defense](#3-pubsub-dead-letter-topics--poison-pill-defense)
- [Hands-On Execution Guide](#hands-on-execution-guide)
  - [Step 1: Simulate Rolling Update Locally (DirectRunner)](#step-1-simulate-rolling-update-locally-directrunner)
  - [Step 2: Deploy and Perform In-Place Update on Cloud Dataflow](#step-2-deploy-and-perform-in-place-update-on-cloud-dataflow)
  - [Step 3: Execute Disaster Recovery Seek Replay](#step-3-execute-disaster-recovery-seek-replay)
- [Official Documentation & Deep Dives](#official-documentation--deep-dives)

---

## Real-World Production Problem & Storyline

### The Scenario: Production Bug Fire Drill & 24h Disaster Recovery
An emergency audit reveals a critical revenue calculation bug in production: the pipeline omitted a 10% loyalty discount for orders $\ge \$100$, overcharging VIP customers for the past 24 hours. A patch is developed (`resilient_stream_v2.py`).

### The Production Disaster (Why Standard Restarts Fail)
1. **The Downtime Dilemma**: You cannot simply `cancel` a 24/7 streaming pipeline. Canceling drops in-flight bundles and causes Pub/Sub backlogs that trigger checkout timeouts for live customers.
2. **The Disaster Recovery Danger**: We need to re-process the corrupted 24 hours of data. If an engineer naively rewinds (`gcloud pubsub subscriptions seek`) the live production subscription, historical messages flood the live pipeline, causing duplicate billing, duplicate BigQuery rows, and massive customer confusion!

### How This Chapter's Demo Solves It
We execute a multi-tier resilience and disaster recovery protocol:
- **Zero-Downtime In-Place Rolling Update (`--update`)**: Upgrades code on the live Dataflow job. Because transform names (`IngestOrders`, `CalculateTaxes`, `WriteToSink`) are strictly preserved, Dataflow drains in-flight bundles on V1 while seamlessly routing new incoming events to V2 with zero downtime and zero dropped records.
- **Pub/Sub Dead-Letter Topics**: Sets `--dead-letter-topic` with `--max-delivery-attempts=5` to prevent poison pills from causing infinite worker crash loops.
- **Isolated Subscription Seek Disaster Recovery**: Leverages Pub/Sub's 7-day retention to create a dedicated recovery subscription (`orders-recovery-backfill-sub`), seeks it to $T - 24\text{h}$, processes backfill records into a temporary BigQuery staging table, and executes an atomic partition swap with zero impact on live shoppers.

### Storyline Connection
- ⬅️ **Previously (Chapter 05)**: Enabled Streaming Engine and configured Cloud Monitoring SLA alert policies.
- ➡️ **Next Crisis (Chapter 07)**: Security and compliance step in: running manual shell scripts with public IPs and default encryption fails SOC2 and PCI-DSS audits!

---

## Architecture Overview

The diagram below contrasts in-place rolling updates with isolated disaster recovery replay:

![Chapter 06 Resilient Operations & Updates DAG](../artifacts/06_resilient_ops_and_updates_dag.png)

<details>
<summary><b>View Mermaid Source Code</b></summary>

```mermaid
flowchart LR
    classDef pcoll fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8,font-weight:bold;
    classDef ptrans fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#b06000,font-weight:bold;
    classDef sink fill:#e6f4ea,stroke:#34a853,stroke-width:2px,color:#137333,font-weight:bold;
    classDef update fill:#f3e8fd,stroke:#9334e6,stroke-width:2px,color:#6b21a8,font-weight:bold;
    classDef dlq fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b,font-weight:bold;

    subgraph RollingUpdate["1. Zero-Downtime Rolling Update"]
        JOB_V1["Running Job V1<br/>Standard Tax (5%)"]:::ptrans
        CHECK{"Compatibility Check<br/>--update"}:::update
        JOB_V2["Running Job V2<br/>Loyalty Discount (10%)"]:::update
        JOB_V1 --> CHECK --> JOB_V2
    end

    subgraph PubSub["2. Pub/Sub 7-Day Retention & Seek"]
        TOPIC[("Pub/Sub Topic<br/>orders-stream")]:::pcoll
        PROD_SUB[("Live Sub<br/>orders-stream-sub")]:::pcoll
        RECOV_SUB[("Recovery Sub<br/>seek to -24h")]:::update
        DLQ[("Dead-Letter Topic<br/>max_delivery=5")]:::dlq
        TOPIC --> PROD_SUB
        TOPIC --> RECOV_SUB
        PROD_SUB -. "Poison Pills" .-> DLQ
    end

    subgraph Sinks["3. Zero-Disruption Backfill"]
        PROD_BQ[("BigQuery: processed_orders")]:::sink
        TEMP_BQ[("BigQuery: temp_backfill")]:::update
        SWAP["Atomic Partition Swap"]:::ptrans
        JOB_V2 --> PROD_BQ
        RECOV_SUB --> TEMP_BQ --> SWAP --> PROD_BQ
    end
```
</details>

---

## 1. Zero-Downtime In-Place Updates (`--update`)

When you need to deploy bug fixes or new features to a running streaming job, traditional web deployment patterns fail:
- **Drain & Snapshot (Anti-Pattern)**: Draining in-flight records takes time and stopping the job creates a processing gap. Restoring from a snapshot requires manual coordination and downtime.
- **Blue-Green Deployments (Anti-Pattern)**: Running two streaming pipelines simultaneously consumes double the infrastructure cost, risks duplicate processing on external sinks, and cannot use load balancers.

### The Solution: Dataflow Update Command
Dataflow supports in-place job replacement using the `--update` flag:

```bash
uv run python pipelines/resilient_stream_v2.py \
  --runner=DataflowRunner \
  --job_name="ch06-orders-pipeline" \
  --update \
  --streaming \
  ...
```

### Graph Compatibility Rules
For Dataflow to accept an update:
1. **Stable Transform Names**: Every transform in the DAG must maintain the same unique name (e.g., `"IngestOrders"`, `"CalculateTaxes"`). If transform names change, specify `--transform_name_mapping` to explicitly map old names to new names.
2. **Coder & State Compatibility**: Output types and per-key state schemas must be backward compatible.
3. **Execution Transition**: Dataflow seamlessly runs both versions concurrently on workers: in-flight bundles finish on the old code while newly ingested elements execute on the new code!

---

## 2. Disaster Recovery via Pub/Sub Subscription Seek

Suppose a critical bug in business logic corrupted downstream BigQuery data 24 hours ago. How do you recover?

### The Production Flaw: Seeking the Live Subscription
Stopping your production pipeline and seeking the live subscription back 24 hours causes severe disruption:
- Ongoing real-time processing halts.
- All downstream systems experience duplicate events.
- Unprocessed real-time backlogs spike system lag.

### The Production Gold Standard: Dedicated Recovery Subscription
1. **Message Retention**: Ensure your Pub/Sub topic has message retention configured (e.g. 7 days):
   ```bash
   gcloud pubsub topics create orders-stream-topic --message-retention-duration=7d
   ```
2. **Create an Isolated Recovery Subscription**:
   ```bash
   gcloud pubsub subscriptions create orders-recovery-backfill-sub \
     --topic=orders-stream-topic
   ```
3. **Seek to Historical Timestamp**:
   ```bash
   gcloud pubsub subscriptions seek orders-recovery-backfill-sub \
     --time="2026-10-26T14:00:00Z"
   ```
4. **Deploy Recovery Pipeline to Staging Sink**: Run a temporary Dataflow job reading from `orders-recovery-backfill-sub` and writing to a temporary BigQuery table (`temp_orders_backfill`).
5. **Atomic Partition Swap**: Once data integrity is verified, merge or swap partitions into the production table using BigQuery DDL.

---

## 3. Pub/Sub Dead-Letter Topics & Poison Pill Defense

When a message payload contains corrupted bytes that crash worker code:
- Without a dead-letter topic, Pub/Sub repeatedly re-delivers the poison pill forever, consuming CPU and generating alerts.
- Configure `--dead-letter-topic` and `--max-delivery-attempts=5`:
  ```bash
  gcloud pubsub subscriptions update orders-stream-sub \
    --dead-letter-topic=orders-dead-letter-topic \
    --max-delivery-attempts=5
  ```
  After 5 failed worker acknowledgments, Pub/Sub routes the poison message to the dead-letter topic and continues normal stream processing.

---

## Hands-On Execution Guide

### Step 1: Simulate Rolling Update Locally (DirectRunner)

Run the comparison script to verify business logic and schema evolution between Version 1 and Version 2:

```bash
bash 06_operations_replay_and_updates/scripts/01_test_rolling_update_simulation.sh
```

**Output Comparison:**
- **Version 1**: Calculates standard 5% tax.
  ```json
  {"order_id": "ord-00001", "subtotal": 291.29, "tax_amount": 14.56, "total_amount": 305.85, "pipeline_version": "v1.0.0"}
  ```
- **Version 2**: Enriches with 10% loyalty discount for orders $\ge \$100$, producing `loyalty_discount` and `net_amount` while keeping transform names identical:
  ```json
  {"order_id": "ord-00001", "subtotal": 291.29, "loyalty_discount": 29.13, "net_amount": 262.16, "tax_amount": 13.11, "total_amount": 275.27, "pipeline_version": "v2.0.0"}
  ```

### Step 2: Deploy and Perform In-Place Update on Cloud Dataflow

Inspect and run the rolling update automation:

```bash
bash 06_operations_replay_and_updates/scripts/02_deploy_v1_and_update_v2.sh
```

### Step 3: Execute Disaster Recovery Seek Replay

Simulate historical data recovery without impacting live production:

```bash
bash 06_operations_replay_and_updates/scripts/03_disaster_recovery_seek_replay.sh
```

---

## Official Documentation & Deep Dives

* [Updating an Existing Dataflow Pipeline](https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline) — Deep dive into in-place `--update`, transform name compatibility, and drained bundle transitions.
* [Stopping a Running Dataflow Job (Drain vs. Cancel)](https://cloud.google.com/dataflow/docs/guides/stopping-a-pipeline) — Understanding zero-loss graceful drain and state flush mechanics.
* [Pub/Sub Replay and Seek Overview](https://cloud.google.com/pubsub/docs/replay-overview) — Replaying unacknowledged and acknowledged messages to timestamp or snapshot.
* [Handling Message Failures with Pub/Sub Dead-Letter Topics](https://cloud.google.com/pubsub/docs/handling-failures#dead_letter_topic) — Forwarding poison pills and setting `max_delivery_attempts`.
* [Cloud Pub/Sub Message Retention](https://cloud.google.com/pubsub/docs/subscriber#message_retention) — Configuring topic and subscription retention duration for auditability and recovery.

