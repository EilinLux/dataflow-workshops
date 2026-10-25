# Chapter 03 — Real-Time Streaming, Windowing & Late Data

[![Apache Beam](https://img.shields.io/badge/Apache_Beam-2.56+-FF6F00?logo=apachebeam&logoColor=white)](https://beam.apache.org/)
[![Google Cloud Dataflow](https://img.shields.io/badge/Google_Cloud-Dataflow-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/dataflow)
[![Google Cloud Pub/Sub](https://img.shields.io/badge/Google_Cloud-Pub%2FSub-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/pubsub)
[![Google BigQuery](https://img.shields.io/badge/Google_Cloud-BigQuery-669DF6?logo=googlecloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)

In real-world data engineering, data arrives continuously, out-of-order, and unpredictably. Batch processing paradigms fail when downstream decision systems require real-time velocity metrics, active session tracking, and continuous fraud detection.

This chapter covers the theoretical and operational foundations of **unbounded stream processing** using Apache Beam and Google Cloud Dataflow. You will build a multi-branch streaming pipeline that ingests event streams from **Google Cloud Pub/Sub**, extracts event-time timestamps, handles out-of-order and late-arriving records, and computes three distinct temporal aggregations simultaneously.

---

## Table of Contents

- [Real-World Production Problem & Storyline](#real-world-production-problem--storyline)
- [Architecture Overview](#architecture-overview)
- [Core Streaming Concepts: The 4 Questions](#core-streaming-concepts-the-4-questions)
- [Event Time vs. Processing Time](#event-time-vs-processing-time)
- [The Three Windowing Topologies](#the-three-windowing-topologies)
  - [1. Fixed (Tumbling) Windows](#1-fixed-tumbling-windows)
  - [2. Sliding (Hopping) Windows](#2-sliding-hopping-windows)
  - [3. Session Windows](#3-session-windows)
- [Watermarks, Allowed Lateness & Triggers](#watermarks-allowed-lateness--triggers)
  - [What is a Watermark?](#what-is-a-watermark)
  - [Handling Late Data](#handling-late-data)
- [Google Cloud Pub/Sub with Ordering Keys](#google-cloud-pubsub-with-ordering-keys)
- [Chapter Directory Structure](#chapter-directory-structure)
- [Step-by-Step Hands-On Guide](#step-by-step-hands-on-guide)
  - [Step 1: Activate the Environment](#step-1-activate-the-environment)
  - [Step 2: Run the Pipeline Locally (DirectRunner)](#step-2-run-the-pipeline-locally-directrunner)
  - [Step 3: Run the Streaming Generator](#step-3-run-the-streaming-generator-optional-live-simulator)
  - [Step 4: Deploy to Google Cloud Dataflow](#step-4-deploy-to-google-cloud-dataflow)
- [Official Documentation & Deep Dives](#official-documentation--deep-dives)

---

## Real-World Production Problem & Storyline

### The Scenario: The Black Friday Real-Time Pivot
Black Friday arrives. OmniRetail launches nationwide flash sales. Flash checkout volume spikes to 50,000 orders/minute. Fraud rings deploy bot swarms purchasing limited-inventory items in milliseconds. Inventory managers cannot wait for a midnight batch report—they need rolling 60-second velocity metrics and immediate session alerts.

### The Production Disaster (Why Batch Paradigms Fail)
1. **The 24-Hour Blind Spot**: A batch pipeline only knows what happened yesterday. Fraud rings exploit this latency to place fraudulent orders and transfer funds before the batch job reconciles.
2. **Network Lag & Out-of-Order Packets**: In the real world, mobile shoppers on flaky 4G/5G networks experience packet delays. A customer purchase timestamped at 10:00:05 AM may arrive at our server at 10:04:12 AM. Naively grouping by processing time (arrival wall-clock) corrupts metrics and misattributes sales to wrong hours.
3. **Poison Streaming Crashes**: In streaming, a single unhandled poison message causes infinite redelivery and worker restart loops, freezing stream processing.

### How This Chapter's Demo Solves It
We pivot our architecture to unbounded stream processing with **Google Cloud Pub/Sub** and Apache Beam:
- **Ordering Keys**: Guarantees per-user FIFO message ordering across distributed Pub/Sub partitions.
- **Event-Time Semantics & `TimestampedValue`**: Reconstructs true real-world order based on when the transaction happened, independent of when packets arrived.
- **The Three Windowing Topologies**:
  - **Fixed (Tumbling) Windows (60s)**: Discrete non-overlapping financial buckets with watermark triggers.
  - **Sliding (Hopping) Windows (60s size, 20s period)**: Smooth moving averages tracking real-time checkout velocity.
  - **Session Windows (30s gap)**: Dynamic user activity bursts identifying bot swarms vs. human shoppers.
- **Watermark Triggers & Allowed Lateness**: Emits speculative early results immediately, while allowing late-arriving packets to update window sums.

### Storyline Connection
- ⬅️ **Previously (Chapter 02)**: Hardened batch pipelines with Side Inputs and Dead-Letter Queues.
- ➡️ **Next Crisis (Chapter 04)**: A celebrity influencer tweets an endorsement, sending 80% of nationwide traffic to a single user key. The single worker crashes with a hot key error, while shopping cart memory leaks exhaust worker RAM!

---

## Architecture Overview

The pipeline ingests unbounded e-commerce telemetry, isolates poison records into a Dead-Letter Queue, and fans out valid events into three parallel windowing topologies:

![Chapter 03 Streaming DAG](../artifacts/03_windowing_and_streaming_dag.png)

<details>
<summary><b>View Mermaid Source Code</b></summary>

```mermaid
flowchart LR
    classDef pcoll fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8,font-weight:bold;
    classDef ptrans fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#b06000,font-weight:bold;
    classDef sink fill:#e6f4ea,stroke:#34a853,stroke-width:2px,color:#137333,font-weight:bold;
    classDef dlq fill:#fce8e6,stroke:#ea4335,stroke-width:2px,color:#c5221f,font-weight:bold;
    classDef win fill:#f3e8fd,stroke:#9334e6,stroke-width:2px,color:#6b21a8,font-weight:bold;

    subgraph StreamIngest["1. Ingestion & Validation"]
        PUBSUB[("Pub/Sub Topic<br/>telemetry-events")]:::pcoll
        READ["'ReadFromPubSub'"]:::ptrans
        PARSE["'ParseAndValidate'<br/><i>TimestampedValue</i>"]:::ptrans
        VALID[("Valid Stream")]:::pcoll
        DLQ_RECS[("DLQ Records")]:::dlq
        DLQ_SINK[("Dead-Letter Sink")]:::dlq
        PUBSUB --> READ --> PARSE
        PARSE --> VALID
        PARSE -- "Malformed" --> DLQ_RECS --> DLQ_SINK
    end

    subgraph WindowingBranches["2. Temporal Windowing Strategies & Sinks"]
        direction TB
        subgraph BranchA["Branch A: Tumbling Windows (60s)"]
            WIN_TUMB["FixedWindows(60s)<br/><i>Trigger + Allowed Lateness</i>"]:::win
            COMB_TUMB["CombinePerKey<br/>'CombineTumbling'"]:::ptrans
            BQ_TUMB[("BigQuery: kpi_tumbling")]:::sink
            WIN_TUMB --> COMB_TUMB --> BQ_TUMB
        end

        subgraph BranchB["Branch B: Sliding Windows (60s, period=20s)"]
            WIN_SLIDE["SlidingWindows(60s, 20s)<br/><i>Rolling Trend Rate</i>"]:::win
            COMB_SLIDE["CombinePerKey<br/>'CombineSliding'"]:::ptrans
            BQ_SLIDE[("BigQuery: kpi_sliding")]:::sink
            WIN_SLIDE --> COMB_SLIDE --> BQ_SLIDE
        end

        subgraph BranchC["Branch C: Session Windows (30s Gap)"]
            WIN_SESS["Sessions(30s)<br/><i>Activity Bursts</i>"]:::win
            COMB_SESS["GroupByKey + Aggregate<br/>'AggregateSession'"]:::ptrans
            BQ_SESS[("BigQuery: kpi_session")]:::sink
            WIN_SESS --> COMB_SESS --> BQ_SESS
        end
    end

    VALID --> WIN_TUMB
    VALID --> WIN_SLIDE
    VALID --> WIN_SESS
```
</details>

---

## Core Streaming Concepts: The 4 Questions

Stream processing in Apache Beam is governed by Tyler Akidau’s foundational model answering four questions:

| Question | Beam Primitive | Meaning |
| :--- | :--- | :--- |
| **1. What is being computed?** | `beam.ParDo`, `CombinePerKey` | Transformations, filters, and mathematical aggregations (e.g. sum, count, average). |
| **2. Where in event-time is it computed?** | `beam.WindowInto(WindowFn)` | Temporal partitioning: Fixed, Sliding, or Session Windows. |
| **3. When in processing-time are results materialized?** | `trigger.TriggerFn` | The Watermark, speculative early triggers, and late firing conditions. |
| **4. How do results relate?** | `AccumulationMode` | `ACCUMULATING` (overwriting with cumulative totals) vs `DISCARDING` (emitting delta differences). |

---

## Event Time vs. Processing Time

1. **Event Time ($t_E$)**: The exact instant an event occurred on the source device or client (e.g. mobile app click, IoT sensor reading). Embedded in the payload.
2. **Ingestion Time ($t_I$)**: The instant Google Cloud Pub/Sub received and acknowledged the message.
3. **Processing Time ($t_P$)**: The clock time on the Dataflow worker VM currently executing the transformation.

```text
[Device Click]  ──────>  [Pub/Sub Ingestion]  ──────>  [Dataflow Worker]
  Event Time               Ingestion Time               Processing Time
 10:00:05 UTC               10:00:18 UTC                 10:00:22 UTC
```

Due to network latency, offline mobile apps, and retry loops, event time and processing time diverge. **A correct streaming pipeline must ALWAYS partition data by Event Time, never by processing time.**

---

## The Three Windowing Topologies

### 1. Fixed (Tumbling) Windows
Non-overlapping, fixed-duration time intervals (e.g., 60-second intervals):
- `[10:00:00 - 10:01:00)`, `[10:01:00 - 10:02:00)`
- Each element belongs to exactly **one** window.
- Perfect for standard periodic financial and operational metric rollups.

### 2. Sliding (Hopping) Windows
Overlapping time intervals defined by **Duration** and **Slide Period** (e.g., 60s window, sliding every 20s):
- `[09:59:40 - 10:00:40)`, `[10:00:00 - 10:01:00)`, `[10:00:20 - 10:01:20)`
- Each element belongs to **multiple** windows simultaneously (`Duration / Period` windows).
- Used for moving averages, velocity checks, and high-frequency spike detection.

### 3. Session Windows
Data-driven windows defined by a **gap of inactivity** (e.g., 30s inactivity timeout):
- Windows dynamically expand and merge as events arrive for a given key (`user_id`).
- When a user ceases activity for > 30 seconds, their session closes.
- Essential for web analytics, user funnel drop-off analysis, and clickstream sessionization.

---

## Watermarks, Allowed Lateness & Triggers

### What is a Watermark?
A **Watermark** is a monotonically increasing timestamp representing the system's estimate of event-time completeness. When the watermark reaches timestamp $T$, the pipeline assumes that all data with event time $t \le T$ has already been processed.

### Handling Late Data
Network delays cause records to arrive after the watermark has already advanced past the end of their window.
Beam provides two controls:
1. **Allowed Lateness (`allowed_lateness`)**: A grace period (e.g., 2 minutes) during which windows remain open in state memory. Late elements arriving within this window are processed; elements arriving after allowed lateness are discarded.
2. **Triggers (`AfterWatermark`)**: Dictate when window panes fire:
   - **Early Trigger (`early=AfterCount(3)`)**: Emits speculative preliminary results before the watermark closes the window.
   - **On-Time Trigger**: Emits when the watermark passes the end of the window.
   - **Late Trigger (`late=AfterCount(1)`)**: Emits an updated revision every time a late record arrives.

---

## Google Cloud Pub/Sub with Ordering Keys

Google Cloud Pub/Sub partitions messages using an **Ordering Key** (e.g., `user_id` or `device_id`).
When ordering keys are enabled:
- Messages published with the same ordering key are guaranteed to be delivered to subscribers in the exact order they were received by Pub/Sub.
- If a subscriber fails to acknowledge a message for a given key, subsequent messages for that key are held until the earlier message is acknowledged or times out.

---

## Chapter Directory Structure

```text
03_realtime_streaming/
├── README.md                                         # This documentation
├── 03_realtime_streaming.ipynb                       # Interactive notebook with runnable cells
├── data/
│   └── sample_events.json                            # Synthetic telemetry events (valid, late, poison)
├── pipelines/
│   └── streaming_windowing_pipeline.py               # Dual-runner streaming windowing pipeline
└── scripts/
    ├── generate_streaming_data.py                    # Pub/Sub & local streaming event generator
    ├── 01_run_local.sh                               # Local DirectRunner test script
    └── 02_run_dataflow.sh                            # Cloud DataflowRunner deployment script
```

---

## Step-by-Step Hands-On Guide

### Step 1: Activate the Environment

Ensure you are in the workshop root directory with your `uv` virtual environment active:

```bash
cd /Users/zelda.luconi/Medium/google-products/dataflow-workshops
source .venv/bin/activate
```

### Step 2: Run the Pipeline Locally (DirectRunner)

Execute the local runner script to run the multi-branch pipeline against synthetic streaming telemetry:

```bash
bash 03_realtime_streaming/scripts/01_run_local.sh
```

**Expected Local Output:**
1. `tumbling_metrics-*.json`: Aggregated spend and event count for each 60-second window per user.
2. `sliding_metrics-*.json`: Rolling 60s windows shifted every 20 seconds.
3. `session_metrics-*.json`: Session start, session end, duration, and metrics clustered by 30-second user inactivity gaps.
4. `dead_letters-*.json`: Poison records (negative amount, empty user ID, malformed JSON) tagged and isolated without pipeline crash!

### Step 3: Run the Streaming Generator (Optional Live Simulator)

To simulate continuous live event publishing to a GCP Pub/Sub topic:

```bash
uv run python 03_realtime_streaming/scripts/generate_streaming_data.py \
  --project="YOUR_PROJECT_ID" \
  --topic="telemetry-events-topic" \
  --rate=3.0 \
  --total-events=150 \
  --late-rate=0.15 \
  --poison-rate=0.10
```

### Step 4: Deploy to Google Cloud Dataflow

To deploy the production streaming pipeline with **Dataflow Streaming Engine** and **BigQuery Storage Write API**:

```bash
bash 03_realtime_streaming/scripts/02_run_dataflow.sh
```

Monitor your streaming job in the Cloud Console:
- **Dataflow Console**: Observe **System Lag** and **Data Freshness** metrics.
- **BigQuery Console**: Query `kpi_tumbling_metrics`, `kpi_sliding_metrics`, `kpi_session_metrics`, and `dead_letter_queue`.

---

## Official Documentation & Deep Dives

* [Apache Beam Windowing Basics](https://beam.apache.org/documentation/programming-guide/#windowing) — Fixed (tumbling), sliding (hopping), and session window concepts.
* [Apache Beam Triggers & Allowed Lateness](https://beam.apache.org/documentation/programming-guide/#triggers) — Speculative early triggers, on-time watermarks, and late accumulation modes.
* [Streaming 101: The World Beyond Batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/) — Tyler Akidau's seminal introduction to streaming data processing.
* [Streaming 102: The World Beyond Batch](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/) — The 4 questions: What, Where, When, and How.
* [Google Cloud Pub/Sub Message Ordering](https://cloud.google.com/pubsub/docs/ordering) — Delivering messages in order using Pub/Sub ordering keys.
* [Google Cloud Dataflow Streaming Overview](https://cloud.google.com/dataflow/docs/concepts/streaming-pipelines) — Executing unbounded streaming pipelines with Streaming Engine.

