# Chapter 04 — Stateful Processing, Timers & Hot Key Fanout

[![Apache Beam](https://img.shields.io/badge/Apache_Beam-2.56+-FF6F00?logo=apachebeam&logoColor=white)](https://beam.apache.org/)
[![Google Cloud Dataflow](https://img.shields.io/badge/Google_Cloud-Dataflow-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/dataflow)
[![Google BigQuery](https://img.shields.io/badge/Google_Cloud-BigQuery-669DF6?logo=googlecloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)

In high-throughput stream processing, two critical operational hurdles routinely crash production data pipelines:
1. **The Hot Key Bottleneck (Data Skew)**: A single entity (e.g., a viral celebrity, high-frequency IoT sensor, or crawler bot) emits orders of magnitude more events than typical entities. Standard distributed hashing routes all elements of that key to a **single worker**, causing out-of-memory (OOM) errors, stranded pipelines, and the dreaded Dataflow error:
   ```text
   Workflow failed. Causes: A hot key was detected in step...
   ```
2. **Unbounded State Management & Leakage**: Maintaining per-user or per-device state (rolling counters, session profiles) without external databases requires Beam's **Stateful Processing API**. However, without automated garbage collection via **Timers (`@TimerId`)**, worker memory grows indefinitely until the pipeline crashes.

This chapter covers the architecture and implementation of **Stateful DoFns**, **Automated State Expiration Timers**, and **Hot Key Fanout Optimization (`with_hot_key_fanout`)**.

---

## Table of Contents

- [Real-World Production Problem & Storyline](#real-world-production-problem--storyline)
- [Architecture Overview](#architecture-overview)
- [1. The Hot Key Problem: Why Increasing Workers Fails](#1-the-hot-key-problem-why-increasing-workers-fails)
  - [The Flawed Anti-Pattern: "Add More Workers"](#the-flawed-anti-pattern-add-more-workers)
  - [The Solution: `with_hot_key_fanout`](#the-solution-with_hot_key_fanout)
- [2. Stateful Stream Processing with `@StateId`](#2-stateful-stream-processing-with-stateid)
- [3. Automated State Expiration with `@TimerId`](#3-automated-state-expiration-with-timerid)
  - [Setting the Timer](#setting-the-timer)
  - [The Timer Callback (`@on_timer`)](#the-timer-callback-on_timer)
- [Hands-On Execution Guide](#hands-on-execution-guide)
  - [Step 1: Run Locally (DirectRunner)](#step-1-run-locally-directrunner)
  - [Step 2: Deploy to Google Cloud Dataflow](#step-2-deploy-to-google-cloud-dataflow)
- [Official Documentation & Deep Dives](#official-documentation--deep-dives)

---

## Real-World Production Problem & Storyline

### The Scenario: The Celebrity Influencer Disaster & Session Leaks
At 11:00 AM, a mega-influencer with 80 million followers shares an OmniRetail promotional link. Within 15 seconds, platform traffic spikes by 20x, but 80% of all events are associated with a single key (`usr_hot_influencer`). Simultaneously, the product team deploys a shopping cart tracker to calculate per-user rolling spend and page history without querying an external database.

### The Production Disaster (Without Hot Key Fanout & Timers)
1. **The Hot Key Crash**: Standard stream engines route elements with the same key to the same worker to preserve state. When 100,000 events/second share the influencer key, Dataflow routes all 100,000 events to a **single worker VM**. That worker's CPU spikes to 100%, memory explodes, and Dataflow throws:
   ```text
   Workflow failed. Causes: A hot key was detected in step...
   ```
   Auto-scaling fails to resolve this: spinning up 50 new worker VMs does nothing because all 50 workers sit idle while the single hot worker suffocates!
2. **The Unbounded State Memory Leak**: Storing per-user cart history in worker state without automated cleanup means millions of guest users who browse once and leave retain state in memory forever, eventually causing worker Out-Of-Memory (OOM) crashes.

### How This Chapter's Demo Solves It
We resolve key skew and stateful stream management using Beam's advanced state API:
- **Hot Key Fanout (`CombinePerKey().withHotKeyFanout(10)`)**: Dynamically salts hot keys with random integers $0..9$, spreading the influencer load across 10 intermediate workers for local pre-combining before merging into the final total, eliminating hot key bottlenecks.
- **Stateful `DoFn` (`@StateId`)**: Tracks per-key accumulators (`ReadModifyWriteStateSpec`) and page click history (`BagStateSpec`) directly in worker memory without database network latency.
- **Automated State Expiration Timers (`@TimerId`)**: Sets a watermark inactivity timer (60s). When a user goes inactive, the `@on_timer` callback flushes the finalized user profile to BigQuery and purges state with `clear()`, permanently preventing memory leaks.

### Storyline Connection
- ⬅️ **Previously (Chapter 03)**: Implemented real-time Pub/Sub streaming with tumbling, sliding, and session windows.
- ➡️ **Next Crisis (Chapter 05)**: Cyber Monday peak hits. The executive dashboard suddenly goes blank, numbers are 20 minutes behind, and persistent disk shuffle is thrashing worker VMs!

---

## Architecture Overview

The pipeline ingests a highly skewed clickstream dataset where a single celebrity account generates 100x more events than typical users, fanning out into two production patterns:

![Chapter 04 Stateful & Fanout DAG](../artifacts/04_stateful_and_fanout_dag.png)

<details>
<summary><b>View Mermaid Source Code</b></summary>

```mermaid
flowchart LR
    classDef pcoll fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8,font-weight:bold;
    classDef ptrans fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#b06000,font-weight:bold;
    classDef sink fill:#e6f4ea,stroke:#34a853,stroke-width:2px,color:#137333,font-weight:bold;
    classDef state fill:#f3e8fd,stroke:#9334e6,stroke-width:2px,color:#6b21a8,font-weight:bold;
    classDef hotkey fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b,font-weight:bold;

    subgraph Ingest["1. Skewed Stream"]
        SKEW_DATA[("skewed_clickstream.json<br/>(100x Key Skew)")]:::hotkey
        PARSE["'ParseAndTimestamp'<br/><i>KV(user_id, event)</i>"]:::ptrans
        SKEW_DATA --> PARSE
    end

    subgraph StatefulEngine["2. Stateful Sessionizer & Timers"]
        STATE_DOFN["'StatefulSessionTracker'<br/><i>ParDo(StatefulSessionizerFn)</i>"]:::state
        STORE["Per-Key State Store<br/>• ReadModifyWriteStateSpec<br/>• BagStateSpec<br/>• TimerSpec(WATERMARK)"]:::state
        BQ_STATE[("BigQuery:<br/>user_session_profiles")]:::sink
        STATE_DOFN <--> STORE
        STATE_DOFN --> BQ_STATE
    end

    subgraph FanoutEngine["3. Hot Key Fanout Mitigation"]
        FANOUT["CombinePerKey.with_hot_key_fanout(10)<br/><i>Phase 1: Salted Sharding</i><br/><i>Phase 2: Global Reduction</i>"]:::hotkey
        BQ_FANOUT[("BigQuery:<br/>skew_aggregated_metrics")]:::sink
        FANOUT --> BQ_FANOUT
    end

    PARSE --> STATE_DOFN
    PARSE --> FANOUT
```
</details>

---

## 1. The Hot Key Problem: Why Increasing Workers Fails

In distributed computing (MapReduce, Spark, Apache Beam, Dataflow), records sharing the same key are routed to the **same worker thread** via consistent hashing:

$$\text{Worker Index} = \text{Hash}(\text{Key}) \pmod{\text{Total Workers}}$$

### The Flawed Anti-Pattern: "Add More Workers"
If `usr_hot_influencer` has 1,000,000 events:
- Increasing worker count from 10 to 1,000 does **not** distribute `usr_hot_influencer`.
- All 1,000,000 events will **still hash to the exact same worker**.
- 999 workers will sit idle while 1 worker runs out of memory, spikes system lag, and aborts the pipeline.

### The Solution: `with_hot_key_fanout`
Apache Beam provides built-in two-phase aggregation via `with_hot_key_fanout(fanout)`:
1. **Phase 1 (Salted Intermediate Sharding)**: Beam appends an internal pseudo-random salt $[0, \text{fanout})$ to the hot key: `(key, salt)`. The work is scattered across `fanout` independent workers, where local partial combination occurs.
2. **Phase 2 (Global Merge)**: Beam strips the salt and combines the `fanout` intermediate accumulators into the final result.

```python
# Apply fanout of 10 shards only to celebrity keys
beam.CombinePerKey(CountAndSumCombineFn()).with_hot_key_fanout(
    lambda key: 10 if ("hot" in key or "celebrity" in key) else 0
)
```

---

## 2. Stateful Stream Processing with `@StateId`

Beam's Stateful Processing API allows a `DoFn` to read, write, and persist per-key state across elements without consulting an external database (such as Redis or Bigtable), slashing I/O latency and infrastructure costs:

| State Spec | Beam Class | Best Used For |
| :--- | :--- | :--- |
| **Scalar Value State** | `ReadModifyWriteStateSpec` | Counters, rolling sums, min/max timestamps, flags. |
| **Bag State** | `BagStateSpec` | Append-only lists (e.g. visited URLs, event history). |
| **Set State** | `SetStateSpec` | Unique item sets (e.g. distinct IP addresses, tags). |
| **Combining Value State** | `CombiningValueStateSpec` | Accumulators that combine elements incrementally using a `CombineFn`. |

---

## 3. Automated State Expiration with `@TimerId`

Without cleanup, state stored in a 24/7 streaming pipeline grows indefinitely, leading to memory leaks and unbounded state disk size on Dataflow Streaming Engine.

### Setting the Timer
In `process()`, schedule a timer whenever an event arrives:
```python
# Reset inactivity timer to fire 60 seconds of event-time in the future
timer.set(element_timestamp + 60)
```

### The Timer Callback (`@on_timer`)
When the watermark passes the scheduled timestamp, Beam invokes the callback:
```python
@on_timer(INACTIVITY_TIMER)
def on_session_inactivity(
    self,
    key=beam.DoFn.KeyParam,
    count_state=beam.DoFn.StateParam(EVENT_COUNT_STATE),
    spend_state=beam.DoFn.StateParam(TOTAL_SPEND_STATE),
    ...
):
    # 1. Emit finalized aggregation
    yield {"user_id": key, "events": count_state.read(), ...}
    
    # 2. CRITICAL: Clean up state to reclaim memory
    count_state.clear()
    spend_state.clear()
```

---

## Hands-On Execution Guide

### Step 1: Run Locally (DirectRunner)

Execute the local runner script to run the stateful pipeline against the skewed clickstream dataset:

```bash
bash 04_stateful_processing_and_hotkeys/scripts/01_run_local.sh
```

**Expected Local Output:**
1. `stateful_sessions-*.json`: Finalized user profiles emitted by the timer callback, including the anomaly flag:
   ```json
   {"user_id": "usr_hot_influencer", "session_duration_sec": 120, "total_events": 500, "is_anomaly_velocity": true}
   {"user_id": "usr_alice", "session_duration_sec": 162, "total_events": 5, "is_anomaly_velocity": false}
   ```
2. `fanout_metrics-*.json`: Aggregated metrics combined across intermediate fanout shards:
   ```json
   {"user_id": "usr_hot_influencer", "total_events": 500, "total_spend": 0.0}
   {"user_id": "usr_david", "total_events": 5, "total_spend": 228.53}
   ```

### Step 2: Deploy to Google Cloud Dataflow

Submit the pipeline to Dataflow with BigQuery Storage Write API:

```bash
bash 04_stateful_processing_and_hotkeys/scripts/02_run_dataflow.sh
```

Inspect the BigQuery output tables:
- `user_session_profiles`: Real-time session profiles emitted upon timer expiration.
- `fanout_user_metrics`: Metrics aggregated without hot key bottlenecks.

---

## Official Documentation & Deep Dives

* [Apache Beam Stateful Processing](https://beam.apache.org/documentation/programming-guide/#state-and-timers) — Using ValueState, BagState, and ReadModifyWriteStateSpec.
* [Timely and Stateful Processing with Apache Beam](https://beam.apache.org/blog/timely-processing/) — State TTL, event-time vs. processing-time timers, and garbage collection.
* [Google Cloud Dataflow: Preventing Hot Keys & withHotKeyFanout](https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#preventing-hot-keys) — Resolving data skew bottlenecks and out-of-memory errors.
* [Apache Beam Python State & Timers API](https://beam.apache.org/documentation/programming-guide/#state-timers-python) — Code examples and type signatures for Python SDK user state.

