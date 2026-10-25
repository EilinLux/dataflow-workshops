# Chapter 01 — Apache Beam & Dataflow Fundamentals

## Table of Contents

- [Overview](#overview)
  - [What You Will Learn](#what-you-will-learn)
- [Real-World Production Problem & Storyline](#real-world-production-problem--storyline)
- [Chapter Contents](#chapter-contents)
- [Pipeline Architecture & DAG Syntax](#pipeline-architecture--dag-syntax)
  - [Visual Shape Legend](#visual-shape-legend)
  - [What Does the "Unique Transform Name" Stand For?](#what-does-the-unique-transform-name-stand-for)
- [Environment Setup & Activating the Kernel](#environment-setup--activating-the-kernel)
- [Quick Start: Running the Pipeline](#quick-start-running-the-pipeline)
  - [1. Run Locally with DirectRunner](#1-run-locally-with-directrunner)
  - [2. Run on Google Cloud Dataflow](#2-run-on-google-cloud-dataflow)
- [Official Documentation & Deep Dives](#official-documentation--deep-dives)

---

## Overview

In this chapter, we introduce the core concepts of the **Apache Beam** unified programming model and explore how **Google Cloud Dataflow** executes Beam pipelines at scale. 

You will build your first data pipeline processing the **Retail Sales** dataset (`sales_data.csv`), test it locally using **`DirectRunner`**, and submit it to Google Cloud's serverless infrastructure using **`DataflowRunner`**.

---

### What You Will Learn

1. **The Apache Beam Philosophy**:
   * How Beam unifies **Batch** and **Streaming** into a single API.
   * The separation between pipeline authoring (Beam SDK) and pipeline execution (Runners).
2. **Core Abstractions**:
   * **`Pipeline`**: The directed graph managing execution flow and configuration.
   * **`PCollection`**: Immutable distributed datasets (bounded or unbounded).
   * **`PTransform`**: Operations applied to `PCollections` using the pipe syntax (`|`).
   * **`DoFn` / `beam.ParDo`**: User-defined element-wise transformation functions.
   * **`CombinePerKey`**: Associative and commutative aggregations with partial local combine.
3. **Execution Environments (Runners)**:
   * **`DirectRunner`**: Local execution for fast iteration, debugging, and unit tests without cloud costs.
   * **`DataflowRunner`**: Managed distributed execution on Google Cloud with auto-scaling and dynamic work rebalancing.

---

## Real-World Production Problem & Storyline

### The Scenario: The Midnight Batch Breakdown
OmniRetail operates 500 retail stores nationwide. Every midnight, cash registers dump raw CSV transaction logs into Google Cloud Storage (`data/sales_data.csv`). The CFO and finance leadership require an aggregated daily revenue report by product category by 6:00 AM for morning inventory restocking and supplier settlements.

### The Production Disaster (Without Apache Beam)
Previously, the engineering team executed a single-node Python script using pandas on a Compute Engine VM. As business volume scaled to tens of millions of transactions:
1. **The `MemoryError` Wall**: Loading multi-gigabyte CSV files into pandas DataFrames consumed all VM RAM, crashing with unhandled Out-Of-Memory (OOM) errors at 3:30 AM.
2. **Zero Fault Tolerance**: When a 3-hour job fails at 90% completion, all intermediate progress is lost and the entire job must restart from scratch, guaranteeing a missed 6:00 AM SLA.
3. **No Elastic Scaling**: Scaling up required provisioning a massive `m2-ultramem` instance that sat idle for 20 hours a day, inflating cloud bills.

### How This Chapter's Demo Solves It
We replace the fragile single-node script with a distributed **Apache Beam** batch pipeline:
- **`beam.io.ReadFromText`**: Reads massive files in parallel chunks without exhausting worker memory.
- **Custom `DoFn` (`ParseSalesRecordFn`, `FilterByDateFn`)**: Parses records into typed dictionaries and isolates business logic.
- **`CombinePerKey(sum)`**: Executes partial combiner aggregation locally on worker VMs before distributed shuffle, slashing network transfer.
- **Run Anywhere**: Developers test locally on workstation sample data with **`DirectRunner`**, then deploy to **`DataflowRunner`** where Google Cloud provisions, balances, and tears down worker VMs automatically.

### Storyline Connection
- ➡️ **Next Crisis (Chapter 02)**: Batch scaling is solved, but marketing introduces an auxiliary product dimension catalog, and cash register glitches inject corrupt records with negative subtotals that silently crash the nightly run!

---

## Chapter Contents

| File / Directory | Description |
| :--- | :--- |
| [`01_beam_fundamentals.ipynb`](./01_beam_fundamentals.ipynb) | Interactive Jupyter notebook walking step-by-step through Beam fundamentals, transforms, and execution. |
| [`data/sales_data.csv`](./data/sales_data.csv) | Sample retail transactions dataset (`transaction_id, product_id, amount, transaction_date`). |
| [`pipelines/sales_pipeline.py`](./pipelines/sales_pipeline.py) | Modular, production-ready Beam pipeline script runnable with `DirectRunner` or `DataflowRunner`. |
| [`scripts/01_run_local.sh`](./scripts/01_run_local.sh) | Shell script to execute the pipeline locally using `uv` and `DirectRunner`. |
| [`scripts/02_run_dataflow.sh`](./scripts/02_run_dataflow.sh) | Shell script to upload data to GCS and launch the job on Google Cloud Dataflow. |

---

## Pipeline Architecture & DAG Syntax

In Apache Beam, pipelines are constructed using the pipe operator (`|`) and the right-shift operator (`>>`):
```python
[Output PCollection] = [Input PCollection] | "Unique Transform Name" >> [PTransform]
```

### Visual Shape Legend
* **🛢️ `[( PCollection )]` (Cylinder / Oval)**: Represents immutable distributed **data** (in-flight records).
* **⚙️ `[ PTransform ]` (Rectangle)**: Represents the **processing operations** that consume and produce `PCollections`.

### What Does the "Unique Transform Name" Stand For?
In `p | "FilterDate" >> beam.ParDo(...)`, the string `"FilterDate"` is a critical runtime identifier:
1. **DAG Graph Node Label**: Apache Beam registers this string as the unique node identifier in the execution DAG.
2. **Google Cloud Dataflow Console Stage**: The GCP Console visualizes each pipeline step using this label, allowing you to click on stages to inspect worker throughput, execution latency, and bottlenecks.
3. **Cloud Monitoring & Logging Scope**: All Dataflow metrics (counters, execution wall time, CPU utilization) are indexed by this step name in Cloud Monitoring.
4. **Uniqueness Constraint**: Every transform name **must be unique** within its scope. Reusing a name raises `ValueError: A transform with name ... already exists in this pipeline`.
5. **Streaming Checkpoints**: For streaming jobs, Dataflow keys window buffers and timers to these names. Preserving names ensures state continuity during rolling pipeline updates (`--update`).

![Retail Sales Pipeline DAG](../artifacts/01_sales_pipeline_dag.png)

```mermaid
flowchart LR
    classDef pcoll fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8,font-weight:bold;
    classDef ptrans fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#b06000,font-weight:bold;

    subgraph S1["1. Ingestion"]
        RAW[("sales_data.csv")]:::pcoll
        T_READ["'ReadCSV'<br/><i>ReadFromText</i>"]:::ptrans
        PC_RAW[("Raw Lines")]:::pcoll
        RAW --> T_READ --> PC_RAW
    end

    subgraph S2["2. Parse & Filter"]
        T_PARSE["'ParseRecords'<br/><i>ParseSalesRecordFn</i>"]:::ptrans
        PC_PARSED[("Parsed Dicts")]:::pcoll
        T_FILTER["'FilterDate'<br/><i>FilterByDateFn</i>"]:::ptrans
        PC_FILTERED[("Target Date")]:::pcoll
        T_PARSE --> PC_PARSED --> T_FILTER --> PC_FILTERED
    end

    subgraph S3["3. Aggregate"]
        T_MAP["'ExtractProductAmount'<br/><i>Map(extract_kv)</i>"]:::ptrans
        PC_KV[("(product_id, amount)")]:::pcoll
        T_COMBINE["'AggregateRevenue'<br/><i>CombinePerKey(sum)</i>"]:::ptrans
        PC_AGG[("(product_id, total)")]:::pcoll
        T_MAP --> PC_KV --> T_COMBINE --> PC_AGG
    end

    subgraph S4["4. Format & Sink"]
        T_FORMAT["'FormatOutput'<br/><i>Map(format_output)</i>"]:::ptrans
        PC_STR[("Formatted Strings")]:::pcoll
        T_WRITE["'WriteResults'<br/><i>WriteToText</i>"]:::ptrans
        OUT[("revenue_summary-*.txt")]:::pcoll
        T_FORMAT --> PC_STR --> T_WRITE --> OUT
    end

    PC_RAW --> T_PARSE
    PC_FILTERED --> T_MAP
    PC_AGG --> T_FORMAT
```

---

## Environment Setup & Activating the Kernel

Before running scripts or opening the notebook, set up the project environment:

```bash
# 1. From the repository root, install dependencies into .venv
uv sync

# 2. Activate the virtual environment in your terminal (optional, if running commands directly)
source .venv/bin/activate

# 3. Register the .venv as a Jupyter kernel (prevents ModuleNotFoundError in notebooks)
uv run python -m ipykernel install --user --name dataflow-workshops --display-name "Python (dataflow-workshops)"
```

When opening [`01_beam_fundamentals.ipynb`](./01_beam_fundamentals.ipynb), click the **Kernel Picker** in the top-right corner of your IDE and select **`Python (dataflow-workshops)`** (or `.venv/bin/python`).

---

## Quick Start: Running the Pipeline

### 1. Run Locally with DirectRunner
Verify pipeline logic on your local machine in seconds:
```bash
bash 01_beam_fundamentals/scripts/01_run_local.sh
```
Expected output:
```text
product_id=PROD_A, total_revenue=275.74
product_id=PROD_B, total_revenue=510.25
product_id=PROD_C, total_revenue=300.50
```

### 2. Run on Google Cloud Dataflow
Ensure you have configured your `.env` file with `GCP_PROJECT_ID` and `DATAFLOW_BUCKET`, then run:
```bash
bash 01_beam_fundamentals/scripts/02_run_dataflow.sh
```
This script:
1. Uploads `data/sales_data.csv` to `gs://${DATAFLOW_BUCKET}/data/sales_data.csv`.
2. Submits the job to Dataflow with `--runner=DataflowRunner`.
3. Returns a direct link to monitor the job execution graph, worker scaling, and throughput in the Google Cloud Console.

---

## Official Documentation & Deep Dives

* [Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) — Comprehensive guide to the Apache Beam unified batch and streaming programming model.
* [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/) — Official getting started guide for Python Beam.
* [Google Cloud Dataflow Documentation](https://cloud.google.com/dataflow/docs) — Full documentation for Google Cloud's fully-managed serverless data processing service.
* [Apache Beam DirectRunner Guide](https://beam.apache.org/documentation/runners/direct/) — Local pipeline execution and debugging guide.
* [Google Cloud Dataflow Runner Guide](https://beam.apache.org/documentation/runners/dataflow/) — Deploying Beam pipelines on Cloud Dataflow.
* [Dataflow Troubleshooting & Best Practices](https://cloud.google.com/dataflow/docs/guides/common-troubleshooting-techniques) — Identifying pipeline bottlenecks, worker logs, and common pitfalls.

