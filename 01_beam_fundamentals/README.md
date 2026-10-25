# Chapter 01 — Apache Beam & Dataflow Fundamentals

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
flowchart TD
    classDef pcoll fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8,font-weight:bold;
    classDef ptrans fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#b06000,font-weight:bold;

    RAW[("[( sales_data.csv )]<br/>Local Disk / Cloud Storage")]:::pcoll
    READ["PTransform: beam.io.ReadFromText<br/>Name: 'ReadCSV'"]:::ptrans
    PC_RAW[("[( PCollection&lt;str&gt; )]<br/>Raw CSV lines")]:::pcoll
    PARSE["PTransform: beam.ParDo(ParseSalesRecordFn)<br/>Name: 'ParseRecords'"]:::ptrans
    PC_PARSED[("[( PCollection&lt;dict&gt; )]<br/>Parsed sales records")]:::pcoll
    FILTER["PTransform: beam.ParDo(FilterByDateFn)<br/>Name: 'FilterDate'"]:::ptrans
    PC_FILTERED[("[( PCollection&lt;dict&gt; )]<br/>Records for 2025-06-12")]:::pcoll
    MAP["PTransform: beam.Map(extract_kv)<br/>Name: 'ExtractProductAmount'"]:::ptrans
    PC_KV[("[( PCollection&lt;tuple&gt; )]<br/>(product_id, amount) pairs")]:::pcoll
    COMBINE["PTransform: beam.CombinePerKey(sum)<br/>Name: 'AggregateRevenuePerProduct'"]:::ptrans
    PC_AGG[("[( PCollection&lt;tuple&gt; )]<br/>(product_id, total_revenue)")]:::pcoll
    FORMAT["PTransform: beam.Map(format_output)<br/>Name: 'FormatOutput'"]:::ptrans
    PC_STR[("[( PCollection&lt;str&gt; )]<br/>Formatted summary strings")]:::pcoll
    WRITE["PTransform: beam.io.WriteToText<br/>Name: 'WriteResults'"]:::ptrans
    OUT[("[( Partitioned Text Files )]<br/>revenue_summary-*.txt")]:::pcoll

    RAW --> READ
    READ --> PC_RAW
    PC_RAW --> PARSE
    PARSE --> PC_PARSED
    PC_PARSED --> FILTER
    FILTER --> PC_FILTERED
    PC_FILTERED --> MAP
    MAP --> PC_KV
    PC_KV --> COMBINE
    COMBINE --> PC_AGG
    PC_AGG --> FORMAT
    FORMAT --> PC_STR
    PC_STR --> WRITE
    WRITE --> OUT
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

