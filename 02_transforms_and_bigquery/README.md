# Chapter 02 — Advanced Transforms, Side Inputs & BigQuery I/O

## Overview

In production data engineering, raw inputs are rarely clean, and business logic often requires enriching in-flight records against dimension tables (such as product catalogs, customer tiers, or currency exchange rates). Furthermore, a single corrupt or unparseable record must never halt a 24/7 mission-critical pipeline or be silently discarded.

In this chapter, you will master advanced **Apache Beam** patterns and deploy a robust data validation and enrichment engine to **Google Cloud Dataflow** and **BigQuery**:

1. **`DoFn` Lifecycle Methods**: Optimizing worker resource initialization (`setup`, `process`, `teardown`).
2. **Side Inputs (`beam.pvalue.AsDict`)**: Broadcasting reference lookup data without expensive distributed shuffles.
3. **Tagged Multi-Outputs & Dead-Letter Queues (DLQ)**: Branching records into clean data vs. poison records using `beam.pvalue.TaggedOutput`.
4. **Google Cloud BigQuery Storage Write API**: High-throughput, cost-effective, exactly-once ingestion into BigQuery tables with automated schema creation.

---

## Pipeline Architecture & DAG

The pipeline ingests raw transactions containing both valid and intentionally corrupted records (`sales_data_with_errors.csv`) alongside a product dimension catalog (`product_catalog.json`). Clean records are enriched and written to **BigQuery**, while poison records are captured in a **Cloud Storage Dead-Letter Queue**:

![Chapter 02 Pipeline Architecture](../artifacts/02_side_inputs_and_bq_dag.png)

<details>
<summary><b>Click to expand raw Mermaid flowchart definition</b></summary>

```mermaid
flowchart TD
    classDef pcoll fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a73e8,font-weight:bold;
    classDef ptrans fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#b06000,font-weight:bold;
    classDef sink fill:#e6f4ea,stroke:#34a853,stroke-width:2px,color:#137333,font-weight:bold;
    classDef dlq fill:#fce8e6,stroke:#ea4335,stroke-width:2px,color:#c5221f,font-weight:bold;

    subgraph Inputs["1. Raw Ingestion"]
        SALES_CSV[("[( sales_data_with_errors.csv )]<br/>Raw Transactions (with errors)")]:::pcoll
        CATALOG_JSON[("[( product_catalog.json )]<br/>Reference Product Dimension")]:::pcoll
    end

    subgraph SideInput["2. Side Input Creation"]
        T_CAT_READ["PTransform: beam.io.ReadFromText<br/>Name: 'ReadCatalog'"]:::ptrans
        T_CAT_PARSE["PTransform: beam.ParDo(ParseCatalogFn)<br/>Name: 'ParseCatalog'"]:::ptrans
        PC_CATALOG[("[( PCollection&lt;tuple&gt; )]<br/>(product_id, metadata)")]:::pcoll
    end

    subgraph Core["3. Processing & Validation Engine"]
        T_SALES_READ["PTransform: beam.io.ReadFromText<br/>Name: 'ReadSales'"]:::ptrans
        PC_RAW_SALES[("[( PCollection&lt;str&gt; )]<br/>Raw CSV Lines")]:::pcoll
        T_ENRICH["PTransform: beam.ParDo(ValidateAndEnrichFn)<br/>Name: 'ValidateAndEnrich'"]:::ptrans
    end

    subgraph Outputs["4. Dual-Sink Routing"]
        PC_VALID[("[( PCollection&lt;dict&gt; (Main Output) )]<br/>Enriched Valid Sales")]:::pcoll
        PC_DLQ[("[( PCollection&lt;dict&gt; ('dead_letter' Tag) )]<br/>Poison Records")]:::dlq
        
        T_BQ["PTransform: beam.io.WriteToBigQuery<br/>Name: 'WriteToBigQuery' (Storage Write API)"]:::ptrans
        BQ_TABLE[("[( BigQuery Table )]<br/>curated_sales")]:::sink
        
        T_DLQ["PTransform: beam.io.WriteToText<br/>Name: 'WriteDLQ'"]:::ptrans
        DLQ_FILES[("[( Cloud Storage DLQ )]<br/>dead_letter_records-*.json")]:::dlq
    end

    CATALOG_JSON --> T_CAT_READ
    T_CAT_READ --> T_CAT_PARSE
    T_CAT_PARSE --> PC_CATALOG
    
    SALES_CSV --> T_SALES_READ
    T_SALES_READ --> PC_RAW_SALES
    PC_RAW_SALES --> T_ENRICH

    PC_CATALOG -. "Side Input: beam.pvalue.AsDict" .-> T_ENRICH

    T_ENRICH -- "Main Output" --> PC_VALID
    T_ENRICH -- "Tagged Output ('dead_letter')" --> PC_DLQ

    PC_VALID --> T_BQ
    T_BQ --> BQ_TABLE

    PC_DLQ --> T_DLQ
    T_DLQ --> DLQ_FILES
```
</details>

---

## Core Concepts Deep Dive

### 1. `DoFn` Lifecycle Methods

In distributed runtimes like Dataflow, worker VMs run multiple processing threads. Understanding the `DoFn` lifecycle prevents severe performance bottlenecks:

| Method | Invocation Frequency | Best Used For | Anti-Pattern to Avoid |
| :--- | :--- | :--- | :--- |
| **`setup(self)`** | Once per worker instance upon initialization | Compiling regexes, opening database connections, initializing ML model weights | Never perform per-element logic here |
| **`start_bundle(self)`** | Once before processing a batch of elements | Opening temporary in-memory buffers or local accumulators | Heavy remote API calls |
| **`process(self, element)`** | Once for every single incoming record | Applying transformations, yields elements | **Never open network sockets or database connections here!** |
| **`finish_bundle(self)`** | Once after processing a batch of elements | Flushing buffered writes | Blocking indefinitely |
| **`teardown(self)`** | Once when worker shuts down | Closing database connections, client cleanups | Emitting data |

### 2. Side Inputs (`beam.pvalue.AsDict`)

When you need to join a massive main stream (`PCollection`) with a reference dimension table (like `product_catalog.json`):
* Performing a distributed `CoGroupByKey` forces Dataflow to shuffle both collections across the entire network—consuming massive network bandwidth and disk I/O.
* A **Side Input** broadcasts the smaller reference collection to worker memory. Each worker receives the lookup table as an in-memory dictionary (`beam.pvalue.AsDict`) accessible in $O(1)$ time inside the `process()` method:

```python
# Pass catalog PCollection as a dictionary side input to ParDo
results = sales_pcoll | "Enrich" >> beam.ParDo(
    ValidateAndEnrichFn(),
    catalog=beam.pvalue.AsDict(catalog_pcoll)
)
```

### 3. Tagged Multi-Outputs & Dead-Letter Queues (DLQ)

In a standard transform, `yield element` emits to the default `PCollection`. Using `beam.pvalue.TaggedOutput`, you can branch execution into multiple streams based on data quality:

```python
# Route valid records to default stream, poison records to 'dead_letter'
if is_valid(element):
    yield enriched_element
else:
    yield beam.pvalue.TaggedOutput("dead_letter", {
        "raw_line": element,
        "error_reason": "Missing product_id",
        "timestamp": now()
    })
```

You then unpack the outputs using `.with_outputs("dead_letter", main="valid_sales")` and attach distinct sinks to each stream!

### 4. BigQuery Storage Write API

Dataflow's `beam.io.WriteToBigQuery` supports modern high-performance ingestion:
* **`Method.STORAGE_WRITE_API`**: Uses Google Cloud's gRPC-based BigQuery Storage Write API.
* **Cost Efficiency**: Eliminates legacy streaming insert costs ($0.05 per GB) while achieving significantly higher throughput.
* **Schema Handling**: Setting `create_disposition=CREATE_IF_NEEDED` automatically provisions the BigQuery dataset and table schema if it does not already exist.

---

## Chapter Contents

| File / Directory | Description |
| :--- | :--- |
| [`02_transforms_and_bigquery.ipynb`](./02_transforms_and_bigquery.ipynb) | Interactive Jupyter notebook demonstrating `DoFn` lifecycle, Side Inputs, DLQs, and BigQuery loading. |
| [`data/sales_data_with_errors.csv`](./data/sales_data_with_errors.csv) | Sales dataset with valid transactions + intentional poison rows (negative amounts, missing IDs, bad dates). |
| [`data/product_catalog.json`](./data/product_catalog.json) | Product dimension catalog with metadata (`product_name`, `category`, `unit_cost`) used for Side Input lookup. |
| [`pipelines/enrich_and_load_bq.py`](./pipelines/enrich_and_load_bq.py) | Production-ready pipeline script supporting local JSON sinks and BigQuery Storage Write API sinks. |
| [`scripts/01_run_local.sh`](./scripts/01_run_local.sh) | Local execution script (`DirectRunner`) verifying Side Input enrichment and dead-letter queue routing. |
| [`scripts/02_run_dataflow.sh`](./scripts/02_run_dataflow.sh) | Cloud submission script (`DataflowRunner`) loading clean data into BigQuery and errors into Cloud Storage DLQ. |

---

## Environment Setup & Activating the Kernel

Before executing scripts or running notebook cells, activate the environment:

```bash
# 1. From repository root, install dependencies into .venv
uv sync

# 2. Activate virtual environment in terminal
source .venv/bin/activate

# 3. Register Jupyter kernel (if not already done)
uv run python -m ipykernel install --user --name dataflow-workshops --display-name "Python (dataflow-workshops)"
```

In [`02_transforms_and_bigquery.ipynb`](./02_transforms_and_bigquery.ipynb), select **`Python (dataflow-workshops)`** from the top-right Kernel Picker.

---

## Quick Start: Running the Pipeline

### 1. Run Locally with DirectRunner
Test the validation rules, Side Input lookups, and Dead-Letter branching locally:
```bash
bash 02_transforms_and_bigquery/scripts/01_run_local.sh
```

**Output Breakdown**:
* **`output/valid_sales-*.json`**: 6 clean transactions enriched with product metadata and computed profit margins.
* **`output/dead_letter_records-*.json`**: 4 poison transactions tagged with exact root causes:
  1. `Product 'PROD_UNKNOWN' not found in catalog`
  2. `Non-positive amount: -50.0`
  3. `Invalid date format 'INVALID_DATE' (must be YYYY-MM-DD)`
  4. `Missing product_id`

### 2. Deploy to Google Cloud Dataflow & BigQuery
Ensure `.env` contains your `GCP_PROJECT_ID` and `DATAFLOW_BUCKET`, then run:
```bash
bash 02_transforms_and_bigquery/scripts/02_run_dataflow.sh
```

The script will:
1. Ensure the BigQuery dataset (`dataflow_workshop`) exists.
2. Upload `sales_data_with_errors.csv` and `product_catalog.json` to your Cloud Storage bucket.
3. Submit the pipeline to Dataflow with `--runner=DataflowRunner`.
4. Stream enriched valid rows directly into BigQuery table `curated_sales` via the **Storage Write API**.
5. Save poison records into `gs://${DATAFLOW_BUCKET}/dlq/dead_letter_records-*.json`.

---

## Official Documentation & Deep Dives

* [Apache Beam DoFn Lifecycle](https://beam.apache.org/documentation/programming-guide/#dofn) — Understanding worker setup, bundle processing, and teardown execution.
* [Apache Beam Side Inputs Guide](https://beam.apache.org/documentation/programming-guide/#side-inputs) — Broadcasting reference collections and dictionaries to worker threads.
* [Apache Beam Additional Outputs (Branching Streams)](https://beam.apache.org/documentation/programming-guide/#additional-outputs) — Dead-letter queues and multi-output routing.
* [Apache Beam BigQuery I/O Connector](https://beam.apache.org/documentation/io/built-in/google-bigquery/) — Configuring BigQuery Storage Read and Storage Write APIs.
* [Google Cloud BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api) — High-throughput streaming ingestion with exactly-once delivery semantics.

