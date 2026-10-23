#!/usr/bin/env python3
"""
Enrich & Load to BigQuery Pipeline — Chapter 02: Advanced Transforms & BigQuery I/O
==================================================================================
Demonstrates:
  1. DoFn Lifecycle Methods (setup, process, teardown)
  2. Side Inputs (beam.pvalue.AsDict) for reference data lookup
  3. Tagged Multi-Outputs (beam.pvalue.TaggedOutput) for Dead-Letter Queues (DLQ)
  4. Writing to BigQuery with Storage Write API and schema auto-creation
"""

import argparse
import datetime
import json
import logging
import re
from typing import Iterator, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions, StandardOptions

# Tag name for routing invalid records to Dead-Letter Queue
DEAD_LETTER_TAG = "dead_letter"

# BigQuery Schema definition for curated sales
CURATED_SALES_SCHEMA = {
    "fields": [
        {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "profit", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseCatalogFn(beam.DoFn):
    """Parses a JSON catalog file into (product_id, metadata_dict) pairs for Side Input."""

    def process(self, line: str) -> Iterator[Tuple[str, dict]]:
        line = line.strip()
        if not line or line in ("[", "]"):
            return
        # Clean trailing commas if line-delimited or array-formatted
        clean_line = line.rstrip(",")
        try:
            item = json.loads(clean_line)
            if "product_id" in item:
                yield (item["product_id"], item)
        except json.JSONDecodeError:
            pass


class ValidateAndEnrichFn(beam.DoFn):
    """Validates transaction records and enriches them using a Side Input dictionary.

    Demonstrates DoFn lifecycle methods:
      - setup: initialize regex pattern once per worker
      - process: validates, enriches via side input, and routes invalid items to DEAD_LETTER_TAG
      - teardown: clean up resources
    """

    def setup(self):
        """Called once when worker initializes before processing bundles."""
        self.date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        logging.info("ValidateAndEnrichFn initialized on worker.")

    def process(self, line: str, catalog: dict) -> Iterator[dict]:
        """Processes each line using the catalog dictionary passed as a Side Input."""
        line = line.strip()
        if not line or line.startswith("transaction_id"):
            return

        parts = [p.strip() for p in line.split(",")]
        now_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Rule 1: Validate field count
        if len(parts) != 4:
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                {"raw_line": line, "error_reason": f"Expected 4 fields, got {len(parts)}", "timestamp": now_ts},
            )
        else:
            tx_id, prod_id, amount_str, tx_date = parts

            # Rule 2: Validate product_id exists
            if not prod_id:
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_TAG,
                    {"raw_line": line, "error_reason": "Missing product_id", "timestamp": now_ts},
                )
            # Rule 3: Validate numeric positive amount
            else:
                try:
                    amount = float(amount_str)
                    if amount <= 0:
                        yield beam.pvalue.TaggedOutput(
                            DEAD_LETTER_TAG,
                            {"raw_line": line, "error_reason": f"Non-positive amount: {amount}", "timestamp": now_ts},
                        )
                    elif not self.date_regex.match(tx_date):
                        yield beam.pvalue.TaggedOutput(
                            DEAD_LETTER_TAG,
                            {"raw_line": line, "error_reason": f"Invalid date format '{tx_date}' (must be YYYY-MM-DD)", "timestamp": now_ts},
                        )
                    else:
                        prod_info = catalog.get(prod_id)
                        if not prod_info:
                            yield beam.pvalue.TaggedOutput(
                                DEAD_LETTER_TAG,
                                {"raw_line": line, "error_reason": f"Product '{prod_id}' not found in catalog", "timestamp": now_ts},
                            )
                        else:
                            unit_cost = prod_info.get("unit_cost", 0.0)
                            profit = round(amount - unit_cost, 2)
                            yield {
                                "transaction_id": tx_id,
                                "product_id": prod_id,
                                "product_name": prod_info.get("product_name", "Unknown"),
                                "category": prod_info.get("category", "General"),
                                "amount": amount,
                                "profit": profit,
                                "transaction_date": tx_date,
                                "processed_at": now_ts,
                            }

    def teardown(self):
        """Called when worker bundle teardown occurs."""
        logging.info("ValidateAndEnrichFn worker teardown complete.")


def run(argv=None):
    """Executes the Chapter 02 pipeline."""
    parser = argparse.ArgumentParser(description="Chapter 02 Side Inputs & BigQuery Pipeline")
    parser.add_argument("--input-sales", required=True, help="Path to sales CSV file(s)")
    parser.add_argument("--input-catalog", required=True, help="Path to product catalog JSON file")
    parser.add_argument("--output-dlq", required=True, help="Path prefix for Dead-Letter Queue output")
    parser.add_argument("--output-bq-table", default="", help="BigQuery table spec: PROJECT:DATASET.TABLE")
    parser.add_argument("--output-local-valid", default="", help="Optional local path prefix for valid JSON records")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read Product Catalog and build Side Input dictionary
        catalog_pcoll = (
            p
            | "ReadCatalog" >> beam.io.ReadFromText(known_args.input_catalog)
            | "ParseCatalog" >> beam.ParDo(ParseCatalogFn())
        )

        # Step 2: Read Sales Data, Validate, and Enrich using Side Input
        validation_results = (
            p
            | "ReadSales" >> beam.io.ReadFromText(known_args.input_sales)
            | "ValidateAndEnrich"
            >> beam.ParDo(
                ValidateAndEnrichFn(),
                catalog=beam.pvalue.AsDict(catalog_pcoll),
            ).with_outputs(DEAD_LETTER_TAG, main="valid_sales")
        )

        valid_sales = validation_results.valid_sales
        dead_letter = validation_results[DEAD_LETTER_TAG]

        # Step 3A: Sink Valid Records to BigQuery (if specified)
        if known_args.output_bq_table:
            logging.info("Configuring BigQuery sink: %s", known_args.output_bq_table)
            (
                valid_sales
                | "WriteToBigQuery"
                >> beam.io.WriteToBigQuery(
                    table=known_args.output_bq_table,
                    schema=CURATED_SALES_SCHEMA,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
                )
            )

        # Step 3B: Sink Valid Records Locally (for local DirectRunner testing)
        if known_args.output_local_valid:
            (
                valid_sales
                | "FormatValidJSON" >> beam.Map(json.dumps)
                | "WriteValidLocal"
                >> beam.io.WriteToText(known_args.output_local_valid, file_name_suffix=".json")
            )

        # Step 4: Sink Dead-Letter Records to Storage DLQ
        (
            dead_letter
            | "FormatDLQJSON" >> beam.Map(json.dumps)
            | "WriteDLQ" >> beam.io.WriteToText(known_args.output_dlq, file_name_suffix=".json")
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
