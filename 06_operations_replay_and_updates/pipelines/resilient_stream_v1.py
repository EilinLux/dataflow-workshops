#!/usr/bin/env python3
"""
Resilient Operations Pipeline (Version 1) — Chapter 06
======================================================
Initial production streaming pipeline.
Demonstrates:
  1. Fixed transform naming hierarchy for zero-downtime update compatibility
  2. Ingestion from Pub/Sub with message retention or local file source
  3. Writing to BigQuery Storage Write API with schema versioning
"""

import argparse
import datetime
import json
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions

ORDER_SCHEMA_V1 = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "subtotal", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "tax_amount", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "total_amount", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "pipeline_version", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseOrderFn(beam.DoFn):
    """Parses raw JSON message or Pub/Sub bytes."""

    def process(self, element: Any) -> Iterator[Dict[str, Any]]:
        if hasattr(element, "data"):
            raw_str = element.data.decode("utf-8", errors="replace")
        elif isinstance(element, bytes):
            raw_str = element.decode("utf-8", errors="replace")
        else:
            raw_str = str(element).strip()

        if not raw_str or raw_str in ("[", "]", ","):
            return

        clean = raw_str.rstrip(",")
        try:
            yield json.loads(clean)
        except Exception as e:
            logging.warning("Error parsing order record: %s (%s)", clean, e)


class CalculateTaxV1Fn(beam.DoFn):
    """V1 Tax Processing: standard 5% sales tax."""

    def process(self, order: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        subtotal = float(order.get("subtotal", 0.0))
        tax = round(subtotal * 0.05, 2)
        total = round(subtotal + tax, 2)
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        yield {
            "order_id": str(order.get("order_id")),
            "customer_id": str(order.get("customer_id")),
            "subtotal": subtotal,
            "tax_amount": tax,
            "total_amount": total,
            "pipeline_version": "v1.0.0",
            "processed_at": now_iso,
        }


def run(argv=None):
    parser = argparse.ArgumentParser(description="Resilient Streaming Pipeline V1 — Chapter 06")
    parser.add_argument("--input_subscription", type=str, default=None, help="Pub/Sub subscription")
    parser.add_argument("--input_file", type=str, default=None, help="Local file path for testing")
    parser.add_argument("--output_table", type=str, default=None, help="BigQuery table (project:dataset.table)")
    parser.add_argument("--output_prefix", type=str, default=None, help="Local output file prefix")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    is_cloud_runner = "DataflowRunner" in str(pipeline_options.get_all_options().get("runner", ""))

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Ingest Orders (Stable Name: IngestOrders)
        if known_args.input_subscription:
            raw_orders = p | "IngestOrders" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        elif known_args.input_file:
            raw_orders = p | "IngestOrders" >> beam.io.ReadFromText(known_args.input_file)
        else:
            raise ValueError("Must provide either --input_subscription or --input_file.")

        # Step 2: Parse Orders (Stable Name: ParseOrders)
        parsed_orders = raw_orders | "ParseOrders" >> beam.ParDo(ParseOrderFn())

        # Step 3: Calculate Taxes V1 (Stable Name: CalculateTaxes)
        curated_orders = parsed_orders | "CalculateTaxes" >> beam.ParDo(CalculateTaxV1Fn())

        # Step 4: Write Orders (Stable Name: WriteOrdersToSink)
        if is_cloud_runner and known_args.output_table:
            _ = curated_orders | "WriteOrdersToSink" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=ORDER_SCHEMA_V1,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        elif known_args.output_prefix:
            _ = (
                curated_orders
                | "FormatJson" >> beam.Map(json.dumps)
                | "WriteOrdersToSink" >> beam.io.WriteToText(known_args.output_prefix, file_name_suffix=".json")
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
