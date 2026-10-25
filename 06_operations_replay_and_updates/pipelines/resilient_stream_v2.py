#!/usr/bin/env python3
"""
Resilient Operations Pipeline (Version 2 — Rolling Update) — Chapter 06
=======================================================================
Upgraded streaming pipeline deployed via:
  gcloud dataflow jobs run ... --update --job_name=EXISTING_JOB_NAME

Demonstrates:
  1. Preserving identical transform names ("IngestOrders", "ParseOrders", "CalculateTaxes", "WriteOrdersToSink")
  2. In-place state preservation and worker migration without data loss or downtime
  3. Enriched business logic: loyalty discounts & backward-compatible BigQuery schema
"""

import argparse
import datetime
import json
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions

# Backward-compatible schema evolution: new fields are NULLABLE
ORDER_SCHEMA_V2 = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "subtotal", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "loyalty_discount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "net_amount", "type": "FLOAT64", "mode": "NULLABLE"},
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


class CalculateTaxV2Fn(beam.DoFn):
    """V2 Upgraded Tax Processing: includes 10% loyalty discount for orders >= $100."""

    def process(self, order: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        subtotal = float(order.get("subtotal", 0.0))

        # New business feature: 10% discount on orders >= $100
        if subtotal >= 100.0:
            loyalty_discount = round(subtotal * 0.10, 2)
            net_amount = round(subtotal - loyalty_discount, 2)
        else:
            loyalty_discount = 0.0
            net_amount = subtotal

        tax = round(net_amount * 0.05, 2)
        total = round(net_amount + tax, 2)
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        yield {
            "order_id": str(order.get("order_id")),
            "customer_id": str(order.get("customer_id")),
            "subtotal": subtotal,
            "loyalty_discount": loyalty_discount,
            "net_amount": net_amount,
            "tax_amount": tax,
            "total_amount": total,
            "pipeline_version": "v2.0.0",
            "processed_at": now_iso,
        }


def run(argv=None):
    parser = argparse.ArgumentParser(description="Resilient Streaming Pipeline V2 (Update) — Chapter 06")
    parser.add_argument("--input_subscription", type=str, default=None, help="Pub/Sub subscription")
    parser.add_argument("--input_file", type=str, default=None, help="Local file path for testing")
    parser.add_argument("--output_table", type=str, default=None, help="BigQuery table (project:dataset.table)")
    parser.add_argument("--output_prefix", type=str, default=None, help="Local output file prefix")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    is_cloud_runner = "DataflowRunner" in str(pipeline_options.get_all_options().get("runner", ""))

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Ingest Orders (MUST MATCH V1 NAME: IngestOrders)
        if known_args.input_subscription:
            raw_orders = p | "IngestOrders" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        elif known_args.input_file:
            raw_orders = p | "IngestOrders" >> beam.io.ReadFromText(known_args.input_file)
        else:
            raise ValueError("Must provide either --input_subscription or --input_file.")

        # Step 2: Parse Orders (MUST MATCH V1 NAME: ParseOrders)
        parsed_orders = raw_orders | "ParseOrders" >> beam.ParDo(ParseOrderFn())

        # Step 3: Calculate Taxes V2 (MUST MATCH V1 NAME: CalculateTaxes)
        curated_orders = parsed_orders | "CalculateTaxes" >> beam.ParDo(CalculateTaxV2Fn())

        # Step 4: Write Orders (MUST MATCH V1 NAME: WriteOrdersToSink)
        if is_cloud_runner and known_args.output_table:
            _ = curated_orders | "WriteOrdersToSink" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=ORDER_SCHEMA_V2,
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
