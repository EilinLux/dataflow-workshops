#!/usr/bin/env python3
"""
Enterprise Secure Orders Pipeline — Chapter 07: Testing, Templates, CI/CD & Security
===================================================================================
Production-grade streaming pipeline demonstrating enterprise security compliance:
  1. Private IP execution: --no_use_public_ips
  2. Customer-Managed Encryption Keys: --dataflow_kms_key
  3. Private Google Access & custom VPC subnetwork routing
  4. Packaging as containerized Dataflow Flex Template
"""

import argparse
import datetime
import json
import logging
from typing import Any, Dict, Iterator

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions, WorkerOptions

SECURE_ORDER_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "subtotal", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "tax_amount", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "total_amount", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "kms_encrypted", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseSecureOrderFn(beam.DoFn):
    """Parses incoming order JSON or Pub/Sub bytes with input sanitization."""

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
            logging.warning("Error parsing secure order record: %s (%s)", clean, e)


class SecureTaxProcessingFn(beam.DoFn):
    """Computes taxes and flags CMEK compliance status."""

    def __init__(self, is_cmek_enabled: bool = False):
        super().__init__()
        self.is_cmek_enabled = is_cmek_enabled

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
            "kms_encrypted": self.is_cmek_enabled,
            "processed_at": now_iso,
        }


def run(argv=None):
    parser = argparse.ArgumentParser(description="Secure Dataflow Pipeline — Chapter 07")
    parser.add_argument("--input_subscription", type=str, default=None, help="Pub/Sub subscription")
    parser.add_argument("--input_file", type=str, default=None, help="Local file path for testing")
    parser.add_argument("--output_table", type=str, default=None, help="BigQuery table (project:dataset.table)")
    parser.add_argument("--output_prefix", type=str, default=None, help="Local output file prefix")
    parser.add_argument("--dataflow_kms_key", type=str, default=None, help="Cloud KMS key resource for CMEK")
    parser.add_argument("--subnetwork", type=str, default=None, help="VPC Subnetwork for private IPs")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Security: Apply Private IPs and CMEK KMS key to WorkerOptions
    worker_options = pipeline_options.view_as(WorkerOptions)
    if known_args.subnetwork:
        worker_options.subnetwork = known_args.subnetwork
    if known_args.dataflow_kms_key:
        pipeline_options.view_as(GoogleCloudOptions).dataflow_kms_key = known_args.dataflow_kms_key

    is_cloud_runner = "DataflowRunner" in str(pipeline_options.get_all_options().get("runner", ""))
    is_cmek = bool(known_args.dataflow_kms_key)

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Ingest Orders
        if known_args.input_subscription:
            raw_orders = p | "IngestOrders" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        elif known_args.input_file:
            raw_orders = p | "IngestOrders" >> beam.io.ReadFromText(known_args.input_file)
        else:
            raise ValueError("Must provide either --input_subscription or --input_file.")

        # Step 2: Parse and Validate
        parsed_orders = raw_orders | "ParseSecureOrders" >> beam.ParDo(ParseSecureOrderFn())

        # Step 3: Compute Taxes & Flag CMEK Encryption
        curated_orders = parsed_orders | "SecureTaxProcess" >> beam.ParDo(
            SecureTaxProcessingFn(is_cmek_enabled=is_cmek)
        )

        # Step 4: Write to Sink
        if is_cloud_runner and known_args.output_table:
            # When writing to BigQuery with CMEK, pass kms_key parameter if configured
            _ = curated_orders | "WriteSecureBigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=SECURE_ORDER_SCHEMA,
                kms_key=known_args.dataflow_kms_key,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        elif known_args.output_prefix:
            _ = (
                curated_orders
                | "FormatJson" >> beam.Map(json.dumps)
                | "WriteLocalSecure" >> beam.io.WriteToText(known_args.output_prefix, file_name_suffix=".json")
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
