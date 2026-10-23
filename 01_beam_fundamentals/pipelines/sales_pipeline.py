#!/usr/bin/env python3
"""
Sales Pipeline — Chapter 01: Apache Beam & Dataflow Fundamentals
==================================================================
This pipeline processes retail transaction CSV records, filters by date,
aggregates total revenue per product, and writes the output to text files.

Supported Runners:
  - DirectRunner (local development / testing)
  - DataflowRunner (Google Cloud Dataflow managed service)
"""

import argparse
import logging
from typing import Iterator, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions, StandardOptions


class ParseSalesRecordFn(beam.DoFn):
    """Parses a CSV line into a structured dictionary.

    Skips the header row and malformed records.
    """

    def process(self, line: str) -> Iterator[dict]:
        line = line.strip()
        if not line or line.startswith("transaction_id"):
            return

        parts = [p.strip() for p in line.split(",")]
        if len(parts) == 4:
            try:
                yield {
                    "transaction_id": parts[0],
                    "product_id": parts[1],
                    "amount": float(parts[2]),
                    "transaction_date": parts[3],
                }
            except ValueError:
                logging.warning("Failed to parse numeric amount in row: %s", line)


class FilterByDateFn(beam.DoFn):
    """Filters records by a target transaction date."""

    def __init__(self, target_date: str = ""):
        self.target_date = target_date

    def process(self, record: dict) -> Iterator[dict]:
        if not self.target_date or record.get("transaction_date") == self.target_date:
            yield record


def format_output(record: Tuple[str, float]) -> str:
    """Formats (product_id, total_amount) tuple as a readable string."""
    product_id, total_amount = record
    return f"product_id={product_id}, total_revenue={total_amount:.2f}"


def run(argv=None):
    """Builds and executes the Apache Beam pipeline."""
    parser = argparse.ArgumentParser(description="Chapter 01 Retail Sales Beam Pipeline")
    parser.add_argument(
        "--input",
        dest="input",
        required=True,
        help="Input path to CSV file(s) (local path or gs://bucket/path.csv)",
    )
    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help="Output path prefix for results (local path or gs://bucket/output/prefix)",
    )
    parser.add_argument(
        "--target-date",
        dest="target_date",
        default="2025-06-12",
        help="Target transaction_date to filter on (default: 2025-06-12). Set empty to include all dates.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Initialize PipelineOptions with remaining arguments (handles --runner, --project, etc.)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logging.info("Starting pipeline execution with runner: %s", pipeline_options.view_as(StandardOptions).runner)
    logging.info("Input: %s | Output: %s | Target Date: %s", known_args.input, known_args.output, known_args.target_date)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadCSV" >> beam.io.ReadFromText(known_args.input)
            | "ParseRecords" >> beam.ParDo(ParseSalesRecordFn())
            | "FilterDate" >> beam.ParDo(FilterByDateFn(target_date=known_args.target_date))
            | "ExtractProductAmount" >> beam.Map(lambda r: (r["product_id"], r["amount"]))
            | "AggregateRevenuePerProduct" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(format_output)
            | "WriteResults" >> beam.io.WriteToText(known_args.output, file_name_suffix=".txt")
        )

    logging.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
