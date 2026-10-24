#!/usr/bin/env python3
"""
Benchmarked Streaming Pipeline & Observability — Chapter 05
===========================================================
Demonstrates production performance tuning and observability:
  1. Beam step-level latency tracking using Metrics.distribution & Metrics.counter
  2. Controllable computational bottlenecks for load testing & autoscaling evaluation
  3. Leveraging Dataflow Streaming Engine and Dataflow Shuffle
  4. Dual sinks: BigQuery Storage Write API (Cloud) or local performance summaries
"""

import argparse
import datetime
import hashlib
import json
import logging
import time
from typing import Any, Dict, Iterable, Iterator, List, Tuple

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions

# BigQuery Table Schemas
CATEGORY_METRICS_SCHEMA = {
    "fields": [
        {"name": "category", "type": "STRING", "mode": "REQUIRED"},
        {"name": "transaction_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_revenue", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "avg_revenue", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "avg_step_latency_ms", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "calculated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseAndBenchmarkFn(beam.DoFn):
    """Parses incoming records, simulates business computation, and records step latency metrics."""

    def __init__(self, simulated_latency_ms: float = 0.0):
        super().__init__()
        self.simulated_latency_ms = simulated_latency_ms
        self.elements_counter = Metrics.counter(self.__class__, "elements_processed")
        self.step_latency_dist = Metrics.distribution(self.__class__, "step_latency_ms")

    def process(self, line: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
        t0 = time.perf_counter()
        line = line.strip()
        if not line or line in ("[", "]", ","):
            return

        clean = line.rstrip(",")
        try:
            data = json.loads(clean)
            category = data.get("category", "uncategorized")

            # Simulate CPU-bound business workload (e.g. SHA-256 hash tokenization)
            if self.simulated_latency_ms > 0:
                # Deterministic busy work
                payload_str = str(data.get("record_id", "")) + str(data.get("amount", 0.0))
                _ = hashlib.sha256(payload_str.encode()).hexdigest()
                time.sleep(self.simulated_latency_ms / 1000.0)

            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            # Record metrics
            self.elements_counter.inc()
            self.step_latency_dist.update(int(elapsed_ms))

            data["step_latency_ms"] = elapsed_ms
            yield (category, data)
        except Exception as e:
            logging.warning("Error parsing benchmark line: %s (%s)", line, e)


class AggregateCategoryMetricsFn(beam.DoFn):
    """Aggregates all transaction metrics per category."""

    def process(self, element: Tuple[str, Iterable[Dict[str, Any]]]) -> Iterator[Dict[str, Any]]:
        category, records = element
        records_list = list(records)
        count = len(records_list)
        if count == 0:
            return

        total_rev = sum(r.get("amount", 0.0) for r in records_list)
        avg_rev = total_rev / count
        avg_latency = sum(r.get("step_latency_ms", 0.0) for r in records_list) / count

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        yield {
            "category": category,
            "transaction_count": count,
            "total_revenue": round(total_rev, 2),
            "avg_revenue": round(avg_rev, 2),
            "avg_step_latency_ms": round(avg_latency, 3),
            "calculated_at": now_iso,
        }


def run(argv=None):
    parser = argparse.ArgumentParser(description="Benchmarked Streaming Pipeline — Chapter 05")
    parser.add_argument("--input_file", type=str, required=True, help="Input NDJSON file or GCS path")
    parser.add_argument("--output_dataset", type=str, default="dataflow_workshop", help="BigQuery Dataset ID")
    parser.add_argument("--output_prefix_metrics", type=str, default=None, help="Local file prefix for metrics")
    parser.add_argument("--simulated_latency_ms", type=float, default=0.5, help="Simulated CPU delay in ms per record")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    is_cloud_runner = "DataflowRunner" in str(pipeline_options.get_all_options().get("runner", ""))

    start_wall_time = time.perf_counter()

    with beam.Pipeline(options=pipeline_options) as p:
        raw_lines = p | "ReadBenchmarkData" >> beam.io.ReadFromText(known_args.input_file)

        # Parse, benchmark step latency, and extract category key
        keyed_records = raw_lines | "ParseAndBenchmark" >> beam.ParDo(
            ParseAndBenchmarkFn(simulated_latency_ms=known_args.simulated_latency_ms)
        )

        # Group and aggregate
        category_metrics = (
            keyed_records
            | "GroupByCategory" >> beam.GroupByKey()
            | "AggregateKPIs" >> beam.ParDo(AggregateCategoryMetricsFn())
        )

        if is_cloud_runner:
            gcp_opts = pipeline_options.view_as(GoogleCloudOptions)
            table_path = f"{gcp_opts.project}:{known_args.output_dataset}.benchmark_category_metrics"

            _ = category_metrics | "WriteMetricsBQ" >> beam.io.WriteToBigQuery(
                table=table_path,
                schema=CATEGORY_METRICS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        elif known_args.output_prefix_metrics:
            _ = (
                category_metrics
                | "FormatJson" >> beam.Map(json.dumps)
                | "WriteLocal"
                >> beam.io.WriteToText(known_args.output_prefix_metrics, file_name_suffix=".json")
            )

    total_wall_time = time.perf_counter() - start_wall_time
    logging.info("==================================================================")
    logging.info("Pipeline execution completed in: %.3f seconds", total_wall_time)
    logging.info("==================================================================")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
