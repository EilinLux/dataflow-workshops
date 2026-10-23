#!/usr/bin/env python3
"""
Streaming Windowing & Late Data Pipeline — Chapter 03: Real-Time Streaming
==========================================================================
Demonstrates production-grade real-time stream processing with Apache Beam & Dataflow:
  1. Ingestion from Google Cloud Pub/Sub with Ordering Keys
  2. Parse, Validate & Tagged Dead-Letter Queue (DLQ) for poison payloads
  3. Event-Time assignment via TimestampedValue
  4. Temporal Windowing Strategies:
     - Fixed (Tumbling) Windows with Watermark Triggers & Allowed Lateness
     - Sliding (Hopping) Windows for rolling velocity / moving aggregations
     - Session Windows for dynamic activity clustering (gap-based sessionization)
  5. Multi-Sink delivery: BigQuery Storage Write API (Cloud) or local files (Local)
"""

import argparse
import datetime
import json
import logging
from typing import Any, Dict, Iterable, Iterator, List, Tuple

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms import trigger
from apache_beam.transforms import window

DEAD_LETTER_TAG = "dead_letter"

# BigQuery Table Schemas
TUMBLING_METRICS_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_spend", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "emitted_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

SLIDING_METRICS_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "rolling_event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "rolling_spend", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "emitted_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

SESSION_METRICS_SCHEMA = {
    "fields": [
        {"name": "session_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "session_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "session_duration_seconds", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "event_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_spend", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "emitted_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

DLQ_SCHEMA = {
    "fields": [
        {"name": "raw_payload", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseAndValidateEventFn(beam.DoFn):
    """Parses incoming JSON messages, validates business rules, routes poison records to DLQ,
    and assigns event-time timestamps using TimestampedValue.
    """

    def __init__(self):
        super().__init__()
        self.valid_counter = Metrics.counter(self.__class__, "valid_events")
        self.invalid_counter = Metrics.counter(self.__class__, "invalid_events")

    def process(self, element: Any) -> Iterator[Any]:
        # Element may be raw bytes (from Pub/Sub), PubsubMessage, or string (from file)
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if hasattr(element, "data"):
            raw_str = element.data.decode("utf-8", errors="replace")
        elif isinstance(element, bytes):
            raw_str = element.decode("utf-8", errors="replace")
        else:
            raw_str = str(element).strip()

        # Handle blank lines or JSON bracket artifacts
        if not raw_str or raw_str in ("[", "]", ","):
            return

        clean_str = raw_str.rstrip(",")
        try:
            data = json.loads(clean_str)
        except Exception as e:
            self.invalid_counter.inc()
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                {"raw_payload": clean_str, "error_reason": f"JSONDecodeError: {str(e)}", "ingested_at": now_iso},
            )
            return

        # Business Rule 1: user_id must be non-empty
        user_id = data.get("user_id")
        if not user_id or not isinstance(user_id, str) or not user_id.strip():
            self.invalid_counter.inc()
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                {"raw_payload": clean_str, "error_reason": "Missing or empty user_id", "ingested_at": now_iso},
            )
            return

        # Business Rule 2: amount must be non-negative
        amount = data.get("amount", 0.0)
        try:
            amount_val = float(amount)
            if amount_val < 0:
                self.invalid_counter.inc()
                yield beam.pvalue.TaggedOutput(
                    DEAD_LETTER_TAG,
                    {"raw_payload": clean_str, "error_reason": f"Negative amount: {amount_val}", "ingested_at": now_iso},
                )
                return
            data["amount"] = amount_val
        except (ValueError, TypeError):
            self.invalid_counter.inc()
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_TAG,
                {"raw_payload": clean_str, "error_reason": f"Invalid amount format: {amount}", "ingested_at": now_iso},
            )
            return

        # Business Rule 3: Valid timestamp extraction
        ts_str = data.get("event_timestamp")
        timestamp_sec = None
        if ts_str:
            try:
                # Support standard ISO-8601 strings (e.g. 2026-10-27T10:00:00Z)
                clean_ts = ts_str.replace("Z", "+00:00")
                dt = datetime.datetime.fromisoformat(clean_ts)
                timestamp_sec = dt.timestamp()
            except Exception:
                timestamp_sec = None

        if timestamp_sec is None:
            # Fall back to current worker time if absent or unparseable
            timestamp_sec = datetime.datetime.now(datetime.timezone.utc).timestamp()

        self.valid_counter.inc()
        # Attach event-time timestamp to element
        yield beam.window.TimestampedValue(data, timestamp_sec)


class FormatTumblingWindowFn(beam.DoFn):
    """Formats aggregated metrics for a Fixed / Tumbling Window."""

    def process(
        self,
        element: Tuple[str, Tuple[int, float]],
        window=beam.DoFn.WindowParam,
    ) -> Iterator[Dict[str, Any]]:
        user_id, (count, total_spend) = element
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "user_id": user_id,
            "event_count": int(count),
            "total_spend": round(float(total_spend), 2),
            "emitted_at": now_iso,
        }


class FormatSlidingWindowFn(beam.DoFn):
    """Formats aggregated metrics for a Sliding / Hopping Window."""

    def process(
        self,
        element: Tuple[str, Tuple[int, float]],
        window=beam.DoFn.WindowParam,
    ) -> Iterator[Dict[str, Any]]:
        user_id, (count, total_spend) = element
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "user_id": user_id,
            "rolling_event_count": int(count),
            "rolling_spend": round(float(total_spend), 2),
            "emitted_at": now_iso,
        }


class AggregateSessionFn(beam.DoFn):
    """Aggregates all events within a Session Window into session-level KPIs."""

    def process(
        self,
        element: Tuple[str, Iterable[Dict[str, Any]]],
        window=beam.DoFn.WindowParam,
    ) -> Iterator[Dict[str, Any]]:
        user_id, events = element
        events_list = list(events)
        count = len(events_list)
        total_spend = sum(e.get("amount", 0.0) for e in events_list)

        # Duration derived from window boundaries (SessionWindow tracks span)
        duration_sec = (window.end.micros - window.start.micros) / 1_000_000.0
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        yield {
            "session_start": window.start.to_utc_datetime().isoformat(),
            "session_end": window.end.to_utc_datetime().isoformat(),
            "user_id": user_id,
            "session_duration_seconds": round(duration_sec, 2),
            "event_count": count,
            "total_spend": round(total_spend, 2),
            "emitted_at": now_iso,
        }


def combine_count_and_sum(accumulator, element):
    count, total = accumulator
    return count + 1, total + element


class CountAndSumCombineFn(beam.CombineFn):
    """Custom CombineFn to calculate both count and sum in a single pass."""

    def create_accumulator(self):
        return (0, 0.0)

    def add_input(self, accumulator, input_value):
        count, total = accumulator
        return (count + 1, total + float(input_value))

    def merge_accumulators(self, accumulators):
        counts, totals = zip(*accumulators)
        return (sum(counts), sum(totals))

    def extract_output(self, accumulator):
        return accumulator


def run(argv=None):
    parser = argparse.ArgumentParser(description="Streaming Windowing Pipeline — Chapter 03")
    parser.add_argument("--input_subscription", type=str, default=None, help="Pub/Sub subscription path")
    parser.add_argument("--input_file", type=str, default=None, help="Local JSON file path for testing")
    parser.add_argument("--output_dataset", type=str, default="dataflow_streaming_db", help="BigQuery Dataset ID")
    parser.add_argument("--output_prefix_tumbling", type=str, default=None, help="Local file prefix for tumbling output")
    parser.add_argument("--output_prefix_sliding", type=str, default=None, help="Local file prefix for sliding output")
    parser.add_argument("--output_prefix_session", type=str, default=None, help="Local file prefix for session output")
    parser.add_argument("--output_prefix_dlq", type=str, default=None, help="Local file prefix for dead letters")
    parser.add_argument("--fixed_window_sec", type=int, default=60, help="Fixed window duration in seconds")
    parser.add_argument("--slide_window_sec", type=int, default=60, help="Sliding window size in seconds")
    parser.add_argument("--slide_period_sec", type=int, default=20, help="Sliding period in seconds")
    parser.add_argument("--session_gap_sec", type=int, default=30, help="Session gap duration in seconds")
    parser.add_argument("--allowed_lateness_sec", type=int, default=120, help="Allowed lateness in seconds")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    is_cloud_runner = "DataflowRunner" in str(pipeline_options.get_all_options().get("runner", ""))

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Ingest Unbounded Stream (Pub/Sub) or Local File Source
        if known_args.input_subscription:
            logging.info("Reading from Pub/Sub Subscription: %s", known_args.input_subscription)
            raw_elements = (
                p
                | "ReadFromPubSub"
                >> beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription,
                    with_attributes=True,
                )
            )
        elif known_args.input_file:
            logging.info("Reading from local input file: %s", known_args.input_file)
            raw_elements = p | "ReadLocalFile" >> beam.io.ReadFromText(known_args.input_file)
        else:
            raise ValueError("Must provide either --input_subscription or --input_file.")

        # Step 2: Parse, Validate, and Route Invalid Records to Dead-Letter Queue
        parsed_results = raw_elements | "ParseAndValidate" >> beam.ParDo(
            ParseAndValidateEventFn()
        ).with_outputs(DEAD_LETTER_TAG, main="valid_events")

        valid_events = parsed_results["valid_events"]
        dead_letters = parsed_results[DEAD_LETTER_TAG]

        # Step 3: Handle Dead-Letter Queue
        if is_cloud_runner:
            gcp_opts = pipeline_options.view_as(GoogleCloudOptions)
            dlq_table = f"{gcp_opts.project}:{known_args.output_dataset}.dead_letter_queue"
            _ = dead_letters | "WriteDLQToBigQuery" >> beam.io.WriteToBigQuery(
                table=dlq_table,
                schema=DLQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        elif known_args.output_prefix_dlq:
            _ = (
                dead_letters
                | "FormatDLQJson" >> beam.Map(json.dumps)
                | "WriteDLQLocal" >> beam.io.WriteToText(known_args.output_prefix_dlq, file_name_suffix=".json")
            )

        # -------------------------------------------------------------------------
        # Branch 1: Fixed (Tumbling) Windows with Watermark Triggers & Allowed Lateness
        # -------------------------------------------------------------------------
        # Trigger Strategy:
        #   - Early firing: After 3 elements accumulate (speculative updates)
        #   - On-time firing: When watermark passes end of window
        #   - Late firing: After each late record arrives within allowed_lateness
        #   - AccumulationMode.ACCUMULATING: emits revised full totals on late arrivals
        # -------------------------------------------------------------------------
        tumbling_trigger = trigger.AfterWatermark(
            early=trigger.AfterCount(3),
            late=trigger.AfterCount(1),
        )

        tumbling_metrics = (
            valid_events
            | "ApplyFixedWindow"
            >> beam.WindowInto(
                window.FixedWindows(known_args.fixed_window_sec),
                trigger=tumbling_trigger,
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=known_args.allowed_lateness_sec,
            )
            | "PairUserAmountTumbling" >> beam.Map(lambda e: (e["user_id"], e["amount"]))
            | "CombineTumblingPerUser" >> beam.CombinePerKey(CountAndSumCombineFn())
            | "FormatTumblingOutput" >> beam.ParDo(FormatTumblingWindowFn())
        )

        # -------------------------------------------------------------------------
        # Branch 2: Sliding (Hopping) Windows for Rolling Aggregations
        # -------------------------------------------------------------------------
        # Calculates moving 60-second activity with a 20-second slide
        # -------------------------------------------------------------------------
        sliding_metrics = (
            valid_events
            | "ApplySlidingWindow"
            >> beam.WindowInto(
                window.SlidingWindows(known_args.slide_window_sec, known_args.slide_period_sec),
                allowed_lateness=known_args.allowed_lateness_sec,
            )
            | "PairUserAmountSliding" >> beam.Map(lambda e: (e["user_id"], e["amount"]))
            | "CombineSlidingPerUser" >> beam.CombinePerKey(CountAndSumCombineFn())
            | "FormatSlidingOutput" >> beam.ParDo(FormatSlidingWindowFn())
        )

        # -------------------------------------------------------------------------
        # Branch 3: Session Windows for User Activity Clustering
        # -------------------------------------------------------------------------
        # Dynamically merges windows if gap between user events <= session_gap_sec
        # -------------------------------------------------------------------------
        session_metrics = (
            valid_events
            | "ApplySessionWindow"
            >> beam.WindowInto(
                window.Sessions(known_args.session_gap_sec),
                allowed_lateness=known_args.allowed_lateness_sec,
            )
            | "PairUserEventSession" >> beam.Map(lambda e: (e["user_id"], e))
            | "GroupSessionEvents" >> beam.GroupByKey()
            | "AggregateSessionKPIs" >> beam.ParDo(AggregateSessionFn())
        )

        # Step 4: Write Outputs (Cloud BigQuery vs Local Text)
        if is_cloud_runner:
            gcp_opts = pipeline_options.view_as(GoogleCloudOptions)
            project = gcp_opts.project
            dataset = known_args.output_dataset

            _ = tumbling_metrics | "WriteTumblingBQ" >> beam.io.WriteToBigQuery(
                table=f"{project}:{dataset}.kpi_tumbling_metrics",
                schema=TUMBLING_METRICS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )

            _ = sliding_metrics | "WriteSlidingBQ" >> beam.io.WriteToBigQuery(
                table=f"{project}:{dataset}.kpi_sliding_metrics",
                schema=SLIDING_METRICS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )

            _ = session_metrics | "WriteSessionBQ" >> beam.io.WriteToBigQuery(
                table=f"{project}:{dataset}.kpi_session_metrics",
                schema=SESSION_METRICS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        else:
            if known_args.output_prefix_tumbling:
                _ = (
                    tumbling_metrics
                    | "FormatTumblingJson" >> beam.Map(json.dumps)
                    | "WriteTumblingLocal"
                    >> beam.io.WriteToText(known_args.output_prefix_tumbling, file_name_suffix=".json")
                )

            if known_args.output_prefix_sliding:
                _ = (
                    sliding_metrics
                    | "FormatSlidingJson" >> beam.Map(json.dumps)
                    | "WriteSlidingLocal"
                    >> beam.io.WriteToText(known_args.output_prefix_sliding, file_name_suffix=".json")
                )

            if known_args.output_prefix_session:
                _ = (
                    session_metrics
                    | "FormatSessionJson" >> beam.Map(json.dumps)
                    | "WriteSessionLocal"
                    >> beam.io.WriteToText(known_args.output_prefix_session, file_name_suffix=".json")
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
