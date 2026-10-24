#!/usr/bin/env python3
"""
Stateful Processing, Timers & Hot Key Fanout Pipeline — Chapter 04
==================================================================
Demonstrates production-grade Apache Beam patterns for:
  1. Stateful stream processing with @StateId (ReadModifyWriteStateSpec & BagStateSpec)
  2. Automated state lifecycle management & session expiration with @TimerId
  3. Mitigating severe data skew and hot key bottlenecks using with_hot_key_fanout
  4. Multi-sink delivery: Cloud BigQuery (Storage Write API) or local JSON files
"""

import argparse
import datetime
import json
import logging
from typing import Any, Dict, Iterator, List, Tuple

import apache_beam as beam
from apache_beam import TimeDomain
from apache_beam.coders import FloatCoder, StrUtf8Coder, VarIntCoder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)

# BigQuery Schema Definitions
USER_SESSION_SCHEMA = {
    "fields": [
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "first_seen", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "last_seen", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "session_duration_sec", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_events", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_spend", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "distinct_pages_visited", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "is_anomaly_velocity", "type": "BOOLEAN", "mode": "REQUIRED"},
        {"name": "finalized_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

FANOUT_METRICS_SCHEMA = {
    "fields": [
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "total_events", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_spend", "type": "FLOAT64", "mode": "REQUIRED"},
        {"name": "emitted_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseAndTimestampFn(beam.DoFn):
    """Parses raw JSON input and assigns event-time timestamp."""

    def process(self, line: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
        line = line.strip()
        if not line or line in ("[", "]", ","):
            return
        clean = line.rstrip(",")
        try:
            event = json.loads(clean)
            user_id = event.get("user_id")
            if not user_id:
                return

            ts_str = event.get("event_timestamp", "")
            if ts_str:
                dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_sec = dt.timestamp()
            else:
                ts_sec = datetime.datetime.now(datetime.timezone.utc).timestamp()

            yield beam.window.TimestampedValue((user_id, event), ts_sec)
        except Exception as e:
            logging.warning("Skipping invalid record: %s (Error: %s)", line, e)


class StatefulSessionizerFn(beam.DoFn):
    """Stateful DoFn maintaining per-key session state and scheduling an inactivity timer.

    Demonstrates:
      - ReadModifyWriteStateSpec: scalar counters and accumulators
      - BagStateSpec: unordered collection of visited pages
      - TimerSpec: automated watermark-based session flush and memory reclamation
    """

    EVENT_COUNT_STATE = ReadModifyWriteStateSpec("event_count", VarIntCoder())
    TOTAL_SPEND_STATE = ReadModifyWriteStateSpec("total_spend", FloatCoder())
    FIRST_SEEN_STATE = ReadModifyWriteStateSpec("first_seen_sec", VarIntCoder())
    LAST_SEEN_STATE = ReadModifyWriteStateSpec("last_seen_sec", VarIntCoder())
    PAGES_BAG_STATE = BagStateSpec("pages_visited", StrUtf8Coder())

    INACTIVITY_TIMER = TimerSpec("inactivity_timer", TimeDomain.WATERMARK)

    def __init__(self, inactivity_timeout_sec: int = 60):
        super().__init__()
        self.inactivity_timeout_sec = inactivity_timeout_sec
        self.sessions_counter = Metrics.counter(self.__class__, "finalized_sessions")

    def process(
        self,
        element: Tuple[str, Dict[str, Any]],
        element_ts=beam.DoFn.TimestampParam,
        count_state=beam.DoFn.StateParam(EVENT_COUNT_STATE),
        spend_state=beam.DoFn.StateParam(TOTAL_SPEND_STATE),
        first_seen_state=beam.DoFn.StateParam(FIRST_SEEN_STATE),
        last_seen_state=beam.DoFn.StateParam(LAST_SEEN_STATE),
        pages_state=beam.DoFn.StateParam(PAGES_BAG_STATE),
        timer=beam.DoFn.TimerParam(INACTIVITY_TIMER),
    ) -> Iterator[Dict[str, Any]]:
        user_id, event = element
        ts_int = int(element_ts)

        # 1. Update event count
        current_count = (count_state.read() or 0) + 1
        count_state.write(current_count)

        # 2. Update spend
        current_spend = (spend_state.read() or 0.0) + float(event.get("amount", 0.0))
        spend_state.write(current_spend)

        # 3. Update timestamps
        first_seen = first_seen_state.read()
        if first_seen is None:
            first_seen_state.write(ts_int)
        last_seen_state.write(ts_int)

        # 4. Append page to BagState
        page = event.get("page", "/unknown")
        pages_state.add(page)

        # 5. Reset inactivity timer: fires when watermark advances past ts_int + timeout
        timer.set(ts_int + self.inactivity_timeout_sec)

        # (Optional) Yield speculative real-time anomaly alerts if velocity is high
        if current_count == 100:
            logging.info("High-velocity spike detected in per-key state for user: %s (100 events)", user_id)

    @on_timer(INACTIVITY_TIMER)
    def on_session_inactivity(
        self,
        key=beam.DoFn.KeyParam,
        count_state=beam.DoFn.StateParam(EVENT_COUNT_STATE),
        spend_state=beam.DoFn.StateParam(TOTAL_SPEND_STATE),
        first_seen_state=beam.DoFn.StateParam(FIRST_SEEN_STATE),
        last_seen_state=beam.DoFn.StateParam(LAST_SEEN_STATE),
        pages_state=beam.DoFn.StateParam(PAGES_BAG_STATE),
    ) -> Iterator[Dict[str, Any]]:
        """Invoked when watermark passes the scheduled timer. Emits session KPIs and cleans state."""
        count = count_state.read() or 0
        if count == 0:
            return

        spend = spend_state.read() or 0.0
        first_seen = first_seen_state.read() or 0
        last_seen = last_seen_state.read() or first_seen
        duration = max(0, last_seen - first_seen)
        pages_list = list(pages_state.read())
        distinct_pages = len(set(pages_list))

        is_anomaly = count >= 100
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        first_iso = datetime.datetime.fromtimestamp(first_seen, tz=datetime.timezone.utc).isoformat()
        last_iso = datetime.datetime.fromtimestamp(last_seen, tz=datetime.timezone.utc).isoformat()

        # Emit finalized session profile
        yield {
            "user_id": str(key),
            "first_seen": first_iso,
            "last_seen": last_iso,
            "session_duration_sec": int(duration),
            "total_events": int(count),
            "total_spend": round(float(spend), 2),
            "distinct_pages_visited": int(distinct_pages),
            "is_anomaly_velocity": bool(is_anomaly),
            "finalized_at": now_iso,
        }

        self.sessions_counter.inc()

        # CRITICAL: Clear all state to prevent memory leaks in continuous streaming
        count_state.clear()
        spend_state.clear()
        first_seen_state.clear()
        last_seen_state.clear()
        pages_state.clear()


class CountAndSumCombineFn(beam.CombineFn):
    """CombineFn to compute count and total sum in an associative/commutative way."""

    def create_accumulator(self) -> Tuple[int, float]:
        return (0, 0.0)

    def add_input(self, accumulator: Tuple[int, float], input_val: float) -> Tuple[int, float]:
        count, total = accumulator
        return (count + 1, total + float(input_val))

    def merge_accumulators(self, accumulators: List[Tuple[int, float]]) -> Tuple[int, float]:
        counts, totals = zip(*accumulators)
        return (sum(counts), sum(totals))

    def extract_output(self, accumulator: Tuple[int, float]) -> Tuple[int, float]:
        return accumulator


class FormatFanoutOutputFn(beam.DoFn):
    """Formats aggregated metrics from CombinePerKey with fanout."""

    def process(self, element: Tuple[str, Tuple[int, float]]) -> Iterator[Dict[str, Any]]:
        user_id, (count, total_spend) = element
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        yield {
            "user_id": user_id,
            "total_events": int(count),
            "total_spend": round(float(total_spend), 2),
            "emitted_at": now_iso,
        }


def run(argv=None):
    parser = argparse.ArgumentParser(description="Stateful Processing & Hot Key Fanout — Chapter 04")
    parser.add_argument("--input_file", type=str, required=True, help="Input JSON file or GCS path")
    parser.add_argument("--output_dataset", type=str, default="dataflow_workshop", help="BigQuery Dataset ID")
    parser.add_argument("--output_prefix_stateful", type=str, default=None, help="Local output prefix for stateful")
    parser.add_argument("--output_prefix_fanout", type=str, default=None, help="Local output prefix for fanout")
    parser.add_argument("--fanout_shards", type=int, default=10, help="Fanout shards for hot keys (default: 10)")
    parser.add_argument("--session_inactivity_sec", type=int, default=60, help="Session inactivity timeout in sec")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    is_cloud_runner = "DataflowRunner" in str(pipeline_options.get_all_options().get("runner", ""))

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read raw records and assign Event-Time timestamps
        raw_lines = p | "ReadClickstream" >> beam.io.ReadFromText(known_args.input_file)
        keyed_events = raw_lines | "ParseAndTimestamp" >> beam.ParDo(ParseAndTimestampFn())

        # ---------------------------------------------------------------------
        # Branch 1: Stateful Sessionizer & Anomaly Detector with @StateId & @TimerId
        # ---------------------------------------------------------------------
        stateful_sessions = (
            keyed_events
            | "StatefulSessionTracker"
            >> beam.ParDo(StatefulSessionizerFn(inactivity_timeout_sec=known_args.session_inactivity_sec))
        )

        # ---------------------------------------------------------------------
        # Branch 2: High-Throughput Aggregation with with_hot_key_fanout
        # ---------------------------------------------------------------------
        # Problem: 'usr_hot_influencer' generates 100x more events than typical users,
        # creating a worker CPU/memory bottleneck under standard GroupByKey.
        # Solution: .with_hot_key_fanout(fanout=10) introduces intermediate sharding!
        # ---------------------------------------------------------------------
        fanout_metrics = (
            keyed_events
            | "ExtractUserAmount" >> beam.Map(lambda kv: (kv[0], kv[1].get("amount", 0.0)))
            | "CombineWithFanout"
            >> beam.CombinePerKey(CountAndSumCombineFn()).with_hot_key_fanout(
                # Dynamic fanout: 15 shards for celebrity keys, 0 for regular keys
                lambda key: known_args.fanout_shards if ("hot" in key or "influencer" in key) else 0
            )
            | "FormatFanoutOutput" >> beam.ParDo(FormatFanoutOutputFn())
        )

        # Step 3: Write Outputs (Cloud BigQuery vs Local Text)
        if is_cloud_runner:
            gcp_opts = pipeline_options.view_as(GoogleCloudOptions)
            project = gcp_opts.project
            dataset = known_args.output_dataset

            _ = stateful_sessions | "WriteStatefulBQ" >> beam.io.WriteToBigQuery(
                table=f"{project}:{dataset}.user_session_profiles",
                schema=USER_SESSION_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )

            _ = fanout_metrics | "WriteFanoutBQ" >> beam.io.WriteToBigQuery(
                table=f"{project}:{dataset}.fanout_user_metrics",
                schema=FANOUT_METRICS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        else:
            if known_args.output_prefix_stateful:
                _ = (
                    stateful_sessions
                    | "FormatStatefulJson" >> beam.Map(json.dumps)
                    | "WriteStatefulLocal"
                    >> beam.io.WriteToText(known_args.output_prefix_stateful, file_name_suffix=".json")
                )

            if known_args.output_prefix_fanout:
                _ = (
                    fanout_metrics
                    | "FormatFanoutJson" >> beam.Map(json.dumps)
                    | "WriteFanoutLocal"
                    >> beam.io.WriteToText(known_args.output_prefix_fanout, file_name_suffix=".json")
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run()
