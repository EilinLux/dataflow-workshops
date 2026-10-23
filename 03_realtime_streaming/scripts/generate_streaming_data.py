#!/usr/bin/env python3
"""
Streaming Telemetry Event Generator — Chapter 03: Real-Time Streaming
=====================================================================
Generates realistic e-commerce streaming events and publishes them to either:
  1. A Google Cloud Pub/Sub Topic with Ordering Keys (by user_id)
  2. A local JSON file or stdout for local pipeline testing

Simulates real-world production conditions:
  - Event-time timestamps distinct from ingestion time
  - Simulated network lag / late-arriving records (watermark drift)
  - Simulated poison / malformed payloads to exercise dead-letter handling
  - Pub/Sub ordering keys ensuring FIFO delivery per partition
"""

import argparse
import datetime
import json
import logging
import random
import sys
import time
from typing import Any, Dict, Optional

# Setup standard logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

USER_IDS = ["usr_alpha", "usr_beta", "usr_gamma", "usr_delta", "usr_epsilon"]
DEVICE_MAP = {
    "usr_alpha": "dev_phone_01",
    "usr_beta": "dev_laptop_02",
    "usr_gamma": "dev_tablet_03",
    "usr_delta": "dev_phone_04",
    "usr_epsilon": "dev_watch_05",
}
EVENT_TYPES = ["page_view", "view_item", "add_to_cart", "remove_from_cart", "purchase"]
PAGES = ["/home", "/categories/electronics", "/product/401", "/product/802", "/cart", "/checkout/success"]


def create_event(
    event_idx: int,
    user_id: str,
    simulate_late: bool = False,
    simulate_poison: bool = False,
) -> Dict[str, Any]:
    """Generates a single synthetic telemetry event."""
    device_id = DEVICE_MAP.get(user_id, "dev_generic")
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # Simulate late arrival (timestamp shifted 60-120 seconds into the past)
    if simulate_late:
        event_time = now_utc - datetime.timedelta(seconds=random.randint(60, 120))
    else:
        event_time = now_utc

    event_type = random.choice(EVENT_TYPES)
    amount = 0.0
    if event_type == "add_to_cart":
        amount = round(random.uniform(15.0, 250.0), 2)
    elif event_type == "purchase":
        amount = round(random.uniform(25.0, 500.0), 2)

    # Corrupt event if poison test
    if simulate_poison:
        poison_type = random.choice(["negative_amount", "empty_user", "invalid_type"])
        if poison_type == "negative_amount":
            amount = -50.0
        elif poison_type == "empty_user":
            user_id = ""
        elif poison_type == "invalid_type":
            event_type = "UNKNOWN_CORRUPT_TYPE"

    event = {
        "event_id": f"evt-{event_idx:06d}",
        "user_id": user_id,
        "device_id": device_id,
        "event_type": event_type,
        "page": random.choice(PAGES),
        "amount": amount,
        "event_timestamp": event_time.isoformat(),
        "is_simulated_late": simulate_late,
    }
    return event


def publish_to_pubsub(
    project_id: str,
    topic_id: str,
    events: list,
    rate_per_sec: float,
) -> None:
    """Publishes events to Google Cloud Pub/Sub with ordering keys enabled."""
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        logging.error("google-cloud-pubsub package not found. Run 'uv sync' or 'pip install google-cloud-pubsub'.")
        sys.exit(1)

    publisher = pubsub_v1.PublisherClient(
        publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
    )
    topic_path = publisher.topic_path(project_id, topic_id)
    logging.info("Publishing to Pub/Sub topic: %s with Ordering Keys enabled...", topic_path)

    delay = 1.0 / max(rate_per_sec, 0.1)
    published_count = 0

    for idx, event in enumerate(events, start=1):
        payload_bytes = json.dumps(event).encode("utf-8")
        ordering_key = event.get("user_id", "")

        # Attributes allow Pub/Sub to pass event timestamp for watermark extraction if configured
        future = publisher.publish(
            topic_path,
            data=payload_bytes,
            ordering_key=ordering_key,
            event_timestamp=event["event_timestamp"],
            event_type=event["event_type"],
        )
        msg_id = future.result()
        published_count += 1
        logging.info(
            "[%d/%d] Published msg_id=%s user=%s type=%s late=%s",
            idx,
            len(events),
            msg_id,
            ordering_key,
            event["event_type"],
            event.get("is_simulated_late", False),
        )
        time.sleep(delay)

    logging.info("Successfully published %d messages to %s", published_count, topic_path)


def write_to_file(filepath: str, events: list) -> None:
    """Writes events as a JSON array to a local file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)
    logging.info("Successfully generated %d events to %s", len(events), filepath)


def main():
    parser = argparse.ArgumentParser(description="Streaming Telemetry Event Generator for Dataflow Chapter 03")
    parser.add_argument("--total-events", type=int, default=30, help="Total events to generate (default: 30)")
    parser.add_argument("--rate", type=float, default=2.0, help="Events published per second (default: 2.0)")
    parser.add_argument("--late-rate", type=float, default=0.15, help="Fraction of late-arriving events (default: 0.15)")
    parser.add_argument("--poison-rate", type=float, default=0.10, help="Fraction of poison records (default: 0.10)")
    parser.add_argument("--project", type=str, default=None, help="GCP Project ID for Pub/Sub publishing")
    parser.add_argument("--topic", type=str, default=None, help="Pub/Sub Topic name for publishing")
    parser.add_argument("--output-file", type=str, default=None, help="Local output JSON file path")

    args = parser.parse_args()

    # Generate synthetic event stream
    events = []
    for i in range(1, args.total_events + 1):
        user_id = random.choice(USER_IDS)
        is_late = random.random() < args.late_rate
        is_poison = random.random() < args.poison_rate
        event = create_event(i, user_id, simulate_late=is_late, simulate_poison=is_poison)
        events.append(event)

    if args.project and args.topic:
        publish_to_pubsub(args.project, args.topic, events, args.rate)
    elif args.output_file:
        write_to_file(args.output_file, events)
    else:
        # Output directly to stdout as formatted JSON
        print(json.dumps(events, indent=2))


if __name__ == "__main__":
    main()
