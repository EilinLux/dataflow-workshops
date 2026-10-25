"""
Streaming Unit Tests with TestStream — Chapter 07
=================================================
Demonstrates deterministic streaming verification using:
  - TestStream
  - Watermark advancement
  - Fixed window aggregation verification
"""

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows, TimestampedValue


class TestStreamingPipelines(unittest.TestCase):
    """Validates streaming temporal windowing and watermark behavior deterministically."""

    def test_windowing_with_test_stream(self):
        """Advances watermark and validates windowed aggregation results."""
        # Define deterministic event stream:
        # Window 1: [0 -> 60) receives 2 events
        # Watermark advances to 60 -> window 1 closes and fires!
        # Window 2: [60 -> 120) receives 1 event
        # Watermark advances to infinity -> window 2 closes and fires!
        test_stream = (
            TestStream()
            .add_elements([
                TimestampedValue(("alice", 10.0), timestamp=15),
                TimestampedValue(("alice", 25.0), timestamp=30),
            ])
            .advance_watermark_to(60)
            .add_elements([
                TimestampedValue(("alice", 50.0), timestamp=75),
            ])
            .advance_watermark_to_infinity()
        )

        with TestPipeline() as p:
            windowed_totals = (
                p
                | "IngestTestStream" >> test_stream
                | "Fixed60sWindow" >> beam.WindowInto(FixedWindows(60))
                | "SumPerUser" >> beam.CombinePerKey(sum)
            )

            # Expected: Window 1 emits ('alice', 35.0), Window 2 emits ('alice', 50.0)
            expected = [("alice", 35.0), ("alice", 50.0)]
            assert_that(windowed_totals, equal_to(expected))


if __name__ == "__main__":
    unittest.main()
