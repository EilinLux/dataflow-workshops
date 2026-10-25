"""
Unit Tests for Apache Beam Transforms — Chapter 07
==================================================
Demonstrates deterministic unit testing using:
  - TestPipeline
  - PAssert (assert_that)
  - equal_to matcher
"""

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


class CalculateTaxFn(beam.DoFn):
    """Business logic transform under test."""

    def process(self, element):
        order_id, subtotal = element
        if subtotal < 0:
            yield beam.pvalue.TaggedOutput("errors", (order_id, "Negative subtotal"))
            return

        tax = round(subtotal * 0.05, 2)
        total = round(subtotal + tax, 2)
        yield {"order_id": order_id, "subtotal": subtotal, "tax": tax, "total": total}


class TestOrderTransforms(unittest.TestCase):
    """Unit test suite validating transform correctness without GCP resources."""

    def test_tax_calculation_success(self):
        """Validates that valid orders compute 5% tax correctly."""
        input_data = [
            ("ord-01", 100.0),
            ("ord-02", 50.0),
        ]
        expected_output = [
            {"order_id": "ord-01", "subtotal": 100.0, "tax": 5.0, "total": 105.0},
            {"order_id": "ord-02", "subtotal": 50.0, "tax": 2.5, "total": 52.5},
        ]

        with TestPipeline() as p:
            orders = p | "CreateOrders" >> beam.Create(input_data)
            results = orders | "CalculateTax" >> beam.ParDo(CalculateTaxFn()).with_outputs("errors", main="valid")

            assert_that(results["valid"], equal_to(expected_output))

    def test_tax_calculation_negative_subtotal_dlq(self):
        """Validates that invalid orders route to the dead-letter tag."""
        input_data = [
            ("ord-valid", 40.0),
            ("ord-invalid", -10.0),
        ]
        expected_errors = [
            ("ord-invalid", "Negative subtotal"),
        ]

        with TestPipeline() as p:
            orders = p | "CreateOrders" >> beam.Create(input_data)
            results = orders | "CalculateTax" >> beam.ParDo(CalculateTaxFn()).with_outputs("errors", main="valid")

            assert_that(results["errors"], equal_to(expected_errors))


if __name__ == "__main__":
    unittest.main()
