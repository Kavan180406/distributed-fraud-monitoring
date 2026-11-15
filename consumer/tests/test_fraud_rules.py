import unittest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from consumer.fraud_rules import evaluate_batch


class FraudRulesTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("FraudRulesUnitTests")
            .master("local[*]")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_transactions(self, records):
        columns = [
            "event_id",
            "transaction_id",
            "source_account",
            "destination_account",
            "event_timestamp",
            "amount",
            "currency"
        ]

        return self.spark.createDataFrame(records, columns)

    def create_record(self, event_id, txn_id, source, destination,
                      timestamp, amount, currency="INR"):

        return (
            event_id,
            txn_id,
            source,
            destination,
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            amount,
            currency
        )

    def evaluate(self, records):
        df = self.make_transactions(records)
        decisions, alerts = evaluate_batch(df)

        results = {
            row["transaction_id"]: row.asDict()
            for row in decisions.collect()
        }

        return results, alerts.count()

    def test_R1_high_value_transaction(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                "E1", "T1", "A1", "B1", now, 50000
            )
        ]

        results, _ = self.evaluate(records)

        self.assertTrue(results["T1"]["R1"])
        self.assertEqual(results["T1"]["decision"], "REVIEW")

    def test_R1_below_threshold(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                "E1", "T1", "A1", "B1", now, 49999
            )
        ]

        results, _ = self.evaluate(records)

        self.assertFalse(results["T1"]["R1"])
        self.assertEqual(results["T1"]["decision"], "APPROVE")

    def test_R2_five_transactions(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                f"E{i}", f"T{i}", "A1", f"B{i}",
                now + timedelta(seconds=i * 10), 1000
            )
            for i in range(1, 6)
        ]

        results, _ = self.evaluate(records)

        for result in results.values():
            self.assertTrue(result["R2"])
            self.assertEqual(result["decision"], "REVIEW")

    def test_R2_four_transactions(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                f"E{i}", f"T{i}", "A1", f"B{i}",
                now + timedelta(seconds=i * 10), 1000
            )
            for i in range(1, 5)
        ]

        results, _ = self.evaluate(records)

        for result in results.values():
            self.assertFalse(result["R2"])
            self.assertEqual(result["decision"], "APPROVE")

    def test_R3_total_above_six_lakhs(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                "E1", "T1", "A1", "B1", now, 300001
            ),
            self.create_record(
                "E2", "T2", "A1", "B2",
                now + timedelta(seconds=10), 300001
            )
        ]

        results, _ = self.evaluate(records)

        for result in results.values():
            self.assertTrue(result["R3"])
            self.assertTrue(result["R1"])
            self.assertEqual(result["decision"], "BLOCK")

    def test_R3_exactly_six_lakhs(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                "E1", "T1", "A1", "B1", now, 300000
            ),
            self.create_record(
                "E2", "T2", "A1", "B2",
                now + timedelta(seconds=10), 300000
            )
        ]

        results, _ = self.evaluate(records)

        for result in results.values():
            self.assertFalse(result["R3"])
            self.assertTrue(result["R1"])
            self.assertEqual(result["decision"], "REVIEW")

    def test_R4_three_transactions_same_destination(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                f"E{i}", f"T{i}", "A1", "B1",
                now + timedelta(seconds=i * 10), 1000
            )
            for i in range(1, 4)
        ]

        results, _ = self.evaluate(records)

        for result in results.values():
            self.assertTrue(result["R4"])
            self.assertEqual(result["decision"], "REVIEW")

    def test_multiple_rules_trigger_block(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                f"E{i}", f"T{i}", "A1", "B1",
                now + timedelta(seconds=i * 10), 1000
            )
            for i in range(1, 6)
        ]

        results, _ = self.evaluate(records)

        for result in results.values():
            self.assertTrue(result["R2"])
            self.assertTrue(result["R4"])
            self.assertEqual(result["decision"], "BLOCK")

    def test_duplicate_transaction_deduplication(self):
        now = datetime(2026, 11, 15, 10, 0, 0)

        records = [
            self.create_record(
                "E1", "T1", "A1", "B1", now, 1000
            ),
            self.create_record(
                "E2", "T1", "A1", "B1",
                now + timedelta(seconds=10), 95000
            )
        ]

        results, _ = self.evaluate(records)

        self.assertEqual(len(results), 1)
        self.assertEqual(results["T1"]["amount"], 1000)
        self.assertEqual(results["T1"]["decision"], "APPROVE")


if __name__ == "__main__":
    unittest.main(verbosity=2)

