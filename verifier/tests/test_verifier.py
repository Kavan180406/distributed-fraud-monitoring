import unittest
import sys
import os

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "src")
)

from verifier import verify_predictions


class TestFraudVerifier(unittest.TestCase):

    def setUp(self):
        self.ground_truth = [
            {"transaction_id": "TXN001", "is_fraud": "0"},
            {"transaction_id": "TXN002", "is_fraud": "1"},
            {"transaction_id": "TXN003", "is_fraud": "0"},
            {"transaction_id": "TXN004", "is_fraud": "1"}
        ]

        self.predictions = [
            {"transaction_id": "TXN001", "predicted_fraud": "0"},
            {"transaction_id": "TXN002", "predicted_fraud": "1"},
            {"transaction_id": "TXN003", "predicted_fraud": "1"},
            {"transaction_id": "TXN004", "predicted_fraud": "0"}
        ]

    def test_confusion_matrix(self):
        result = verify_predictions(
            self.ground_truth, self.predictions
        )

        self.assertEqual(result["TP"], 1)
        self.assertEqual(result["TN"], 1)
        self.assertEqual(result["FP"], 1)
        self.assertEqual(result["FN"], 1)

    def test_accuracy(self):
        result = verify_predictions(
            self.ground_truth, self.predictions
        )

        self.assertEqual(result["accuracy"], 0.5)

    def test_duplicate_ground_truth(self):
        data = self.ground_truth + [self.ground_truth[0]]

        with self.assertRaises(ValueError):
            verify_predictions(data, self.predictions)

    def test_duplicate_predictions(self):
        data = self.predictions + [self.predictions[0]]

        with self.assertRaises(ValueError):
            verify_predictions(self.ground_truth, data)

    def test_mismatched_transactions(self):
        data = self.predictions.copy()
        data[0] = {
            "transaction_id": "TXN999",
            "predicted_fraud": "0"
        }

        with self.assertRaises(ValueError):
            verify_predictions(self.ground_truth, data)

    def test_invalid_ground_truth_label(self):
        data = self.ground_truth.copy()
        data[0] = {
            "transaction_id": "TXN001",
            "is_fraud": "5"
        }

        with self.assertRaises(ValueError):
            verify_predictions(data, self.predictions)

    def test_invalid_prediction_label(self):
        data = self.predictions.copy()
        data[0] = {
            "transaction_id": "TXN001",
            "predicted_fraud": "3"
        }

        with self.assertRaises(ValueError):
            verify_predictions(self.ground_truth, data)

    # NEW TEST CASES

    def test_empty_ground_truth(self):
        with self.assertRaises(ValueError):
            verify_predictions([], self.predictions)

    def test_empty_predictions(self):
        with self.assertRaises(ValueError):
            verify_predictions(self.ground_truth, [])

    def test_all_predictions_correct(self):
        predictions = [
            {"transaction_id": "TXN001", "predicted_fraud": "0"},
            {"transaction_id": "TXN002", "predicted_fraud": "1"},
            {"transaction_id": "TXN003", "predicted_fraud": "0"},
            {"transaction_id": "TXN004", "predicted_fraud": "1"}
        ]

        result = verify_predictions(
            self.ground_truth, predictions
        )

        self.assertEqual(result["accuracy"], 1.0)
        self.assertEqual(result["FP"], 0)
        self.assertEqual(result["FN"], 0)

    def test_all_predictions_incorrect(self):
        predictions = [
            {"transaction_id": "TXN001", "predicted_fraud": "1"},
            {"transaction_id": "TXN002", "predicted_fraud": "0"},
            {"transaction_id": "TXN003", "predicted_fraud": "1"},
            {"transaction_id": "TXN004", "predicted_fraud": "0"}
        ]

        result = verify_predictions(
            self.ground_truth, predictions
        )

        self.assertEqual(result["accuracy"], 0.0)
        self.assertEqual(result["TP"], 0)
        self.assertEqual(result["TN"], 0)

    def test_missing_transaction_id(self):
        data = self.predictions.copy()
        data[0] = {
            "transaction_id": "",
            "predicted_fraud": "0"
        }

        with self.assertRaises(ValueError):
            verify_predictions(self.ground_truth, data)


if __name__ == "__main__":
    unittest.main(verbosity=2)
