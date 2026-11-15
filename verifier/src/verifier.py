import csv
import os
import sys
import subprocess
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GROUND_TRUTH_FILE = os.path.join(BASE_DIR, "data", "ground_truth.csv")
PREDICTIONS_FILE = os.path.join(BASE_DIR, "data", "predictions.csv")

REPORT_FILE = os.path.join(BASE_DIR, "reports", "verification_report.txt")
ANALYSIS_FILE = os.path.join(BASE_DIR, "reports", "transaction_analysis.csv")


def load_csv(filename):
    with open(filename, "r", newline="") as file:
        return list(csv.DictReader(file))


def verify_predictions(ground_truth, predictions):

    actual = {
        row["transaction_id"]: int(row["is_fraud"])
        for row in ground_truth
    }

    predicted = {
        row["transaction_id"]: int(row["predicted_fraud"])
        for row in predictions
    }

    if len(actual) != len(ground_truth):
        raise ValueError("Duplicate transaction IDs in ground truth")

    if len(predicted) != len(predictions):
        raise ValueError("Duplicate transaction IDs in predictions")

    if set(actual) != set(predicted):
        raise ValueError("Transaction IDs do not match")

    for value in list(actual.values()) + list(predicted.values()):
        if value not in (0, 1):
            raise ValueError("Fraud labels must be 0 or 1")

    tp = tn = fp = fn = 0
    transaction_results = []

    for transaction_id in actual:

        a = actual[transaction_id]
        p = predicted[transaction_id]

        if a == 1 and p == 1:
            classification = "TP"
            tp += 1

        elif a == 0 and p == 0:
            classification = "TN"
            tn += 1

        elif a == 0 and p == 1:
            classification = "FP"
            fp += 1

        else:
            classification = "FN"
            fn += 1

        transaction_results.append({
            "transaction_id": transaction_id,
            "actual": a,
            "predicted": p,
            "classification": classification
        })

    total = tp + tn + fp + fn

    accuracy = (tp + tn) / total if total else 0
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) else 0
    )

    fpr = fp / (fp + tn) if (fp + tn) else 0

    return {
        "total": total,
        "TP": tp,
        "TN": tn,
        "FP": fp,
        "FN": fn,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "false_positive_rate": fpr,
        "transactions": transaction_results
    }


def generate_report(results):

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report = f"""
========================================
       FRAUD VERIFICATION REPORT
========================================

Generated on: {timestamp}

Total Transactions: {results['total']}

CONFUSION MATRIX
----------------------------------------
True Positives  : {results['TP']}
True Negatives  : {results['TN']}
False Positives : {results['FP']}
False Negatives : {results['FN']}

EVALUATION METRICS
----------------------------------------
Accuracy              : {results['accuracy']:.2%}
Precision             : {results['precision']:.2%}
Recall                : {results['recall']:.2%}
F1 Score              : {results['f1_score']:.2%}
False Positive Rate   : {results['false_positive_rate']:.2%}

========================================
         VERIFICATION COMPLETED
========================================
"""

    with open(REPORT_FILE, "w") as file:
        file.write(report)

    print(report)
    print("Report saved:", REPORT_FILE)


def generate_transaction_analysis(results):

    with open(ANALYSIS_FILE, "w", newline="") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "transaction_id",
                "actual",
                "predicted",
                "classification"
            ]
        )

        writer.writeheader()
        writer.writerows(results["transactions"])

    print("Transaction analysis saved:", ANALYSIS_FILE)


def upload_to_hdfs():

    files = [
        REPORT_FILE,
        ANALYSIS_FILE
    ]

    destination = "/fraud-monitoring/processed/"

    for filename in files:

        result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", filename, destination],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("Uploaded to HDFS:", os.path.basename(filename))
        else:
            print("HDFS upload failed:", result.stderr)


def main():

    try:

        ground_truth = load_csv(GROUND_TRUTH_FILE)
        predictions = load_csv(PREDICTIONS_FILE)

        results = verify_predictions(ground_truth, predictions)

        generate_report(results)

        generate_transaction_analysis(results)

        upload_to_hdfs()

    except (FileNotFoundError, KeyError, ValueError) as error:

        print("Verification Error:", error)
        sys.exit(1)


if __name__ == "__main__":
    main()
