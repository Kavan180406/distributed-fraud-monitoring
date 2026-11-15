# Distributed Fraud Monitoring System

## 1. Project Overview

The Distributed Fraud Monitoring System is designed to process transaction data, identify potentially fraudulent activities, validate transaction records, and evaluate fraud predictions against ground-truth labels.

This repository contains the VM3 implementation, focusing on HDFS storage, Apache Spark analytics, data-quality validation, and ground-truth verification.

## 2. VM3 Responsibilities

The responsibilities assigned to VM3 are:

- HDFS setup and storage management.
- Spark-based transaction analytics.
- Data-quality validation.
- Ground-truth verification.
- Evaluation of fraud detection predictions.
- Storage of processed results and reports in HDFS.
- Spark Worker integration with the Spark Master.
- Apache Iceberg integration.

**Implementation status:** HDFS, local Spark analytics, data-quality validation, and ground-truth verification have been implemented and tested. Spark Worker integration and Iceberg integration remain pending.

## 3. Technologies Used

| Technology | Purpose |
|---|---|
| Ubuntu 24.04 | Operating system |
| Java 11 | Java runtime |
| Apache Hadoop 3.3.6 | Distributed storage using HDFS |
| Apache Spark 3.3.4 | Data processing and analytics |
| Python 3.12 | Verification and Spark application scripts |
| PySpark | Python interface for Apache Spark |
| CSV | Transaction and ground-truth datasets |
| ZeroTier | VM network connectivity |

## 4. Project Structure

```text
fraud-verifier/
│
├── account_fraud_analysis.py
├── data_quality.py
├── fraud_analytics.py
├── high_value_analysis.py
├── spark_test.py
│
├── data/
│   ├── ground_truth.csv
│   ├── invalid_transactions.csv
│   └── predictions.csv
│
├── reports/
│   ├── transaction_analysis.csv
│   └── verification_report.txt
│
├── src/
│   └── verifier.py
│
├── tests/
│   └── test_verifier.py
│
└── README.md
```

## 5. HDFS Storage Structure

The project uses HDFS to store transaction data, ground-truth datasets, predictions, and analytics outputs.

Main HDFS directories:

```text
/fraud-monitoring/
│
├── transactions/
├── alerts/
├── ground-truth/
├── processed/
├── test-data/
└── analytics/
    ├── fraud_summary/
    ├── fraudulent_transactions/
    ├── high_value_transactions/
    ├── account_summary/
    ├── fraud_accounts/
    ├── valid_transactions/
    └── invalid_transactions/
```

## 6. Implemented Modules

### 6.1 Spark Integration Test

File: `spark_test.py`

- Creates a Spark session.
- Reads ground-truth data from HDFS.
- Displays transaction records.
- Calculates fraud and legitimate transaction counts.

### 6.2 Fraud Analytics

File: `fraud_analytics.py`

- Reads transaction labels from HDFS.
- Calculates total transactions.
- Counts fraudulent and legitimate transactions.
- Calculates the fraud percentage.
- Saves the summary to HDFS.

### 6.3 High-Value Transaction Analysis

File: `high_value_analysis.py`

- Calculates total transaction amount.
- Calculates average and maximum transaction amounts.
- Identifies fraudulent transactions.
- Identifies transactions above the illustrative threshold of 10,000.
- Saves the results to HDFS.

### 6.4 Account-Level Fraud Analysis

File: `account_fraud_analysis.py`

- Groups transactions by account ID.
- Calculates transaction counts and total amounts.
- Calculates average transaction amounts.
- Calculates fraud counts and fraud percentages.
- Identifies accounts with fraudulent transactions.

### 6.5 Data-Quality Validation

File: `data_quality.py`

Validates transaction records for:

- Missing transaction IDs.
- Missing account IDs.
- Invalid or negative transaction amounts.
- Invalid fraud labels.
- Duplicate transaction IDs.

Separates records into valid and invalid datasets and saves both outputs to HDFS.

### 6.6 Ground-Truth Verification

File: `src/verifier.py`

Compares predicted fraud labels with actual ground-truth labels.

The verifier calculates:

- True Positives (TP)
- True Negatives (TN)
- False Positives (FP)
- False Negatives (FN)
- Accuracy
- Precision
- Recall
- F1-score
- False Positive Rate

It generates:

- `reports/verification_report.txt`
- `reports/transaction_analysis.csv`

### 6.7 Unit Testing

File: `tests/test_verifier.py`

Contains 12 unit tests covering:

- Accuracy calculation.
- Correct predictions.
- Incorrect predictions.
- Confusion matrix.
- Duplicate ground-truth records.
- Duplicate predictions.
- Empty datasets.
- Invalid labels.
- Mismatched transactions.
- Missing transaction IDs.

## 7. Sample Results

### Ground-Truth Verification

| Metric | Result |
|---|---:|
| True Positives | 3 |
| True Negatives | 3 |
| False Positives | 1 |
| False Negatives | 1 |
| Accuracy | 75% |
| Precision | 75% |
| Recall | 75% |
| F1-score | 75% |
| False Positive Rate | 25% |

These results are based on the sample prediction and ground-truth datasets.

### Fraud Analytics

For the sample transaction dataset:

| Metric | Result |
|---|---:|
| Total Transactions | 3 |
| Fraudulent Transactions | 1 |
| Legitimate Transactions | 2 |
| Fraud Percentage | 33.33% |

### Data-Quality Validation

For the original transaction dataset:

| Metric | Result |
|---|---:|
| Total Records | 3 |
| Valid Records | 3 |
| Invalid Records | 0 |

The invalid test dataset was also used to verify duplicate IDs, missing fields, negative amounts, and invalid fraud labels.

## 8. Running the Project

### Start HDFS

```bash
start-dfs.sh
```

Verify:

```bash
jps
```

### Run Spark Analytics

```bash
cd ~/fraud-verifier

python3 spark_test.py
python3 fraud_analytics.py
python3 high_value_analysis.py
python3 account_fraud_analysis.py
python3 data_quality.py
```

### Run Ground-Truth Verification

```bash
python3 -m unittest discover -s tests -v
```

### Inspect HDFS Outputs

```bash
hdfs dfs -ls /fraud-monitoring/analytics
```

Example:

```bash
hdfs dfs -cat /fraud-monitoring/analytics/valid_transactions/part-*.csv
```

## 9. Testing Summary

The following components have been successfully tested:

- HDFS startup and storage.
- Spark and HDFS integration.
- Fraud summary analytics.
- High-value transaction analysis.
- Account-level fraud analysis.
- Data-quality validation using valid and invalid datasets.
- Ground-truth verification.
- All 12 ground-truth verifier unit tests.

## 10. Future Integration

The following tasks remain for completing the distributed VM3 role:

- Connect the Spark Worker to the team's Spark Master.
- Integrate Apache Iceberg for table-based data management.
- Integrate VM3 processing with the complete distributed pipeline.
- Validate end-to-end processing across all project VMs.

## 11. Conclusion

The VM3 implementation provides HDFS-based storage, Spark analytics, transaction data-quality validation, and ground-truth evaluation.

These components support the distributed fraud monitoring pipeline by providing validated transaction data, analytical summaries, and measurable fraud detection evaluation results.
