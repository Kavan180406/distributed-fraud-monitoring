# VM3 Setup Guide

## Components
- Hadoop HDFS 3.3.6
- Apache Spark 3.3.4
- Apache Iceberg 1.8.1
- Ground-truth fraud verifier
- Data quality validation
- Fraud analytics

## Environment
- Ubuntu 24.04
- Python 3.12.3
- Java OpenJDK 11.0.33
- Hadoop 3.3.6
- Spark 3.3.4
- Scala 2.12.15

## Network
VM3 ZeroTier IP: 10.103.77.92
HDFS: hdfs://10.103.77.92:9000

## HDFS Verification
Check Hadoop processes:

    jps

Check HDFS health:

    hdfs dfsadmin -report

Check project storage:

    hdfs dfs -ls -R /fraud-monitoring

## Iceberg
Catalog: demo
Table: demo.fraud.transactions
Warehouse: /fraud-monitoring/iceberg-warehouse

Table schema:
- transaction_id STRING
- account_id STRING
- amount DOUBLE
- is_fraud INT

Iceberg runtime:
iceberg-spark-runtime-3.3_2.12-1.8.1.jar

## Run Ground-Truth Verifier

    cd verifier
    python3 -m unittest discover -s tests -v

## Run Analytics

    python3 data_quality.py
    python3 fraud_analytics.py
    python3 high_value_analysis.py
    python3 account_fraud_analysis.py

## Verify Iceberg Storage

    hdfs dfs -ls -R /fraud-monitoring/iceberg-warehouse/fraud/transactions

## Notes
- Start HDFS before executing jobs that access HDFS.
- Make the Iceberg runtime JAR available to Spark.
- Use VM3's reachable network address for remote access.
- Avoid rerunning sample insertion scripts unless additional records are intended.
