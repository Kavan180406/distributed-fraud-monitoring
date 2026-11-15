# Iceberg Storage – VM3

Apache Iceberg 1.8.1 is used with Spark 3.3.4 and Hadoop 3.3.6.

## Catalog
- Catalog: demo
- Type: Hadoop
- Database: fraud
- Table: transactions
- Identifier: demo.fraud.transactions

## Warehouse
hdfs://localhost:9000/fraud-monitoring/iceberg-warehouse

For remote clients, use VM3's reachable network IP.

## Schema

| Column | Type |
|---|---|
| transaction_id | STRING |
| account_id | STRING |
| amount | DOUBLE |
| is_fraud | INT |

## Configuration
See conf/iceberg-spark.conf.

## Scripts
- scripts/test_iceberg.py
- scripts/append_test_transaction.py
- scripts/create_remote_table.py
- scripts/fix_table_location.py

## HDFS Verification

    hdfs dfs -ls -R /fraud-monitoring/iceberg-warehouse/fraud/transactions

The table stores Parquet data and Iceberg metadata and snapshots.

## Runtime JAR
iceberg-spark-runtime-3.3_2.12-1.8.1.jar

The runtime JAR must be available to Spark. It is not included in this repository.
