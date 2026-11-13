# Kafka Setup and Execution Documentation

## 1. Project Overview

Apache Kafka is used as the distributed event streaming platform in the Distributed Fraud Monitoring System.

The transaction producer publishes payment events to Kafka. Downstream Spark Structured Streaming processes these events for fraud detection.

## 2. Environment

- Operating System: Ubuntu 24.04
- Java: OpenJDK 17
- Apache Kafka: 4.4.0
- Python: 3.12.3
- Python Kafka Library: kafka-python
- Kafka Mode: KRaft
- Kafka Port: 9092

## 3. Project Components

- `producer/kafka_producer.py` – Publishes transaction events.
- `producer/inspect_dataset.py` – Inspects the transaction dataset.
- `producer/verify_kafka.py` – Verifies Kafka messages.
- `producer/data/transactions.csv` – Input transaction dataset.

## 4. Start Kafka Broker

Navigate to the Kafka installation directory:

    cd ~/tools/kafka_2.13-4.4.0

Start the broker:

    bin/kafka-server-start.sh config/server.properties

Keep the broker terminal running.

## 5. Kafka Topic Configuration

Topic name: transactions-run1

Configuration:
- Partitions: 3
- Replication factor: 1

Describe the topic:

    bin/kafka-topics.sh --describe --topic transactions-run1 --bootstrap-server localhost:9092

## 6. Python Environment

Activate the project virtual environment:

    cd ~/distributed-fraud-monitoring
    source .venv/bin/activate

Install dependencies:

    python -m pip install -r producer/requirements.txt

## 7. Dataset Inspection

Execute:

    python producer/inspect_dataset.py

Dataset statistics:

- Total records: 2,000
- Unique transaction IDs: 1,990
- Duplicate records: 10
- Columns: 15

## 8. Publish Transactions

Execute:

    python producer/kafka_producer.py

The producer publishes all 2,000 records to the Kafka topic, preserving the original CSV order and duplicate records.

## 9. Kafka Message Verification

Execute:

    python producer/verify_kafka.py

Verified results:

- Total valid messages: 2,000
- Unique transaction IDs: 1,990
- Duplicate records: 10
- Malformed messages: 0

Result: SUCCESS.

## 10. Distributed Network Configuration

The project uses ZeroTier for communication between team members' machines.

VM1 ZeroTier address:

    10.103.77.213

The initial Kafka test uses localhost:9092.

Remote Kafka connectivity through ZeroTier and advertised listener configuration are pending integration testing.

## 11. Current Status

Completed:
- Kafka installation and KRaft setup.
- Kafka topic creation.
- Transaction producer implementation.
- Dataset inspection.
- Publication of 2,000 transaction records.
- Independent Kafka message verification.

Pending:
- Remote Kafka connectivity.
- Spark Structured Streaming integration.
- Distributed processing and fault-tolerance experiments.
