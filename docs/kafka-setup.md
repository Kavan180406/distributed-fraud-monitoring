# Kafka Setup and Execution Documentation

## 1. Project Overview

Apache Kafka is used as the distributed event streaming platform in the Distributed Fraud Monitoring System.

VM1 is responsible for hosting the Kafka broker and running the transaction producer. The producer publishes transaction events to Kafka for downstream Spark Structured Streaming processing.

## 2. Environment

- Operating System: Ubuntu 24.04
- Java: OpenJDK 17
- Apache Kafka: 4.4.0
- Python: 3.12.3
- Python Kafka Library: kafka-python 3.0.13
- Kafka Mode: KRaft
- Kafka Port: 9092
- Network: ZeroTier

## 3. Project Components

- `producer/kafka_producer.py` – Publishes transaction events to Kafka.
- `producer/inspect_dataset.py` – Inspects the transaction dataset.
- `producer/verify_kafka.py` – Verifies published Kafka messages.
- `producer/data/transactions.csv` – Input transaction dataset.
- `experiments/generate_disorder.py` – Generates out-of-order transaction events.

## 4. Start Kafka Broker

Navigate to the Kafka installation directory:

    cd ~/tools/kafka_2.13-4.4.0

Start the broker:

    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    nohup bin/kafka-server-start.sh config/server.properties > ~/kafka.log 2>&1 &

Check whether Kafka is running:

    jps

The Kafka process should appear in the output.

## 5. Kafka Topic Configuration

Three project topics were created.

| Topic | Partitions | Replication Factor | Purpose |
|---|---:|---:|---|
| transactions-run1 | 3 | 1 | Baseline transaction stream |
| transactions-under-1min | 1 | 1 | Out-of-order events with less than 1-minute delay |
| transactions-over-1min | 1 | 1 | Out-of-order events with more than 1-minute delay |

List topics:

    bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Describe a topic:

    bin/kafka-topics.sh --describe --topic transactions-run1 --bootstrap-server localhost:9092

## 6. Python Environment

Navigate to the project directory:

    cd ~/distributed-fraud-monitoring

Activate the virtual environment:

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

The producer publishes all 2,000 transaction records to `transactions-run1`.

The producer preserves the original CSV order and publishes duplicate records without removing them.

## 9. Kafka Message Verification

Execute:

    python producer/verify_kafka.py

Verified results:

- Total valid messages: 2,000
- Unique transaction IDs: 1,990
- Duplicate records: 10
- Malformed messages: 0

Result: SUCCESS.

## 10. Out-of-Order Event Experiments

The script `experiments/generate_disorder.py` was used to publish transaction events in reverse event-time order.

### Experiment 1: Out-of-order events under 1 minute

Topic: `transactions-under-1min`

Expected event-time difference: 47 seconds.

Publication order:

1. TXN0000004
2. TXN0000003

Kafka successfully published both messages. The consumer confirmed that the later event timestamp was received before the earlier event timestamp.

### Experiment 2: Out-of-order events over 1 minute

Topic: `transactions-over-1min`

Expected event-time difference: 91 seconds.

Publication order:

1. TXN0000423
2. TXN0000422

Kafka successfully published both messages. The consumer confirmed that the later event timestamp was received before the earlier event timestamp.

Note: These experiments verify Kafka publication order. Spark watermark acceptance and late-event handling must be validated separately through the Spark Structured Streaming application.

## 11. Kafka Broker Recovery Test

A broker recovery test was performed.

Steps:

1. Started the Kafka broker.
2. Verified that the Kafka process was running.
3. Stopped the broker process.
4. Confirmed that the Kafka process had stopped.
5. Restarted the Kafka broker.
6. Verified that the Kafka process was running again.

Result: Kafka successfully restarted.

## 12. Data Persistence Verification

After restarting Kafka, the following checks were performed:

- Verified that all three project topics were still available.
- Read the previously published messages from the out-of-order experiment topic.
- Confirmed that the messages remained available after broker restart.

Result: Topic and message persistence was successfully verified.

## 13. Distributed Network Configuration

The project uses ZeroTier for communication between team members' virtual machines.

VM1 ZeroTier address:

    10.103.77.213

The initial Kafka tests were performed using:

    localhost:9092

Remote Kafka connectivity through ZeroTier and advertised listener configuration remain pending integration testing.

## 14. Current Work Status

### Completed

- Kafka 4.4.0 installation and KRaft configuration.
- Kafka broker startup and operation.
- Creation of three project topics.
- Transaction producer implementation.
- Dataset inspection.
- Publication of 2,000 transaction records.
- Kafka message verification.
- Out-of-order event publication under 1 minute.
- Out-of-order event publication over 1 minute.
- Kafka broker stop-and-restart recovery test.
- Topic and message persistence verification.

### Pending

- Remote Kafka connectivity and integration with the Spark consumer.
- Spark watermark behaviour validation by the Spark team.
- Distributed processing and Spark fault-tolerance experiments.
- Final screenshots and experiment evidence compilation.
