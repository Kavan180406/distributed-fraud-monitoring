# VM2 - Spark Master Setup

## Team Member
Gokul - VM2

## Assigned Responsibilities
- Spark Master
- Spark Driver
- Kafka Consumer

## Environment Details

| Component | Version |
|---|---|
| Operating System | Ubuntu 24.04 |
| Python | 3.12.3 |
| Java | OpenJDK 11 |
| Apache Spark | 3.3.4 |
| Scala | 2.12.15 |
| kafka-python | 3.0.13 |
| Pandas | 3.1.1 |

## Network Configuration

- ZeroTier Network: BigData-Lab
- VM2 ZeroTier IP: 10.103.77.41
- VM1 ZeroTier IP: 10.103.77.213

## Spark Configuration

File: spark-env.sh

```bash
export SPARK_LOCAL_IP=10.103.77.41
export SPARK_MASTER_HOST=10.103.77.41 
## Spark Master Details

- Master URL: spark://10.103.77.41:7077
- Web UI: http://10.103.77.41:8080
- Status: Running and verified

## Kafka Connectivity

- Bootstrap Server: 10.103.77.213:9092
- TCP connectivity: Successful
- Python KafkaAdminClient: Successful

Available topics:
- transactions-run1
- transactions-under-1min
- transactions-over-1min

## Pending Work

- Connect Spark Workers from VM3 and VM4.
- Implement Spark Structured Streaming Kafka consumer.
- Implement fraud detection rules R1-R5.
- Configure event-time windows and watermarks.
- Integrate HDFS and Apache Iceberg.
- Implement checkpointing and recovery.
- Perform integration and verification experiments.
