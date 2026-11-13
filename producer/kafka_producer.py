import csv
import json
import time
import argparse
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "transactions-run1"

# Dataset location
BASE_DIR = Path(__file__).resolve().parent
CSV_FILE = BASE_DIR / "data" / "transactions.csv"


def create_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,

        key_serializer=lambda key: key.encode("utf-8"),

        value_serializer=lambda value: json.dumps(value).encode("utf-8"),

        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1
    )


def stream_transactions(interval):

    if not CSV_FILE.exists():
        raise FileNotFoundError(f"Dataset not found: {CSV_FILE}")

    producer = create_producer()

    total_sent = 0
    failed = 0

    print("\n====================================")
    print(" DISTRIBUTED FRAUD MONITORING SYSTEM")
    print(" KAFKA TRANSACTION PRODUCER")
    print("====================================")

    print(f"Broker: {KAFKA_BROKER}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Dataset: {CSV_FILE}")
    print(f"Interval: {interval} seconds")

    try:

        with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                transaction_id = row.get("transaction_id")

                if not transaction_id:
                    print("[SKIPPED] Missing transaction ID")
                    failed += 1
                    continue

                try:

                    future = producer.send(
                        KAFKA_TOPIC,
                        key=transaction_id,
                        value=row
                    )

                    metadata = future.get(timeout=30)

                    total_sent += 1

                    print(
                        f"[SENT] {transaction_id} | "
                        f"Partition: {metadata.partition} | "
                        f"Offset: {metadata.offset}"
                    )

                except KafkaError as error:

                    failed += 1
                    print(f"[ERROR] {transaction_id}: {error}")

                time.sleep(interval)

    except KeyboardInterrupt:

        print("\nProducer interrupted.")

    finally:

        producer.flush()
        producer.close()

        print("\n========== PRODUCER SUMMARY ==========")
        print(f"Successfully sent: {total_sent}")
        print(f"Failed: {failed}")
        print("=======================================")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--interval",
        type=float,
        default=0.1
    )

    args = parser.parse_args()

    if args.interval < 0:
        parser.error("Interval cannot be negative")

    stream_transactions(args.interval)
