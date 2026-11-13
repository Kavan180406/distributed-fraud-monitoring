import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer


BASE_DIR = Path(__file__).resolve().parents[1]
CSV_FILE = BASE_DIR / "producer" / "data" / "transactions.csv"

BROKER = "localhost:9092"

EXPERIMENTS = {
    "under": {
        "topic": "transactions-under-1min",
        "earlier_id": "TXN0000003",
        "later_id": "TXN0000004",
        "expected_lateness": 47,
    },
    "over": {
        "topic": "transactions-over-1min",
        "earlier_id": "TXN0000422",
        "later_id": "TXN0000423",
        "expected_lateness": 91,
    },
}


def load_transactions():
    with open(CSV_FILE, "r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=10,
    )


def run_experiment(name):
    config = EXPERIMENTS[name]
    records = load_transactions()

    by_id = {
        row["transaction_id"]: row
        for row in records
    }

    earlier = by_id[config["earlier_id"]]
    later = by_id[config["later_id"]]

    print("\n========== DISORDER EXPERIMENT ==========")
    print("Experiment:", name)
    print("Topic:", config["topic"])
    print("Expected lateness:", config["expected_lateness"], "seconds")

    producer = create_producer()

    try:
        # Publish later event first.
        first = producer.send(
            config["topic"],
            key=later["transaction_id"],
            value=later,
        )
        first_metadata = first.get(timeout=30)

        print(
            f"[1] Published {later['transaction_id']} "
            f"| Partition: {first_metadata.partition} "
            f"| Offset: {first_metadata.offset}"
        )

        # Ensure the first event has been acknowledged before sending the older one.
        time.sleep(1)

        # Publish earlier event second.
        second = producer.send(
            config["topic"],
            key=earlier["transaction_id"],
            value=earlier,
        )
        second_metadata = second.get(timeout=30)

        print(
            f"[2] Published {earlier['transaction_id']} "
            f"| Partition: {second_metadata.partition} "
            f"| Offset: {second_metadata.offset}"
        )

        producer.flush()

        print("\nExperiment messages published successfully.")

    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--experiment",
        choices=["under", "over"],
        required=True,
    )

    args = parser.parse_args()

    run_experiment(args.experiment)
