import json
import time
import uuid
import argparse
from collections import Counter
from kafka import KafkaConsumer


def verify_kafka(topic, expected_count, bootstrap_servers, idle_timeout):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"verification-{uuid.uuid4()}",
    )

    print(f"\nVerifying Kafka topic: {topic}")
    print("Reading messages from the beginning...\n")

    transactions = []
    malformed = 0
    last_message_time = time.time()

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            if records:
                last_message_time = time.time()

                for partition_records in records.values():
                    for message in partition_records:
                        try:
                            data = json.loads(message.value.decode("utf-8"))
                            transactions.append(data["transaction_id"])
                        except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
                            malformed += 1

            elif time.time() - last_message_time >= idle_timeout:
                break

    finally:
        consumer.close()

    counts = Counter(transactions)

    total = len(transactions)
    unique = len(counts)
    duplicates = total - unique

    print("========== KAFKA VERIFICATION ==========")
    print(f"Topic                  : {topic}")
    print(f"Total valid messages   : {total}")
    print(f"Unique transaction IDs : {unique}")
    print(f"Duplicate records      : {duplicates}")
    print(f"Malformed messages     : {malformed}")
    print(f"Expected messages      : {expected_count}")

    print("\n========== VERIFICATION RESULT ==========")

    if total == expected_count and malformed == 0:
        print("SUCCESS: All expected messages were read.")
    else:
        print("WARNING: Message count or data validation failed.")

    print("\nVerification completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--topic", default="transactions-run1")
    parser.add_argument("--expected", type=int, default=2000)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--idle-timeout", type=int, default=5)

    args = parser.parse_args()

    verify_kafka(
        args.topic,
        args.expected,
        args.bootstrap,
        args.idle_timeout,
    )
