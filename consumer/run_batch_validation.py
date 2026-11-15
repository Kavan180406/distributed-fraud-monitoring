from pyspark.sql import SparkSession
from fraud_rules import evaluate_batch


def main():

    spark = (
        SparkSession.builder
        .appName("FraudBatchValidation")
        .master("local[2]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        dataset_path = "producer/data/transactions.csv"

        print("\n========== READING DATASET ==========")

        transactions = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(dataset_path)
        )

        input_count = transactions.count()

        print("Input transaction rows:", input_count)

        print("\n========== RUNNING FRAUD RULES ==========")

        decisions, alerts = evaluate_batch(transactions)

        print("\n========== DECISION SUMMARY ==========")

        decisions.groupBy("decision").count().orderBy(
            "decision"
        ).show()

        print("Unique transactions evaluated:", decisions.count())

        print("Total fraud alerts:", alerts.count())

        print("\n========== INDIVIDUAL RULE COUNTS ==========")

        for rule in ["R1", "R2", "R3", "R4"]:

            count = decisions.filter(
                decisions[rule]
            ).count()

            print(f"{rule}: {count}")

        print("\n========== FRAUD ALERT SAMPLES ==========")

        alerts.select(
            "transaction_id",
            "source_account",
            "destination_account",
            "amount",
            "currency",
            "triggered_rules",
            "rule_count",
            "decision"
        ).show(20, truncate=False)

        print("\n========== VALIDATION COMPLETED ==========")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
