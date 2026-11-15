from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

KAFKA_SERVER = "10.103.77.213:9092"
KAFKA_TOPIC = "transactions-run1"

CHECKPOINT = (
    "file:///home/pes2ug24cs246/distributed-fraud-monitoring/"
    "checkpoints/vm2-streaming-ingest-test-v2"
)
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("source_account", StringType(), True),
    StructField("destination_account", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("account_age_days", StringType(), True),
])


def main():
    spark = (
    SparkSession.builder
    .appName("FraudStreamingIngestion")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "500")
            .load()
        )

        parsed = (
            kafka_stream
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    schema
                ).alias("data")
            )
            .select("data.*")
            .withColumn(
                "event_time",
                F.to_timestamp(
                    "event_timestamp",
                    "yyyy-MM-dd'T'HH:mm:ssX"
                )
            )
            .withColumn(
                "amount",
                F.col("amount").cast("decimal(18,2)")
            )
            .withColumn(
                "account_age_days",
                F.col("account_age_days").cast("integer")
            )
            .withColumn(
                "currency",
                F.upper(F.trim(F.col("currency")))
            )
            .filter(
                F.col("transaction_id").isNotNull()
                & F.col("event_time").isNotNull()
                & F.col("amount").isNotNull()
            )
        )

        # Retain event-time state for one minute to handle late data.
        deduplicated = (
            parsed
            .withWatermark("event_time", "1 minute")
            .dropDuplicates(["transaction_id"])
        )

        query = (
            deduplicated
            .select(
                "event_id",
                "transaction_id",
                "source_account",
                "destination_account",
                "event_time",
                "amount",
                "currency",
                "transaction_type",
                "channel"
            )
            .writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", "20")
            .option("checkpointLocation", CHECKPOINT)
            .trigger(processingTime="5 seconds")
            .start()
        )

        print("Fraud streaming ingestion started.")
        print("Kafka:", KAFKA_SERVER)
        print("Topic:", KAFKA_TOPIC)
        print("Watermark: 1 minute")
        print("Checkpoint:", CHECKPOINT)

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
