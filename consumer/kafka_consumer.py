from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "10.103.77.213:9092"
KAFKA_TOPIC = "transactions-run1"

# Schema for incoming transaction JSON messages
transaction_schema = StructType([
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
    StructField("account_age_days", StringType(), True)
])

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("FraudMonitoringKafkaConsumer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    # Read transaction messages from Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON messages into structured transaction records
    transactions = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(
            from_json(
                col("json_value"),
                transaction_schema
            ).alias("transaction")
        )
        .select("transaction.*")
        # Convert amount into a numeric value for fraud calculations
        .withColumn("amount", col("amount").cast("double"))
    )

    # Display transactions in the console
    query = (
        transactions.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 10)
        .option(
            "checkpointLocation",
            "file:///tmp/fraud-monitoring-kafka-consumer-checkpoint-v2"
        )
        .start()
    )

    print("Spark Kafka consumer started.")
    print(f"Reading topic: {KAFKA_TOPIC}")

    query.awaitTermination()

finally:
    spark.stop()
