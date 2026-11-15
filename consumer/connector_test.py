from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaConnectorTest")
    .getOrCreate()
)

try:
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.103.77.213:9092")
        .option("subscribe", "transactions-run1")
        .option("startingOffsets", "earliest")
        .load()
    )

    print("SUCCESS: Spark Kafka connector loaded!")
    kafka_df.printSchema()

finally:
    spark.stop()
