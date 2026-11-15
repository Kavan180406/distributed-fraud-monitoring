from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, count
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("InvalidTransactionQualityTest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n===== INVALID TRANSACTION DATA QUALITY TEST =====")

# Read the existing local test dataset
df = spark.read.csv(
    "file:///home/pes2ug24cs313/distributed-fraud-monitoring/verifier/data/invalid_transactions.csv",
    header=True,
    inferSchema=True
)

# Detect duplicate transaction IDs
window = Window.partitionBy("transaction_id")

df = df.withColumn(
    "id_count",
    count("*").over(window)
)

# Validate each record
validated_df = df.withColumn(
    "validation_status",
    when(
        col("transaction_id").isNull() |
        (trim(col("transaction_id")) == ""),
        "MISSING_TRANSACTION_ID"
    )
    .when(
        col("account_id").isNull() |
        (trim(col("account_id")) == ""),
        "MISSING_ACCOUNT_ID"
    )
    .when(
        col("amount").isNull() |
        (col("amount") < 0),
        "INVALID_AMOUNT"
    )
    .when(
        col("is_fraud").isNull() |
        (~col("is_fraud").isin(0, 1)),
        "INVALID_FRAUD_LABEL"
    )
    .when(
        (col("id_count") > 1),
        "DUPLICATE_TRANSACTION_ID"
    )
    .otherwise("VALID")
)

print("\n===== VALIDATION RESULTS =====")
validated_df.select(
    "transaction_id",
    "account_id",
    "amount",
    "is_fraud",
    "validation_status"
).show(truncate=False)

print("\n===== DATA QUALITY SUMMARY =====")

print("Total records:", validated_df.count())

print(
    "Valid records:",
    validated_df.filter(
        col("validation_status") == "VALID"
    ).count()
)

print(
    "Invalid records:",
    validated_df.filter(
        col("validation_status") != "VALID"
    ).count()
)

print("\n===== ERROR CATEGORY SUMMARY =====")

validated_df.groupBy(
    "validation_status"
).count().show(truncate=False)

print("\n===== INVALID DATA QUALITY TEST COMPLETED =====")

spark.stop()
