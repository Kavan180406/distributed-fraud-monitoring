from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    count,
    when
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("FraudDataQualityValidation") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n===== FRAUD DATA QUALITY VALIDATION =====")

# Read transactions from HDFS
df = spark.read.csv(
    "hdfs://localhost:9000/fraud-monitoring/transactions/transactions.csv",
    header=True,
    inferSchema=True
)

print("\n===== ORIGINAL TRANSACTION DATA =====")
df.show()

# Identify invalid records
missing_transaction_id = (
    col("transaction_id").isNull() |
    (trim(col("transaction_id")) == "")
)

missing_account_id = (
    col("account_id").isNull() |
    (trim(col("account_id")) == "")
)

invalid_amount = (
    col("amount").isNull() |
    (col("amount") < 0)
)

invalid_fraud_label = (
    col("is_fraud").isNull() |
    (~col("is_fraud").isin(0, 1))
)

# Find duplicate transaction IDs
duplicate_ids = df.groupBy("transaction_id") \
    .count() \
    .filter(col("count") > 1) \
    .select("transaction_id")

df = df.join(
    duplicate_ids,
    on="transaction_id",
    how="left"
)

# Add validation status
validated_df = df.withColumn(
    "validation_status",
    when(missing_transaction_id, "MISSING_TRANSACTION_ID")
    .when(missing_account_id, "MISSING_ACCOUNT_ID")
    .when(invalid_amount, "INVALID_AMOUNT")
    .when(invalid_fraud_label, "INVALID_FRAUD_LABEL")
    .when(col("transaction_id").isin(
        [row["transaction_id"] for row in duplicate_ids.collect()]
    ), "DUPLICATE_TRANSACTION_ID")
    .otherwise("VALID")
)

print("\n===== VALIDATION RESULTS =====")
validated_df.show(truncate=False)

# Separate valid and invalid records
valid_df = validated_df.filter(
    col("validation_status") == "VALID"
)

invalid_df = validated_df.filter(
    col("validation_status") != "VALID"
)

# Display summary
print("\n===== DATA QUALITY SUMMARY =====")

print("Total records:", validated_df.count())
print("Valid records:", valid_df.count())
print("Invalid records:", invalid_df.count())

# Save invalid records to HDFS
invalid_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "hdfs://localhost:9000/fraud-monitoring/analytics/invalid_transactions"
    )

# Save valid records to HDFS
valid_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "hdfs://localhost:9000/fraud-monitoring/analytics/valid_transactions"
    )

print("\nValidation results successfully saved to HDFS.")

spark.stop()

print("\n===== DATA QUALITY VALIDATION COMPLETED =====")
