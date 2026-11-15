from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    when,
    round,
    col
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("AccountLevelFraudAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n===== ACCOUNT LEVEL FRAUD ANALYSIS =====")

# Read transaction data from HDFS
df = spark.read.csv(
    "hdfs://localhost:9000/fraud-monitoring/transactions/transactions.csv",
    header=True,
    inferSchema=True
)

print("\n===== TRANSACTION DATA =====")
df.show()

# Group transactions by account
account_summary = df.groupBy("account_id").agg(
    count("*").alias("total_transactions"),
    sum("amount").alias("total_amount"),
    round(avg("amount"), 2).alias("average_amount"),
    sum(
        when(col("is_fraud") == 1, 1).otherwise(0)
    ).alias("fraud_count"),
    sum(
        when(col("is_fraud") == 1, col("amount")).otherwise(0)
    ).alias("fraud_amount")
)

# Calculate fraud percentage
account_summary = account_summary.withColumn(
    "fraud_percentage",
    round(
        col("fraud_count") / col("total_transactions") * 100,
        2
    )
)

print("\n===== ACCOUNT SUMMARY =====")
account_summary.orderBy(
    col("fraud_count").desc()
).show()

# Identify accounts with fraudulent transactions
fraud_accounts = account_summary.filter(
    col("fraud_count") > 0
)

print("\n===== ACCOUNTS WITH FRAUD =====")
fraud_accounts.show()

# Save complete account analysis to HDFS
account_summary.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "hdfs://localhost:9000/fraud-monitoring/analytics/account_summary"
    )

# Save accounts with fraud to HDFS
fraud_accounts.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(
        "hdfs://localhost:9000/fraud-monitoring/analytics/fraud_accounts"
    )

print("\nAccount analysis successfully saved to HDFS.")

spark.stop()

print("\n===== ACCOUNT ANALYSIS COMPLETED =====")
