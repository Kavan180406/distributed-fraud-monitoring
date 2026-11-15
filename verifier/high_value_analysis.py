from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    round
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("HighValueFraudAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n===== HIGH VALUE FRAUD ANALYSIS =====")

# Read transaction data from HDFS
df = spark.read.csv(
    "hdfs://localhost:9000/fraud-monitoring/transactions/transactions.csv",
    header=True,
    inferSchema=True
)

# Display transaction data
print("\n===== TRANSACTION RECORDS =====")
df.show()

# Calculate overall statistics
print("\n===== TRANSACTION STATISTICS =====")

df.select(
    count("*").alias("total_transactions"),
    round(avg("amount"), 2).alias("average_amount"),
    sum("amount").alias("total_amount"),
    max("amount").alias("highest_transaction")
).show()

# Identify fraudulent transactions
fraud_df = df.filter(col("is_fraud") == 1)

print("\n===== FRAUDULENT TRANSACTIONS =====")
fraud_df.show()

# Calculate fraud statistics
print("\n===== FRAUD AMOUNT ANALYSIS =====")

fraud_df.select(
    count("*").alias("fraud_count"),
    sum("amount").alias("total_fraud_amount"),
    round(avg("amount"), 2).alias("average_fraud_amount"),
    max("amount").alias("highest_fraud_amount")
).show()

# Identify high-value transactions above 10000
print("\n===== HIGH VALUE TRANSACTIONS (ABOVE 10000) =====")

high_value_df = df.filter(col("amount") > 10000)

high_value_df.show()

# Save high-value fraud results to HDFS
fraud_df.write.mode("overwrite").csv(
    "hdfs://localhost:9000/fraud-monitoring/analytics/fraudulent_transactions",
    header=True
)

high_value_df.write.mode("overwrite").csv(
    "hdfs://localhost:9000/fraud-monitoring/analytics/high_value_transactions",
    header=True
)

print("\nResults successfully saved to HDFS.")

spark.stop()

print("\n===== ANALYSIS COMPLETED =====")
