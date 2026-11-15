from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    col,
    round as spark_round
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("FraudAnalytics") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n===== FRAUD ANALYTICS SYSTEM =====")

# Read data directly from HDFS
df = spark.read.csv(
    "hdfs://localhost:9000/fraud-monitoring/ground-truth/ground_truth.csv",
    header=True,
    inferSchema=True
)

# Display transactions
print("\n===== TRANSACTION DATA =====")
df.show()

# Total transactions
total = df.count()

# Fraudulent transactions
fraud = df.filter(col("is_fraud") == 1).count()

# Legitimate transactions
legitimate = df.filter(col("is_fraud") == 0).count()

# Fraud percentage
fraud_percentage = (fraud / total) * 100 if total > 0 else 0

print("\n===== FRAUD SUMMARY =====")
print("Total Transactions:", total)
print("Fraudulent Transactions:", fraud)
print("Legitimate Transactions:", legitimate)
print("Fraud Percentage:", round(fraud_percentage, 2), "%")

# Group transactions
summary = df.groupBy("is_fraud").agg(
    count("*").alias("transaction_count")
)

print("\n===== FRAUD DISTRIBUTION =====")
summary.show()

# Save results to HDFS
summary.write.mode("overwrite").csv(
    "hdfs://localhost:9000/fraud-monitoring/analytics/fraud_summary",
    header=True
)

print("\nAnalytics results successfully saved to HDFS.")

spark.stop()

print("\n===== ANALYTICS COMPLETED =====")
