from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg

spark = SparkSession.builder \
    .appName("FraudMonitoringTest") \
    .master("local[*]") \
    .getOrCreate()

print("\n===== SPARK SESSION STARTED =====")


df = spark.read.csv(
   "hdfs://localhost:9000/fraud-monitoring/ground-truth/ground_truth.csv",
   header=True,
   inferSchema=True
)

print("\n===== GROUND TRUTH DATA =====")
df.show()

print("\n===== FRAUD STATISTICS =====")

df.groupBy("is_fraud").agg(
    count("*").alias("total_transactions")
).show()

print("\n===== TOTAL TRANSACTIONS =====")
print("Total:", df.count())

spark.stop()

print("\n===== SPARK TEST COMPLETED =====")
