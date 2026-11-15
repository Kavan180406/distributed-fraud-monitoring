from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VM3-Cluster-Test")
    .master("spark://10.103.77.41:7077")
    .config("spark.driver.host", "10.103.77.92")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .getOrCreate()
)

print("===== SPARK CLUSTER TEST =====")

data = [(1, "TXN001", 5000), (2, "TXN002", 95000), (3, "TXN003", 1200)]

df = spark.createDataFrame(data, ["id", "transaction_id", "amount"])

df.show()

print("Total transactions:", df.count())
print("Total amount:", df.agg({"amount": "sum"}).collect()[0][0])

print("Spark Master:", spark.sparkContext.master)

spark.stop()
