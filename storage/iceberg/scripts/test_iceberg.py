from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FraudMonitoring-Iceberg-Test")
    .getOrCreate()
)

print("\n===== ICEBERG INTEGRATION TEST =====")

# Create an Iceberg namespace
spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS demo.fraud
""")

# Create an Iceberg table
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.fraud.transactions (
        transaction_id STRING,
        account_id STRING,
        amount DOUBLE,
        is_fraud INT
    )
    USING iceberg
""")

# Insert sample transactions
spark.sql("""
    INSERT INTO demo.fraud.transactions VALUES
    ('TXN001', 'ACC101', 5000, 0),
    ('TXN002', 'ACC102', 95000, 1),
    ('TXN003', 'ACC103', 1200, 0)
""")

# Display the stored records
print("\n===== ICEBERG TABLE DATA =====")

spark.sql("""
    SELECT * FROM demo.fraud.transactions
""").show()

# Display fraud summary
print("\n===== FRAUD SUMMARY =====")

spark.sql("""
    SELECT is_fraud, COUNT(*) AS transaction_count
    FROM demo.fraud.transactions
    GROUP BY is_fraud
""").show()

print("\n===== ICEBERG TEST COMPLETED =====")

spark.stop()
