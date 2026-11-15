from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VM3-VM4-Integration-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

table = "demo.fraud.transactions"
test_id = "TXN_TEST_VM3_001"

try:
    print("\n===== VM3 ICEBERG INSERT TEST =====")

    # Avoid inserting the same test record twice
    existing = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM {table}
        WHERE transaction_id = '{test_id}'
    """).first()["total"]

    if existing == 0:
        spark.sql(f"""
            INSERT INTO {table}
            VALUES ('{test_id}', 'ACC_TEST_001', 7777.0, 0)
        """)
        print("Test transaction inserted successfully!")
    else:
        print("Test transaction already exists. No duplicate inserted.")

    print("\n===== UPDATED ICEBERG TABLE =====")

    spark.sql(f"""
        SELECT *
        FROM {table}
        ORDER BY transaction_id
    """).show(truncate=False)

    print("Total transactions:", spark.table(table).count())

finally:
    spark.stop()
