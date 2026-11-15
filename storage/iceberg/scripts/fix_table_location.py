from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FixFraudIcebergTableLocation")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.demo",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config(
        "spark.sql.catalog.demo.warehouse",
        "hdfs://localhost:9000/fraud-monitoring/iceberg-warehouse"
    )
    .getOrCreate()
)

try:
    print("\n===== STEP 1: READING ORIGINAL TABLE =====")

    original = spark.table("demo.fraud.transactions")

    original.show(truncate=False)

    print("Original transaction count:", original.count())

    print("\n===== STEP 2: CHECKING NEW TABLE =====")

    tables = spark.sql("SHOW TABLES IN demo.fraud").collect()

    if any(row.tableName == "transactions_vm4" for row in tables):
        raise Exception(
            "transactions_vm4 already exists. "
            "Stopping to protect existing data."
        )

    print("\n===== STEP 3: CREATING NEW TABLE =====")

    # Create the table using the catalog's configured warehouse.
    # The catalog will generate its location using localhost.
    spark.sql("""
        CREATE TABLE demo.fraud.transactions_vm4
        USING iceberg
        AS SELECT * FROM demo.fraud.transactions
    """)

    print("\n===== STEP 4: VERIFYING NEW TABLE =====")

    corrected = spark.table("demo.fraud.transactions_vm4")

    corrected.show(truncate=False)

    print("Total transactions:", corrected.count())

    print(
        "Fraudulent transactions:",
        corrected.filter("is_fraud = 1").count()
    )

    print(
        "Genuine transactions:",
        corrected.filter("is_fraud = 0").count()
    )

    print("\n===== STEP 5: TABLE LOCATION =====")

    spark.sql("""
        DESCRIBE EXTENDED demo.fraud.transactions_vm4
    """).show(100, truncate=False)

    print("\n===== TABLE COPY COMPLETED =====")

finally:
    spark.stop()
