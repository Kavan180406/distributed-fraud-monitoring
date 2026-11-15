from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CreateRemoteFraudIcebergTable")
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
    .config(
        "spark.sql.catalog.remote",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.remote.type", "hadoop")
    .config(
        "spark.sql.catalog.remote.warehouse",
        "hdfs://10.103.77.92:9000/fraud-monitoring/iceberg-warehouse-remote"
    )
    .getOrCreate()
)

try:
    print("STEP 1: Reading original table")

    original = spark.table("demo.fraud.transactions")
    original.show(truncate=False)

    print("Original count:", original.count())

    print("STEP 2: Creating remote namespace")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS remote.fraud")

    print("STEP 3: Creating remote table")

    spark.sql("""
        CREATE TABLE remote.fraud.transactions
        USING iceberg
        AS SELECT * FROM demo.fraud.transactions
    """)

    print("STEP 4: Verifying remote table")

    result = spark.table("remote.fraud.transactions")

    result.show(truncate=False)

    print("Total transactions:", result.count())
    print(
        "Fraudulent transactions:",
        result.filter("is_fraud = 1").count()
    )

    print("STEP 5: Checking table location")

    spark.sql(
        "DESCRIBE EXTENDED remote.fraud.transactions"
    ).show(100, truncate=False)

    print("REMOTE TABLE CREATION COMPLETED")

finally:
    spark.stop()

