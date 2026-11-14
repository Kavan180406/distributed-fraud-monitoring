from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def evaluate_batch(transactions: DataFrame):
    """
    Batch/reference fraud-rule engine.

    Required columns:
      event_id, transaction_id, source_account, destination_account,
      event_timestamp, amount, currency

    Returns:
      decisions: one row per unique transaction ID
      alerts: REVIEW and BLOCK decisions only

    R2-R4 use 2-minute event-time windows sliding every 30 seconds.
    """

    required = {
        "event_id",
        "transaction_id",
        "source_account",
        "destination_account",
        "event_timestamp",
        "amount",
        "currency",
    }

    missing = required.difference(transactions.columns)
    if missing:
        raise ValueError(
            "Missing columns: " + ", ".join(sorted(missing))
        )

    normalized = (
        transactions
        .withColumn("event_time", F.to_timestamp("event_timestamp"))
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .filter(
            F.col("transaction_id").isNotNull()
            & F.col("source_account").isNotNull()
            & F.col("destination_account").isNotNull()
            & F.col("event_time").isNotNull()
            & F.col("amount").isNotNull()
        )
    )

    # Deduplicate before evaluating any fraud rule.
    dedup_window = Window.partitionBy("transaction_id").orderBy(
        F.col("event_time").asc(),
        F.col("event_id").asc_nulls_last(),
    )

    unique_transactions = (
        normalized
        .withColumn("_rank", F.row_number().over(dedup_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    # R1: One INR transaction >= Rs. 50,000.
    transaction_windows = (
        unique_transactions
        .withColumn(
            "R1",
            (F.col("currency") == "INR")
            & (F.col("amount") >= F.lit(50000)),
        )
        .withColumn(
            "_window",
            F.window("event_time", "2 minutes", "30 seconds"),
        )
    )

    # R2: At least 5 transactions from the same account.
    # R3: At least 2 INR transactions totalling more than Rs. 6,00,000.
    account_metrics = (
        transaction_windows
        .groupBy("source_account", "_window")
        .agg(
            F.count("*").alias("_account_count"),
            F.sum(
                F.when(
                    F.col("currency") == "INR",
                    F.col("amount"),
                ).otherwise(F.lit(0).cast("decimal(18,2)"))
            ).alias("_inr_total"),
            F.sum(
                F.when(F.col("currency") == "INR", 1).otherwise(0)
            ).alias("_inr_count"),
        )
        .withColumn("_R2", F.col("_account_count") >= 5)
        .withColumn(
            "_R3",
            (F.col("_inr_count") >= 2)
            & (F.col("_inr_total") > F.lit(600000)),
        )
        .select("source_account", "_window", "_R2", "_R3")
    )

    # R4: At least 3 transactions from the same account
    # to the same destination.
    destination_metrics = (
        transaction_windows
        .groupBy(
            "source_account",
            "destination_account",
            "_window",
        )
        .agg(F.count("*").alias("_destination_count"))
        .withColumn("_R4", F.col("_destination_count") >= 3)
        .select(
            "source_account",
            "destination_account",
            "_window",
            "_R4",
        )
    )

    per_window = (
        transaction_windows
        .join(account_metrics, ["source_account", "_window"], "left")
        .join(
            destination_metrics,
            ["source_account", "destination_account", "_window"],
            "left",
        )
        .withColumn("R2", F.coalesce(F.col("_R2"), F.lit(False)))
        .withColumn("R3", F.coalesce(F.col("_R3"), F.lit(False)))
        .withColumn("R4", F.coalesce(F.col("_R4"), F.lit(False)))
    )

    # A transaction can belong to multiple overlapping windows.
    # A rule is triggered if any of those windows meets its threshold.
    decisions = (
        per_window
        .groupBy("transaction_id")
        .agg(
            F.first("event_id", ignorenulls=True).alias("event_id"),
            F.first("source_account").alias("source_account"),
            F.first("destination_account").alias("destination_account"),
            F.first("event_time").alias("event_time"),
            F.first("amount").alias("amount"),
            F.first("currency").alias("currency"),
            F.max(F.col("R1").cast("int")).alias("_r1"),
            F.max(F.col("R2").cast("int")).alias("_r2"),
            F.max(F.col("R3").cast("int")).alias("_r3"),
            F.max(F.col("R4").cast("int")).alias("_r4"),
        )
        .withColumn("R1", F.col("_r1") == 1)
        .withColumn("R2", F.col("_r2") == 1)
        .withColumn("R3", F.col("_r3") == 1)
        .withColumn("R4", F.col("_r4") == 1)
        .withColumn(
            "rule_count",
            F.col("_r1") + F.col("_r2") + F.col("_r3") + F.col("_r4"),
        )
        .withColumn(
            "triggered_rules",
            F.concat_ws(
                ",",
                F.when(F.col("R1"), "R1"),
                F.when(F.col("R2"), "R2"),
                F.when(F.col("R3"), "R3"),
                F.when(F.col("R4"), "R4"),
            ),
        )
        .withColumn(
            "decision",
            F.when(F.col("rule_count") >= 2, "BLOCK")
            .when(F.col("rule_count") == 1, "REVIEW")
            .otherwise("APPROVE"),
        )
        .drop("_r1", "_r2", "_r3", "_r4")
    )

    alerts = decisions.filter(
        F.col("decision").isin("REVIEW", "BLOCK")
    )

    return decisions, alerts
