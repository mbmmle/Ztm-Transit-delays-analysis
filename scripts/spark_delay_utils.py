"""
Reusable Spark utilities for delay aggregation jobs.

Functions in this module are used by Gold aggregation scripts to keep logic
consistent between stop-level and district-level calculations.

- add_weighted_delay_columns: applies lower weight to early-stop delays.
- build_recent_minute_windows: creates a dense minute timeline for the recent
    window so missing minutes can still be represented in output tables.
"""

from pyspark.sql import functions as F


def add_weighted_delay_columns(df, early_stop_threshold=3, early_weight=0.5):
    return (
        df.withColumn(
            "weighted_delay_minutes",
            F.when(F.col("stop_sequence") <= early_stop_threshold, F.col("delay_minutes") * early_weight)
            .otherwise(F.col("delay_minutes")),
        )
        .withColumn(
            "weighted_delay_seconds",
            F.when(F.col("stop_sequence") <= early_stop_threshold, F.col("delay_seconds") * early_weight)
            .otherwise(F.col("delay_seconds")),
        )
    )


def build_recent_minute_windows(spark, minutes=10):
    start_offset_minutes = max(int(minutes) - 1, 0)
    return spark.sql(
        f"""
        SELECT
            explode(
                sequence(
                    date_trunc('minute', current_timestamp() - interval {start_offset_minutes} minutes),
                    date_trunc('minute', current_timestamp()),
                    interval 1 minute
                )
            ) AS window_start
        """
    ).withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 1 minute"))
