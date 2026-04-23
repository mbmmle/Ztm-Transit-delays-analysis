# Auto-generated from notebook: work/Gold/Gold_Delays_by_Stop.ipynb

"""
Gold stop-level delay aggregation job.

Reads recent rows from gold.bus_delays, computes per-minute stop aggregates,
adds weighted-delay metrics, and materializes a dense minute x stop output so
consumer dashboards can render zero-delay states for stops without recent
observations.
"""

import findspark
findspark.init()
from config import db_properties,jdbc_url, SPARK_MEMORY, DB_CONFIG
from pyspark.sql import SparkSession
import psycopg2
spark = SparkSession.builder \
    .appName("Warsaw_Bus_Project") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .config("spark.driver.memory", SPARK_MEMORY) \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

import sys
import gc
sys.path.append('../work')

import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from pyspark.sql import functions as F
from spark_delay_utils import add_weighted_delay_columns, build_recent_minute_windows

df_bus_delays = spark.read.jdbc(
  url=jdbc_url,
  table="(SELECT * FROM gold.bus_delays WHERE time_gps >= NOW() - INTERVAL '10 minutes' AND delay_minutes <= 8) AS recent_bus_delays",
  properties=db_properties,
)

if df_bus_delays.rdd.isEmpty():
        print("[Gold_Delays_by_Stop] No recent gold.bus_delays rows. Skipping aggregation.")
        spark.stop()
        raise SystemExit(99)

df_bus_delays = add_weighted_delay_columns(df_bus_delays)

df_bus_delays_agg = df_bus_delays.groupBy(F.window(F.col('time_gps'), "1 minute"),
                                      "stop_id")\
                                    .agg(
                                    F.first("stop_name").alias("stop_name"),
                                    F.first("stop_lat").alias("stop_lat"),
                                    F.first("stop_lon").alias("stop_lon"),
                                    F.first("district").alias("district"),
                                    F.avg("delay_minutes").alias("average_delay_minutes"),
                                    F.avg("delay_seconds").alias("average_delay_seconds"),
                                    F.max("delay_minutes").alias("max_delay_minutes"),
                                    F.min("delay_minutes").alias("min_delay_minutes"),
                                    F.count("stop_id").alias("Bus_on_stop_count"),
                                    F.avg("weighted_delay_minutes").alias("average_weighted_delay_minutes"),
                                    F.avg("weighted_delay_seconds").alias("average_weighted_delay_seconds")
                                    )\
                                                                        .withColumn("average_delay_minutes", F.round(F.col("average_delay_minutes"), 2))\
                                                                        .withColumn("average_delay_seconds", F.round(F.col("average_delay_seconds"), 2))\
                                                                        .withColumn("max_delay_minutes", F.round(F.col("max_delay_minutes"), 2))\
                                                                        .withColumn("min_delay_minutes", F.round(F.col("min_delay_minutes"), 2))\
                                                                        .withColumn("average_weighted_delay_minutes", F.round(F.col("average_weighted_delay_minutes"), 2))\
                                                                        .withColumn("average_weighted_delay_seconds", F.round(F.col("average_weighted_delay_seconds"), 2))\
                                    .withColumn("window_start", F.col("window.start"))\
                                    .withColumn("window_end", F.col("window.end"))\
                                    .drop("window")\
                                    .orderBy("window_start", "stop_name")

# Build dense stop x minute grid so missing stops are filled with zero delay for visualization.
df_windows = build_recent_minute_windows(spark, minutes=10)

df_stops = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT stop_id, stop_name, stop_lat, stop_lon, district FROM silver.stops) AS stops_dim",
    properties=db_properties,
)

df_expected = df_windows.crossJoin(df_stops)

df_bus_delays = (
    df_expected.alias("e")
    .join(
        df_bus_delays_agg.alias("a"),
        on=["window_start", "window_end", "stop_id"],
        how="left",
    )
    .select(
        F.col("window_start"),
        F.col("window_end"),
        F.col("stop_id"),
        F.coalesce(F.col("a.stop_name"), F.col("e.stop_name")).alias("stop_name"),
        F.coalesce(F.col("a.stop_lat"), F.col("e.stop_lat")).alias("stop_lat"),
        F.coalesce(F.col("a.stop_lon"), F.col("e.stop_lon")).alias("stop_lon"),
        F.coalesce(F.col("a.district"), F.col("e.district")).alias("district"),
        F.coalesce(F.col("a.average_delay_minutes"), F.lit(0.0)).alias("average_delay_minutes"),
        F.coalesce(F.col("a.average_delay_seconds"), F.lit(0.0)).alias("average_delay_seconds"),
        F.coalesce(F.col("a.max_delay_minutes"), F.lit(0.0)).alias("max_delay_minutes"),
        F.coalesce(F.col("a.min_delay_minutes"), F.lit(0.0)).alias("min_delay_minutes"),
        F.coalesce(F.col("a.Bus_on_stop_count"), F.lit(0)).alias("Bus_on_stop_count"),
        F.coalesce(F.col("a.average_weighted_delay_minutes"), F.lit(0.0)).alias("average_weighted_delay_minutes"),
        F.coalesce(F.col("a.average_weighted_delay_seconds"), F.lit(0.0)).alias("average_weighted_delay_seconds"),
    )
    .orderBy("window_start", "stop_name")
)

row_count = df_bus_delays.count()
print(f"[Gold_Delays_by_Stop] Aggregated rows for last 10 minutes: {row_count}")

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()
cur.execute(
    """
    DO $$
    BEGIN
        IF to_regclass('gold.delays_by_stop') IS NOT NULL THEN
            DELETE FROM gold.delays_by_stop
            WHERE window_start >= NOW() - INTERVAL '15 minutes';
        END IF;
    END $$;
    """
)
conn.commit()
cur.close()
conn.close()

df_bus_delays.write.jdbc(url=jdbc_url, table="gold.delays_by_stop", mode="append", properties=db_properties)
print(f"[Gold_Delays_by_Stop] Rows written to delays_by_stop: {row_count}")

with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE VIEW gold.v_delays_by_stop AS
            SELECT 
                *,
                window_end::date AS date,
                EXTRACT(HOUR FROM window_end) AS time
            FROM gold.delays_by_stop;
        """)
    conn.commit()
spark.catalog.clearCache()
gc.collect()
spark.stop()

