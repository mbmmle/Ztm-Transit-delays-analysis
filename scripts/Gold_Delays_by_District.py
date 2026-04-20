# Auto-generated from notebook: work/Gold/Gold_Delays_by_District.ipynb

"""
Gold district-level delay aggregation job.

Reads recent rows from gold.bus_delays, computes per-minute district delay
metrics, applies weighted-delay logic, and writes a dense minute x district
result with explicit zero values for district-minute combinations without
recent delay events.
"""

import findspark
from config import db_properties,jdbc_url, SPARK_MEMORY, DB_CONFIG
findspark.init()
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

df_bus_delays_dist = spark.read.jdbc(
  url=jdbc_url,
  table="(SELECT * FROM gold.bus_delays WHERE time_gps >= NOW() - INTERVAL '10 minutes' AND delay_minutes <= 8) AS recent_bus_delays",
  properties=db_properties,
)

if df_bus_delays_dist.rdd.isEmpty():
        print("[Gold_Delays_by_District] No recent gold.bus_delays rows. Skipping aggregation.")
        spark.stop()
        raise SystemExit(99)

df_bus_delays_dist = add_weighted_delay_columns(df_bus_delays_dist)

df_bus_delays_dist_agg = df_bus_delays_dist.groupBy(F.window(F.col('time_gps'), "1 minute"),
                                      "district")\
                                    .agg(
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
                                    .orderBy("window_start", "district")

# Build dense district x minute grid so missing districts are filled with zero delay for visualization.
df_windows = build_recent_minute_windows(spark, minutes=10)

df_districts = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT DISTINCT district FROM silver.stops WHERE district IS NOT NULL) AS district_dim",
    properties=db_properties,
)

df_expected = df_windows.crossJoin(df_districts)

df_bus_delays_dist = (
    df_expected.alias("e")
    .join(
        df_bus_delays_dist_agg.alias("a"),
        on=["window_start", "window_end", "district"],
        how="left",
    )
    .select(
        F.col("window_start"),
        F.col("window_end"),
        F.col("district"),
        F.coalesce(F.col("a.average_delay_minutes"), F.lit(0.0)).alias("average_delay_minutes"),
        F.coalesce(F.col("a.average_delay_seconds"), F.lit(0.0)).alias("average_delay_seconds"),
        F.coalesce(F.col("a.max_delay_minutes"), F.lit(0.0)).alias("max_delay_minutes"),
        F.coalesce(F.col("a.min_delay_minutes"), F.lit(0.0)).alias("min_delay_minutes"),
        F.coalesce(F.col("a.Bus_on_stop_count"), F.lit(0)).alias("Bus_on_stop_count"),
        F.coalesce(F.col("a.average_weighted_delay_minutes"), F.lit(0.0)).alias("average_weighted_delay_minutes"),
        F.coalesce(F.col("a.average_weighted_delay_seconds"), F.lit(0.0)).alias("average_weighted_delay_seconds"),
    )
    .orderBy("window_start", "district")
)

row_count = df_bus_delays_dist.count()
print(f"[Gold_Delays_by_District] Aggregated rows for last 10 minutes: {row_count}")

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()
cur.execute(
"""
DO $$
BEGIN
  IF to_regclass('gold.delays_by_district') IS NOT NULL THEN
  DELETE FROM gold.delays_by_district
    WHERE window_start >= NOW() - INTERVAL '10 minutes';
    END IF;
END $$;
"""
)
conn.commit()
cur.close()
conn.close()

df_bus_delays_dist.write.jdbc(url=jdbc_url, table="gold.delays_by_district", mode="append", properties=db_properties)
print(f"[Gold_Delays_by_District] Rows written to delays_by_district: {row_count}")

with psycopg2.connect(**DB_CONFIG) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE OR REPLACE VIEW gold.v_delays_by_district AS
            SELECT 
                *,
                window_end::date AS date,
                window_end::time AS time
            FROM gold.delays_by_district;
        """)
    conn.commit()
spark.catalog.clearCache()
gc.collect()
spark.stop()

