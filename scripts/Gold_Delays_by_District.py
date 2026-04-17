# Auto-generated from notebook: work/Gold/Gold_Delays_by_District.ipynb

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
api_key = os.getenv("WARSAW_API_KEY")
from pyspark.sql import functions as F

df_bus_delays_dist = spark.read.jdbc(
  url=jdbc_url,
  table="(SELECT * FROM gold.bus_delays WHERE time_gps >= NOW() - INTERVAL '15 minutes') AS recent_bus_delays",
  properties=db_properties,
)

df_bus_delays_dist = df_bus_delays_dist.withColumn("weighted_delay_minutes",F.when(F.col("stop_sequence")<=3 
                                                    ,F.col("delay_minutes")*0.5)
                                                    .otherwise(F.col("delay_minutes"))) \
                            .withColumn("weighted_delay_seconds",F.when(F.col("stop_sequence")<=3
                                                    ,F.col("delay_seconds")*0.5)
                                                    .otherwise(F.col("delay_seconds")))  

df_bus_delays_dist = df_bus_delays_dist.groupBy(F.window(F.col('time_gps'), "1 minute"),
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
                                    .withColumn("window_start", F.col("window.start"))\
                                    .withColumn("window_end", F.col("window.end"))\
                                    .drop("window")\
                                    .orderBy("window_start", "district")

row_count = df_bus_delays_dist.count()
print(f"[Gold_Delays_by_District] Aggregated rows for last 15 minutes: {row_count}")

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()
cur.execute(
"""
DO $$
BEGIN
    IF to_regclass('public.delays_by_district') IS NOT NULL THEN
    DELETE FROM public.delays_by_district
    WHERE window_start >= NOW() - INTERVAL '15 minutes';
    END IF;
END $$;
"""
)
conn.commit()
cur.close()
conn.close()

df_bus_delays_dist.write.jdbc(url=jdbc_url, table="gold.delays_by_district", mode="append", properties=db_properties)
print(f"[Gold_Delays_by_District] Rows written to delays_by_district: {row_count}")

spark.catalog.clearCache()
gc.collect()
spark.stop()

