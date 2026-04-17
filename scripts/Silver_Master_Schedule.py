# Auto-generated from notebook: work/Silver/Silver_Master_Schedule.ipynb

import findspark
findspark.init()
from config import db_properties,DB_CONFIG,jdbc_url, SPARK_MEMORY
from pyspark.sql import SparkSession
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
import psycopg2
from pyspark.sql import functions as F

df_stop_times_silver = spark.read.jdbc(url=jdbc_url, table="silver.stop_times", properties=db_properties)
df_trips_silver = spark.read.jdbc(url=jdbc_url, table="silver.trips", properties=db_properties)
df_routes_silver = spark.read.jdbc(url=jdbc_url, table="silver.routes", properties=db_properties)
df_stops_silver = spark.read.jdbc(url=jdbc_url, table="silver.stops", properties=db_properties)

df_master_schedule = df_stop_times_silver \
    .join(df_trips_silver,"trip_id","inner") 
df_master_schedule = df_master_schedule \
    .join(df_routes_silver,"route_id","inner") 
df_master_schedule = df_master_schedule \
    .join(df_stops_silver,"stop_id","inner")

df_master_schedule = df_master_schedule\
    .withColumn("schedule_id", F.md5(F.concat_ws("-",F.col("trip_id"),F.col("stop_sequence"),F.col("arrival_time"))))

df_master_schedule = df_master_schedule\
                    .select(F.col("schedule_id"),F.col("trip_id"),F.col("stop_id"),F.col("route_id"),
                            F.col("route_name"),F.col("stop_name"),F.col("district"),
                            F.col("stop_sequence"),F.col("stop_lat"),F.col("stop_lon"),
                            F.col("arrival_time"),F.col("departure_time")                            )

df_master_schedule.write.jdbc(url=jdbc_url, table="silver.master_schedule_staging", mode="overwrite", properties=db_properties)

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()

try:
    cur.execute("""
                DROP TABLE IF EXISTS silver.master_schedule_backup;
                ALTER TABLE IF EXISTS silver.master_schedule RENAME TO master_schedule_backup;
                ALTER TABLE IF EXISTS silver.master_schedule_staging RENAME TO master_schedule;""")
    conn.commit()
except Exception as e:
    conn.rollback()
    print(f"Error during table swap: {e}")
finally:
    cur.close()
    conn.close()

spark.catalog.clearCache()
gc.collect()
spark.stop()

