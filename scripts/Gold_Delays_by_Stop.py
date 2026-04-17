# Auto-generated from notebook: work/Gold/Gold_Delays_by_Stop.ipynb

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Warsaw_Bus_Project") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

import sys
import gc
sys.path.append('../work')
from config import db_properties,jdbc_url
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from pyspark.sql import functions as F

df_bus_delays = spark.read.jdbc(url=jdbc_url, table="gold.bus_delays", properties=db_properties)

df_bus_delays = df_bus_delays.withColumn("weighted_delay_minutes",F.when(F.col("stop_sequence")<=3 
                                                    ,F.col("delay_minutes")*0.5)
                                                    .otherwise(F.col("delay_minutes"))) \
                            .withColumn("weighted_delay_seconds",F.when(F.col("stop_sequence")<=3
                                                    ,F.col("delay_seconds")*0.5)
                                                    .otherwise(F.col("delay_seconds")))  

df_bus_delays = df_bus_delays.groupBy(F.window(F.col('time_gps'), "1 minute"),
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
                                    .withColumn("window_start", F.col("window.start"))\
                                    .withColumn("window_end", F.col("window.end"))\
                                    .drop("window")\
                                    .orderBy("window_start", "stop_name")

df_bus_delays.write.jdbc(url=jdbc_url, table="delays_by_stop", mode="overwrite", properties=db_properties)

spark.catalog.clearCache()
gc.collect()
spark.stop()

