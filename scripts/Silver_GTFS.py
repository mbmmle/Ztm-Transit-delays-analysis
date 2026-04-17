# Auto-generated from notebook: work/Silver/Silver_GTFS.ipynb

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
from pathlib import Path
sys.path.append('../work')
from config import db_properties,DB_CONFIG,jdbc_url
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
from pyspark.sql.types import FloatType, IntegerType, BooleanType
import psycopg2
from pyspark.sql import functions as F
import geopandas as gpd
from shapely.geometry import Point

BASE_DIR = Path(__file__).resolve().parent.parent
DISTRICTS_GEOJSON = BASE_DIR / "Silver" / "warszawa-dzielnice.geojson"

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
conn.commit()
conn.close()

df_stops_bronze = spark.read.jdbc(url=jdbc_url, table="bronze.stops", properties=db_properties)

df_stops_silver = df_stops_bronze.select(
    "stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"
)\
                .withColumn("stop_id",F.col("stop_id").cast(IntegerType()))\
                .withColumn("stop_lat",F.col("stop_lat").cast(FloatType()))\
                .withColumn("stop_lon",F.col("stop_lon").cast(FloatType()))

pdf_stops = df_stops_silver.select("stop_id", "stop_lat", "stop_lon").toPandas()
geometry = [Point(xy) for xy in zip(pdf_stops.stop_lon, pdf_stops.stop_lat)]
gdf_stops = gpd.GeoDataFrame(pdf_stops, geometry=geometry, crs="EPSG:4326")
gdf_districts = gpd.read_file(DISTRICTS_GEOJSON)
gdf_stops_with_districts = gpd.sjoin(gdf_stops, gdf_districts, how="left", predicate="within")
gdf_stops_with_districts['name'] = gdf_stops_with_districts['name'].fillna("Poza Warszawa")

df_districts = spark.createDataFrame(gdf_stops_with_districts[["stop_id", "name"]].rename(columns={"name": "district"}))
df_stops_silver = df_stops_silver.join(df_districts, on="stop_id", how="left")
df_stops_silver = df_stops_silver.where(F.col("district")!="Warszawa")

df_stops_silver.write.jdbc(url=jdbc_url, table="silver.stops", mode="overwrite", properties=db_properties)

df_routes_bronze = spark.read.jdbc(url=jdbc_url, table="bronze.routes", properties=db_properties)

df_routes_bronze = df_routes_bronze.drop("route_color","route_text_color")

df_routes_silver = df_routes_bronze\
                .withColumn("agency_id",F.col("agency_id").cast(IntegerType()))\
                .withColumn("route_type",F.col("route_type").cast(IntegerType()))\
                .withColumnRenamed("route_short_name", "route_name")\
                .withColumnRenamed("route_long_name", "route_desc")
df_routes_silver = df_routes_silver.where(F.col("route_type")==3)

df_routes_silver.write.jdbc(url=jdbc_url, table="silver.routes", mode="overwrite", properties=db_properties)

df_trips_bronze = spark.read.jdbc(url=jdbc_url, table="bronze.trips", properties=db_properties)

df_trips_bronze = df_trips_bronze.drop(
    "shape_id", "trip_short_name", "exceptional", "wheelchair_accessible",
    "block_id","block_short_name", "variant_code", "fleet_type"
)

df_trips_silver = df_trips_bronze\
                .withColumn("direction_id",F.col("direction_id").cast(IntegerType())) \

df_trips_silver

df_trips_silver.write.jdbc(url=jdbc_url, table="silver.trips", mode="overwrite", properties=db_properties)

df_stop_times_bronze = spark.read.jdbc(url=jdbc_url, table="bronze.stop_times", properties=db_properties)
df_stop_times_bronze

df_stop_times_bronze = df_stop_times_bronze\
                .withColumn("stop_sequence",F.col("stop_sequence").cast(IntegerType()))\
                .withColumn("pickup_type",F.col("pickup_type").cast(IntegerType()))\
                .withColumn("drop_off_type",F.col("drop_off_type").cast(IntegerType()))\
                .withColumn("stop_id",F.col("stop_id").cast(IntegerType()))\
                .withColumn("shape_dist_traveled",F.col("shape_dist_traveled").cast(FloatType()))
df_stop_times_bronze


df_stop_times_joined = df_stop_times_bronze.join(df_trips_silver, on="trip_id", how="inner")\
                


df_stop_times_silver = df_stop_times_joined \
    .withColumn(
        "op_date",
        F.coalesce(
            F.to_date(F.regexp_extract(F.col("service_id"), r"^(\\d{4}-\\d{2}-\\d{2})", 1), "yyyy-MM-dd"),
            F.current_date()
        )
    ) \
    .withColumn("arrival_sec", F.col("arrival_time").substr(1, 2).cast(IntegerType()) * 3600 + \
                                F.col("arrival_time").substr(4, 2).cast(IntegerType()) * 60 + \
                                F.col("arrival_time").substr(7, 2).cast(IntegerType())) \
    .withColumn("departure_sec", F.col("departure_time").substr(1, 2).cast(IntegerType()) * 3600 + \
                                F.col("departure_time").substr(4, 2).cast(IntegerType()) * 60 + \
                                F.col("departure_time").substr(7, 2).cast(IntegerType())) \
    .withColumn("parsed_arrival_date",
                F.expr("cast(to_timestamp(op_date) + make_interval(0, 0, 0, 0, 0, 0, arrival_sec) as timestamp)")) \
    .withColumn("parsed_departure_date",
                F.expr("cast(to_timestamp(op_date) + make_interval(0, 0, 0, 0, 0, 0, departure_sec) as timestamp)"))

df_stop_times_silver = df_stop_times_silver.select(
    "trip_id", "stop_id", "stop_sequence", "pickup_type", 
    "drop_off_type", "shape_dist_traveled",
    F.col("parsed_arrival_date").alias("arrival_time"), F.col("parsed_departure_date").alias("departure_time")
)
df_stop_times_silver

df_stop_times_silver.repartition(16).write.jdbc(
    url=jdbc_url,
    table="silver.stop_times",
    mode="overwrite",
    properties=db_properties
)

spark.catalog.clearCache()
gc.collect()
spark.stop()

