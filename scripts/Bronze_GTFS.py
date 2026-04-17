# Auto-generated from notebook: work/Bronze/Bronze_GTFS.ipynb

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Warsaw_Bus_Project") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

import sys
import gc
sys.path.append('../work')
from config import db_properties,DB_CONFIG,jdbc_url
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
api_key = os.getenv("WARSAW_API_KEY")
import psycopg2
import zipfile
import urllib.request
from pathlib import Path

try:
    gtfs_url = "https://mkuran.pl/gtfs/warsaw.zip" 
    zip_path = "/tmp/warsaw.zip"
    extract_path = "/tmp/gtfs_data"
    urllib.request.urlretrieve(gtfs_url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print("GTFS data downloaded and extracted successfully")
except Exception as e:
    sys.exit(1)

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
conn.commit()
conn.close()

df_stop_times = spark.read.csv(f"{extract_path}/stop_times.txt", header=True)

df_stop_times.write.jdbc(url=jdbc_url, table="bronze.stop_times", \
                         mode="overwrite", properties=db_properties)



df_stops = spark.read.csv(f"{extract_path}/stops.txt", header=True)

df_stops.write.jdbc(url=jdbc_url, table="bronze.stops", \
                         mode="overwrite", properties=db_properties)



df_routes = spark.read.csv(f"{extract_path}/routes.txt", header=True)

df_routes.write.jdbc(url=jdbc_url, table="bronze.routes", \
                         mode="overwrite", properties=db_properties)





df_trips = spark.read.csv(f"{extract_path}/trips.txt", header=True)

df_trips.write.jdbc(url=jdbc_url, table="bronze.trips", \
                         mode="overwrite", properties=db_properties)


shapes_path = Path(extract_path) / "shapes.txt"
if shapes_path.exists():
    df_shapes = spark.read.csv(str(shapes_path), header=True)
    df_shapes.write.jdbc(
        url=jdbc_url,
        table="bronze.shapes",
        mode="overwrite",
        properties=db_properties,
    )

calendar_path = Path(extract_path) / "calendar.txt"
if calendar_path.exists():
    df_calendar = spark.read.csv(str(calendar_path), header=True)
    df_calendar.write.jdbc(
        url=jdbc_url,
        table="bronze.calendar",
        mode="overwrite",
        properties=db_properties,
    )




spark.catalog.clearCache()
gc.collect()
spark.stop()

