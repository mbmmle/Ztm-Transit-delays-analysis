# Auto-generated from notebook: work/Silver/Silver_Bus_Live_Feed.ipynb

import os
import sys
api_key = os.getenv("WARSAW_API_KEY")
import requests
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG
import hashlib
from google.transit import gtfs_realtime_pb2

url = "https://mkuran.pl/gtfs/warsaw/vehicles.pb"
engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    dane_autobusow = [
        (
            e.vehicle.vehicle.id,
            e.vehicle.trip.trip_id,
            e.vehicle.trip.route_id,
            e.vehicle.position.latitude,
            e.vehicle.position.longitude,
            e.vehicle.timestamp
        )
        for e in feed.entity
        if e.HasField('vehicle') and e.vehicle.trip.trip_id
    ]
    df_rt = pd.DataFrame(dane_autobusow, columns=[
        "vehicle_number", "trip_id", "route_id", "lat", "lon", "time_gps_raw"
    ])
except Exception as e:
    print(f"Error: {e}")
if df_rt.empty:
    sys.exit(99)

df_rt['time_gps'] = pd.to_datetime(df_rt['time_gps_raw'], unit='s')
df_rt = df_rt.drop(columns=['time_gps_raw', 'route_id'])
df_rt['etl_timestamp'] = pd.Timestamp.now()

hash_input = df_rt['vehicle_number'].astype(str) + '_' + df_rt['time_gps'].astype(str)
df_rt['gps_id'] = [hashlib.md5(x.encode('utf-8')).hexdigest() for x in hash_input]

df_rt = df_rt[['gps_id', 'trip_id', 'lat', 'lon', 'vehicle_number', 'time_gps', 'etl_timestamp']]

df_rt.to_sql('bus_live_feed', engine,schema='silver', if_exists='append', index=False)

