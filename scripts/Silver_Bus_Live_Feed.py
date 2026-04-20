# Auto-generated from notebook: work/Silver/Silver_Bus_Live_Feed.ipynb

"""
Silver live feed ingestion for GTFS-RT vehicle positions.

The script fetches real-time vehicle positions, validates and decodes protobuf
payloads, normalizes core fields used by matching, converts timestamps to local
UTC wall time, and appends snapshots to silver.bus_live_feed.

This table is the operational input for bus-to-schedule matching.
"""

import sys
import requests
import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG
import hashlib
from google.transit import gtfs_realtime_pb2

# Algorithm overview:
# 1) Fetch and decode GTFS-RT vehicle positions from mkuran feed.
# 2) Build a normalized dataframe with vehicle id, trip id, coordinates and timestamp.
# 3) Convert UTC epoch to UTC wall-clock used consistently across pipeline layers.
# 4) Build deterministic gps_id (vehicle_number + time_gps hash) and append to silver.bus_live_feed.

url = "https://mkuran.pl/gtfs/warsaw/vehicles.pb"
engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")

def fetch_realtime_feed(feed_url):
    try:
        response = requests.get(feed_url, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        print(f"[Silver_Bus_Live_Feed] Feed request failed: {exc}")
        sys.exit(99)

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except Exception as exc:
        print(f"[Silver_Bus_Live_Feed] Feed decode failed: {exc}")
        sys.exit(99)

    records = [
        (
            entity.vehicle.vehicle.id,
            entity.vehicle.trip.trip_id,
            entity.vehicle.trip.route_id,
            entity.vehicle.position.latitude,
            entity.vehicle.position.longitude,
            entity.vehicle.timestamp,
        )
        for entity in feed.entity
        if entity.HasField("vehicle") and entity.vehicle.trip.trip_id
    ]
    return pd.DataFrame(
        records,
        columns=["vehicle_number", "trip_id", "route_id", "lat", "lon", "time_gps_raw"],
    )


df_rt = fetch_realtime_feed(url)

if df_rt.empty:
    print("[Silver_Bus_Live_Feed] No GTFS-RT vehicle records. Skipping run.")
    sys.exit(99)

print(f"[Silver_Bus_Live_Feed] GTFS-RT records fetched: {len(df_rt)}")

# GTFS-RT vehicle timestamp is Unix epoch in UTC; store as UTC-naive timestamp.
df_rt['time_gps'] = (
    pd.to_datetime(df_rt['time_gps_raw'], unit='s', utc=True)
    .dt.tz_localize(None)
)
df_rt = df_rt.drop(columns=['time_gps_raw', 'route_id'])
df_rt['etl_timestamp'] = pd.Timestamp.utcnow().tz_localize(None)

hash_input = df_rt['vehicle_number'].astype(str) + '_' + df_rt['time_gps'].astype(str)
df_rt['gps_id'] = [hashlib.md5(x.encode('utf-8')).hexdigest() for x in hash_input]

df_rt = df_rt[['gps_id', 'trip_id', 'lat', 'lon', 'vehicle_number', 'time_gps', 'etl_timestamp']]

df_rt.to_sql('bus_live_feed', engine,schema='silver', if_exists='append', index=False)
print(f"[Silver_Bus_Live_Feed] Records written to silver.bus_live_feed: {len(df_rt)}")

