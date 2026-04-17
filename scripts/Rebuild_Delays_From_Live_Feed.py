import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import DB_CONFIG


def haversine_vectorized(lat1, lon1, lat2, lon2):
    r = 6371000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))


engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

query_feed = """
    SELECT gps_id, trip_id, lat AS gps_lat, lon AS gps_lon, time_gps, vehicle_number
    FROM silver.bus_live_feed
"""

query_schedules = """
    SELECT *
    FROM silver.master_schedule
    WHERE trip_id = ANY(%(trip_ids)s)
"""

df_feed = pd.read_sql(query_feed, engine)
df_feed = df_feed.dropna(subset=["trip_id"]).drop_duplicates(subset=["gps_id"], keep="last")
trip_ids = df_feed["trip_id"].unique().tolist()

df_schedules = pd.read_sql(query_schedules, engine, params={"trip_ids": trip_ids})

df_merged = pd.merge(df_feed, df_schedules, on="trip_id", how="inner")
df_merged["distance"] = haversine_vectorized(
    df_merged["gps_lat"],
    df_merged["gps_lon"],
    df_merged["stop_lat"],
    df_merged["stop_lon"],
)

df_top2 = (
    df_merged.sort_values(["gps_id", "distance"])
    .groupby("gps_id")
    .head(2)
)

df_matched = (
    df_top2.sort_values(["gps_id", "stop_sequence"], ascending=[True, False])
    .groupby("gps_id")
    .head(1)
)

df_matched.to_sql("bus_matched", engine, schema="silver", if_exists="replace", index=False)

with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
    conn.execute(text("DROP TABLE IF EXISTS gold.bus_delays"))


df_gold = df_matched.copy()
df_gold["delay_seconds"] = (df_gold["arrival_time"] - df_gold["time_gps"]).dt.total_seconds().astype(int)
df_gold["delay_minutes"] = (df_gold["delay_seconds"] / 60).round(1)
df_gold["gold_timestamp"] = pd.Timestamp.now()

df_gold.to_sql("bus_delays", engine, schema="gold", if_exists="append", index=False)
