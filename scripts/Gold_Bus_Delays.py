# Auto-generated from notebook: work/Gold/Gold_Bus_Delays.ipynb

"""
Gold delay computation and fleet snapshot publisher.

This script converts matched Silver records into delay facts by comparing GPS
observation time with scheduled arrival time (including midnight rollover
normalization). It writes detailed delay rows to gold.bus_delays and publishes
an up-to-date per-vehicle view to gold.fleet.

Business rules currently include:
- skip terminal records at the last stop_sequence of each trip,
- mark delayed vehicles with is_delayed flag in fleet output.
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config import DB_CONFIG


def parse_gtfs_time_to_seconds(value):
    if pd.isna(value):
        return np.nan

    value_str = str(value).strip()
    parts = value_str.split(":")
    if len(parts) != 3:
        return np.nan

    try:
        hh = int(parts[0])
        mm = int(parts[1])
        ss = int(parts[2])
    except ValueError:
        return np.nan

    return hh * 3600 + mm * 60 + ss


def minimal_time_diff_seconds(arrival_time, gps_time):
    base = (arrival_time - gps_time).dt.total_seconds()
    minus_day = (arrival_time - pd.Timedelta(days=1) - gps_time).dt.total_seconds()
    plus_day = (arrival_time + pd.Timedelta(days=1) - gps_time).dt.total_seconds()

    stacked = np.vstack([base.to_numpy(), minus_day.to_numpy(), plus_day.to_numpy()])
    min_idx = np.abs(stacked).argmin(axis=0)
    return stacked[min_idx, np.arange(stacked.shape[1])]


engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

BUS_DELAYS_WRITE_COLS = [
    "gps_id",
    "trip_id",
    "gps_lat",
    "gps_lon",
    "time_gps",
    "vehicle_number",
    "time_diff_seconds",
    "abs_time_diff_seconds",
    "delay_seconds",
    "delay_minutes",
    "schedule_id",
    "stop_id",
    "route_id",
    "route_name",
    "stop_name",
    "district",
    "stop_sequence",
    "stop_lat",
    "stop_lon",
    "arrival_time",
    "departure_time",
    "distance",
    "gold_timestamp",
]

FLEET_COLS = [
    "vehicle_number",
    "route_id",
    "trip_id",
    "gps_lat",
    "gps_lon",
    "time_gps",
    "window_start",
    "window_end",
    "stop_id",
    "stop_name",
    "stop_district",
    "stop_sequence",
    "delay_seconds",
    "delay_minutes",
    "is_delayed",
]

# Step 6: Compute delays from silver.bus_matched.
df_matched = pd.read_sql("SELECT * FROM silver.bus_matched", engine)
df_stop_dim = pd.read_sql(
    "SELECT stop_id, stop_name, district FROM silver.stops",
    engine,
)
df_trip_last_stop = pd.read_sql(
    """
    SELECT trip_id, MAX(stop_sequence) AS max_stop_sequence
    FROM silver.stop_times
    WHERE stop_sequence IS NOT NULL
    GROUP BY trip_id
    """,
    engine,
)

with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS gold.fleet (
                vehicle_number TEXT,
                route_id TEXT,
                trip_id TEXT,
                time_gps TIMESTAMP,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                stop_id BIGINT,
                stop_name TEXT,
                stop_district TEXT,
                stop_sequence BIGINT,
                delay_seconds BIGINT,
                delay_minutes DOUBLE PRECISION,
                is_delayed BOOLEAN
            )
            """
        )
    )
    conn.execute(text("ALTER TABLE gold.fleet ADD COLUMN IF NOT EXISTS window_start TIMESTAMP"))
    conn.execute(text("ALTER TABLE gold.fleet ADD COLUMN IF NOT EXISTS window_end TIMESTAMP"))
    conn.execute(text("ALTER TABLE gold.fleet ADD COLUMN IF NOT EXISTS stop_name TEXT"))
    conn.execute(text("ALTER TABLE gold.fleet ADD COLUMN IF NOT EXISTS stop_district TEXT"))
    conn.execute(text("ALTER TABLE gold.fleet ADD COLUMN IF NOT EXISTS gps_lat DOUBLE PRECISION"))
    conn.execute(text("ALTER TABLE gold.fleet ADD COLUMN IF NOT EXISTS gps_lon DOUBLE PRECISION"))
    conn.execute(text("ALTER TABLE gold.fleet DROP COLUMN IF EXISTS snapshot_timestamp"))
    conn.execute(text("ALTER TABLE gold.fleet DROP COLUMN IF EXISTS gold_timestamp"))

if df_matched.empty:
    print("[Gold_Bus_Delays] No matched rows, appending empty batch to gold.fleet")
    pd.DataFrame(columns=FLEET_COLS).to_sql("fleet", engine, schema="gold", if_exists="append", index=False)
    raise SystemExit(99)

if "time_gps" not in df_matched.columns or "arrival_time" not in df_matched.columns:
    raise RuntimeError("silver.bus_matched must contain columns: time_gps and arrival_time")

df_matched["time_gps"] = pd.to_datetime(df_matched["time_gps"], errors="coerce")
df_matched = df_matched[df_matched["time_gps"].notna()].copy()
df_matched["time_gps_utc"] = df_matched["time_gps"].dt.tz_localize("UTC")

# Do not report delay for the final stop on a trip.
df_matched["stop_sequence"] = pd.to_numeric(df_matched["stop_sequence"], errors="coerce")
if not df_trip_last_stop.empty:
    df_trip_last_stop["max_stop_sequence"] = pd.to_numeric(df_trip_last_stop["max_stop_sequence"], errors="coerce")
    df_matched = pd.merge(df_matched, df_trip_last_stop, on="trip_id", how="left")
    before_last_stop_filter = len(df_matched)
    df_matched = df_matched[
        df_matched["max_stop_sequence"].isna()
        | (df_matched["stop_sequence"] < df_matched["max_stop_sequence"])
    ].copy()
    print(
        "[Gold_Bus_Delays] Last-stop filter: "
        f"{len(df_matched)} / {before_last_stop_filter} rows kept"
    )
    df_matched = df_matched.drop(columns=["max_stop_sequence"])

df_matched["scheduled_arrival_local"] = pd.to_datetime(df_matched["arrival_time"], errors="coerce")
df_matched = df_matched[df_matched["scheduled_arrival_local"].notna()].copy()

if "departure_time" in df_matched.columns:
    df_matched["scheduled_departure_local"] = pd.to_datetime(df_matched["departure_time"], errors="coerce")
else:
    df_matched["scheduled_departure_local"] = df_matched["scheduled_arrival_local"]

df_matched["scheduled_arrival"] = (
    df_matched["scheduled_arrival_local"]
    .dt.tz_localize("Europe/Warsaw", ambiguous="NaT", nonexistent="NaT")
    .dt.tz_convert("UTC")
    .dt.tz_localize(None)
)
df_matched["scheduled_departure"] = (
    df_matched["scheduled_departure_local"]
    .dt.tz_localize("Europe/Warsaw", ambiguous="NaT", nonexistent="NaT")
    .dt.tz_convert("UTC")
    .dt.tz_localize(None)
)

df_matched["arrival_time"] = df_matched["scheduled_arrival"]
df_matched["departure_time"] = df_matched["scheduled_departure"]

df_matched["delay_seconds"] = (df_matched["time_gps"] - df_matched["scheduled_arrival"]).dt.total_seconds()
df_matched["delay_seconds"] = pd.to_numeric(df_matched["delay_seconds"], errors="coerce").fillna(0).round().astype(int)
df_matched["delay_minutes"] = (df_matched["delay_seconds"] / 60.0).round(2)
now_utc = pd.Timestamp.utcnow().tz_localize(None)
df_matched["gold_timestamp"] = now_utc
batch_window_start = now_utc.floor("min")
batch_window_end = batch_window_start + pd.Timedelta(minutes=1)

# Keep only delayed records in gold.bus_delays (skip early/on-time arrivals).
before_delay_filter = len(df_matched)
df_matched = df_matched[df_matched["delay_seconds"] > 0].copy()
print(
    "[Gold_Bus_Delays] Positive-delay filter: "
    f"{len(df_matched)} / {before_delay_filter} rows kept"
)

if df_matched.empty:
    print("[Gold_Bus_Delays] No delayed rows, appending empty batch to gold.fleet")
    pd.DataFrame(columns=FLEET_COLS).to_sql("fleet", engine, schema="gold", if_exists="append", index=False)
    raise SystemExit(99)

# Prepare a schema-stable dataset for gold.bus_delays writes.
if "lat" in df_matched.columns:
    df_matched["gps_lat"] = pd.to_numeric(df_matched["lat"], errors="coerce")
if "lon" in df_matched.columns:
    df_matched["gps_lon"] = pd.to_numeric(df_matched["lon"], errors="coerce")
if "distance_meters" in df_matched.columns:
    df_matched["distance"] = pd.to_numeric(df_matched["distance_meters"], errors="coerce")
df_matched["stop_id"] = pd.to_numeric(df_matched.get("stop_id"), errors="coerce")
df_matched["stop_sequence"] = pd.to_numeric(df_matched.get("stop_sequence"), errors="coerce")
df_matched["stop_lat"] = pd.to_numeric(df_matched.get("stop_lat"), errors="coerce")
df_matched["stop_lon"] = pd.to_numeric(df_matched.get("stop_lon"), errors="coerce")

if not df_stop_dim.empty:
    df_stop_dim["stop_id"] = pd.to_numeric(df_stop_dim["stop_id"], errors="coerce")
    df_matched = pd.merge(
        df_matched,
        df_stop_dim[["stop_id", "stop_name", "district"]],
        on="stop_id",
        how="left",
        suffixes=("", "_dim"),
    )
    if "stop_name_dim" in df_matched.columns:
        if "stop_name" in df_matched.columns:
            df_matched["stop_name"] = df_matched["stop_name"].where(df_matched["stop_name"].notna(), df_matched["stop_name_dim"])
        else:
            df_matched["stop_name"] = df_matched["stop_name_dim"]
        df_matched = df_matched.drop(columns=["stop_name_dim"])
    if "district_dim" in df_matched.columns:
        if "district" in df_matched.columns:
            df_matched["district"] = df_matched["district"].where(df_matched["district"].notna(), df_matched["district_dim"])
        else:
            df_matched["district"] = df_matched["district_dim"]
        df_matched = df_matched.drop(columns=["district_dim"])

for col in BUS_DELAYS_WRITE_COLS:
    if col not in df_matched.columns:
        df_matched[col] = pd.NA

df_bus_delays = df_matched[BUS_DELAYS_WRITE_COLS].copy()
df_bus_delays = (
    df_bus_delays.sort_values(["time_gps", "vehicle_number", "trip_id", "distance"], ascending=[False, True, True, True])
    .drop_duplicates(subset=["gps_id", "vehicle_number", "trip_id", "time_gps", "stop_id"], keep="first")
    .copy()
)

print(f"[Gold_Bus_Delays] Rows written to gold.bus_delays: {len(df_bus_delays)}")
df_bus_delays.to_sql("bus_delays", engine, schema="gold", if_exists="append", index=False)

# gold.fleet: current fleet snapshot with delay status (1 row per vehicle).
df_fleet = df_bus_delays.copy()
df_fleet["window_start"] = batch_window_start
df_fleet["window_end"] = batch_window_end
df_fleet["stop_district"] = df_fleet.get("district")
df_fleet["is_delayed"] = pd.to_numeric(df_fleet["delay_seconds"], errors="coerce").fillna(0).astype(int) > 0
df_fleet = (
    df_fleet.sort_values(["vehicle_number", "time_gps"], ascending=[True, False])
    .drop_duplicates(subset=["vehicle_number"], keep="first")
    .copy()
)
for col in FLEET_COLS:
    if col not in df_fleet.columns:
        df_fleet[col] = pd.NA
df_fleet = df_fleet[FLEET_COLS]

print(f"[Gold_Bus_Delays] Rows written to gold.fleet: {len(df_fleet)}")
df_fleet.to_sql("fleet", engine, schema="gold", if_exists="append", index=False)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE OR REPLACE VIEW gold.v_fleet AS
        SELECT *,
        window_end::date AS date,
        EXTRACT(HOUR FROM window_end) AS time
        FROM gold.fleet;
    """))
