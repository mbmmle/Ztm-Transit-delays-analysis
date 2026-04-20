# Auto-generated from notebook: work/Silver/Silver_Bus_Matched.ipynb

"""
Silver live-to-schedule matching pipeline.

This script takes the latest live bus positions, keeps vehicles that show
movement over the recent window, joins them to schedule candidates for the
same trip_id, applies progression-aware filtering using last known stop
sequence, and selects the nearest valid stop as the current match.

It also publishes up to five future stops (id and sequence) to support
multi-stop delay projection in downstream Gold logic.
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from config import DB_CONFIG

LOOKBACK_POINTS = 3
MIN_MOVEMENT_METERS = 20.0
NEXT_STOP_COUNT = 5
MAX_MATCH_DISTANCE_METERS = 1200.00


def empty_matched_df():
    base_cols = [
        "gps_id",
        "vehicle_number",
        "trip_id",
        "route_id",
        "lat",
        "lon",
        "time_gps",
        "stop_id",
        "arrival_time",
        "stop_sequence",
        "stop_lat",
        "stop_lon",
        "distance_meters",
        "moved_meters_last3",
        "is_moving_last3",
    ]
    next_cols = []
    for i in range(1, NEXT_STOP_COUNT + 1):
        next_cols.extend([f"next_stop_{i}_id", f"next_stop_{i}_sequence"])
    return pd.DataFrame(columns=base_cols + next_cols)


def haversine_vectorized(lat1, lon1, lat2, lon2):
    earth_radius_m = 6371000.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    return earth_radius_m * (2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a)))


def build_movement_state(df_points):
    rows = []
    for vehicle_number, group in df_points.groupby("vehicle_number"):
        group = group.sort_values("time_gps")
        moved_meters_last3 = 0.0

        if len(group) >= 2:
            segment_distances = haversine_vectorized(
                group["lat"].iloc[:-1].astype(float).to_numpy(),
                group["lon"].iloc[:-1].astype(float).to_numpy(),
                group["lat"].iloc[1:].astype(float).to_numpy(),
                group["lon"].iloc[1:].astype(float).to_numpy(),
            )
            moved_meters_last3 = float(np.nansum(segment_distances))

        rows.append(
            {
                "vehicle_number": vehicle_number,
                "moved_meters_last3": moved_meters_last3,
                "is_moving_last3": moved_meters_last3 >= MIN_MOVEMENT_METERS,
            }
        )

    return pd.DataFrame(rows)


def attach_next_stops(df_matched, df_schedule, next_stop_count=NEXT_STOP_COUNT):
    df_out = df_matched.copy()
    expected_cols = []
    for i in range(1, next_stop_count + 1):
        expected_cols.extend([f"next_stop_{i}_id", f"next_stop_{i}_sequence"])

    if df_out.empty:
        for col in expected_cols:
            if col not in df_out.columns:
                df_out[col] = pd.NA
        return df_out

    df_work = df_out[["vehicle_number", "trip_id", "stop_sequence"]].copy()
    df_work["stop_sequence"] = pd.to_numeric(df_work["stop_sequence"], errors="coerce")

    df_sched = df_schedule[["trip_id", "stop_id", "stop_sequence"]].copy()
    df_sched["stop_sequence"] = pd.to_numeric(df_sched["stop_sequence"], errors="coerce")

    df_candidates = pd.merge(df_work, df_sched, on="trip_id", how="left", suffixes=("_current", "_next"))
    df_candidates = df_candidates[
        df_candidates["stop_sequence_next"] > df_candidates["stop_sequence_current"]
    ].copy()

    if not df_candidates.empty:
        df_candidates = (
            df_candidates.sort_values(["vehicle_number", "trip_id", "stop_sequence_next"])
            .drop_duplicates(subset=["vehicle_number", "trip_id", "stop_sequence_next"], keep="first")
            .copy()
        )
        df_candidates["next_rank"] = (
            df_candidates.groupby(["vehicle_number", "trip_id"]).cumcount() + 1
        )
        df_candidates = df_candidates[df_candidates["next_rank"] <= next_stop_count].copy()

        if not df_candidates.empty:
            df_ids = (
                df_candidates.pivot(
                    index=["vehicle_number", "trip_id"],
                    columns="next_rank",
                    values="stop_id",
                )
                .rename(columns=lambda i: f"next_stop_{int(i)}_id")
                .reset_index()
            )
            df_seq = (
                df_candidates.pivot(
                    index=["vehicle_number", "trip_id"],
                    columns="next_rank",
                    values="stop_sequence_next",
                )
                .rename(columns=lambda i: f"next_stop_{int(i)}_sequence")
                .reset_index()
            )
            df_out = pd.merge(df_out, df_ids, on=["vehicle_number", "trip_id"], how="left")
            df_out = pd.merge(df_out, df_seq, on=["vehicle_number", "trip_id"], how="left")

    for col in expected_cols:
        if col not in df_out.columns:
            df_out[col] = pd.NA

    return df_out


engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

# Step 1: Load live feed and static GTFS reference data.
query_live = """
    SELECT
        blf.gps_id,
        blf.vehicle_number,
        blf.trip_id,
        t.route_id,
        blf.lat,
        blf.lon,
        blf.time_gps
    FROM silver.bus_live_feed blf
    LEFT JOIN silver.trips t ON t.trip_id = blf.trip_id
    WHERE blf.etl_timestamp >= NOW() - INTERVAL '5 minutes'
"""
query_live_for_motion = """
    SELECT
        vehicle_number,
        lat,
        lon,
        time_gps
    FROM silver.bus_live_feed
    WHERE etl_timestamp >= NOW() - INTERVAL '30 minutes'
"""
query_stop_times = """
    SELECT trip_id, stop_id, arrival_time, stop_sequence
    FROM silver.stop_times
    WHERE stop_sequence > 0
"""
query_stops = """
    SELECT stop_id, stop_lat, stop_lon
    FROM silver.stops
"""
query_last_progress = """
    SELECT vehicle_number, trip_id, stop_sequence
    FROM (
        SELECT
            vehicle_number,
            trip_id,
            stop_sequence,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle_number, trip_id
                ORDER BY gold_timestamp DESC NULLS LAST, time_gps DESC NULLS LAST
            ) AS rn
        FROM gold.bus_delays
        WHERE time_gps >= NOW() - INTERVAL '24 hours'
          AND stop_sequence IS NOT NULL
    ) s
    WHERE rn = 1
"""

df_live = pd.read_sql(query_live, engine)
df_live_motion = pd.read_sql(query_live_for_motion, engine)
df_stop_times = pd.read_sql(query_stop_times, engine)
df_stops = pd.read_sql(query_stops, engine)

try:
    df_last_progress = pd.read_sql(query_last_progress, engine)
except Exception:
    df_last_progress = pd.DataFrame(columns=["vehicle_number", "trip_id", "stop_sequence"])

if df_live.empty:
    print("[Silver_Bus_Matched] No live rows, writing empty silver.bus_matched")
    empty_matched_df().to_sql("bus_matched", engine, schema="silver", if_exists="replace", index=False)
    raise SystemExit(99)

# Keep only the newest live point per vehicle.
df_live["time_gps"] = pd.to_datetime(df_live["time_gps"], errors="coerce")
df_live = df_live[df_live["time_gps"].notna()].copy()
df_live = df_live.sort_values("time_gps", ascending=False).drop_duplicates(subset=["vehicle_number"], keep="first")

# Compute movement state from the last 3 observations per vehicle.
df_live_motion["time_gps"] = pd.to_datetime(df_live_motion["time_gps"], errors="coerce")
df_live_motion = df_live_motion[df_live_motion["time_gps"].notna()].copy()
df_live_motion = (
    df_live_motion.sort_values(["vehicle_number", "time_gps"], ascending=[True, False])
    .groupby("vehicle_number")
    .head(LOOKBACK_POINTS)
    .copy()
)
df_movement_state = build_movement_state(df_live_motion)

if not df_movement_state.empty:
    df_live = pd.merge(df_live, df_movement_state, on="vehicle_number", how="left")
else:
    df_live["moved_meters_last3"] = 0.0
    df_live["is_moving_last3"] = False

df_live["is_moving_last3"] = df_live["is_moving_last3"].fillna(False)
df_live["moved_meters_last3"] = pd.to_numeric(df_live["moved_meters_last3"], errors="coerce").fillna(0.0)

before_movement_filter = len(df_live)
df_live = df_live[df_live["is_moving_last3"]].copy()
print(
    "[Silver_Bus_Matched] Movement filter (last 3 occurrences): "
    f"{len(df_live)} / {before_movement_filter} vehicles kept "
    f"(min movement {MIN_MOVEMENT_METERS}m)"
)

if df_live.empty:
    print("[Silver_Bus_Matched] No moving vehicles, writing empty silver.bus_matched")
    empty_matched_df().to_sql("bus_matched", engine, schema="silver", if_exists="replace", index=False)
    raise SystemExit(99)

# Step 2: Build schedule candidate grid.
df_schedule = pd.merge(df_stop_times, df_stops, on="stop_id", how="left")
df_schedule["stop_sequence"] = pd.to_numeric(df_schedule["stop_sequence"], errors="coerce")
df_schedule = df_schedule[df_schedule["stop_sequence"].notna() & (df_schedule["stop_sequence"] > 0)].copy()

# Step 3: Join live data with schedule by trip_id.
df_joined = pd.merge(df_live, df_schedule, on="trip_id", how="inner")

# Keep only stops ahead of the last known stop_sequence for each vehicle-trip pair.
if not df_last_progress.empty:
    df_last_progress = df_last_progress.rename(columns={"stop_sequence": "last_stop_sequence"})
    df_last_progress["last_stop_sequence"] = pd.to_numeric(df_last_progress["last_stop_sequence"], errors="coerce")
    df_joined = pd.merge(
        df_joined,
        df_last_progress[["vehicle_number", "trip_id", "last_stop_sequence"]],
        on=["vehicle_number", "trip_id"],
        how="left",
    )
    forward_mask = df_joined["last_stop_sequence"].isna() | (df_joined["stop_sequence"] > df_joined["last_stop_sequence"])
    before_forward_filter = len(df_joined)
    df_joined = df_joined[forward_mask].copy()
    print(
        "[Silver_Bus_Matched] Forward-only sequence filter: "
        f"{len(df_joined)} / {before_forward_filter} candidates kept"
    )

if df_joined.empty:
    print("[Silver_Bus_Matched] No joined rows for current trip_ids, writing empty silver.bus_matched")
    empty_matched_df().to_sql("bus_matched", engine, schema="silver", if_exists="replace", index=False)
    raise SystemExit(99)

# Step 4: Compute vectorized Haversine distance in meters.
df_joined["distance_meters"] = haversine_vectorized(
    df_joined["lat"].astype(float).to_numpy(),
    df_joined["lon"].astype(float).to_numpy(),
    pd.to_numeric(df_joined["stop_lat"], errors="coerce").to_numpy(),
    pd.to_numeric(df_joined["stop_lon"], errors="coerce").to_numpy(),
)

before_distance_filter = len(df_joined)
df_joined = df_joined[pd.to_numeric(df_joined["distance_meters"], errors="coerce").notna()].copy()
print(
    "[Silver_Bus_Matched] Distance validation: "
    f"{len(df_joined)} / {before_distance_filter} candidates with numeric distance"
)

if df_joined.empty:
    print("[Silver_Bus_Matched] No candidates after distance filter, writing empty silver.bus_matched")
    empty_matched_df().to_sql("bus_matched", engine, schema="silver", if_exists="replace", index=False)
    raise SystemExit(99)

# Step 5: Select the nearest stop for each vehicle_number + trip_id.
df_nearest = (
    df_joined.sort_values("distance_meters", ascending=True)
    .drop_duplicates(subset=["vehicle_number", "trip_id"], keep="first")
    .copy()
)
# Step 6: Keep only matches within a reasonable distance threshold and attach next stops.
df_nearest = df_nearest[df_nearest["distance_meters"] <= MAX_MATCH_DISTANCE_METERS].copy()
df_nearest = attach_next_stops(df_nearest, df_schedule, NEXT_STOP_COUNT)
with_next = int(df_nearest["next_stop_1_id"].notna().sum()) if "next_stop_1_id" in df_nearest.columns else 0
print(
    "[Silver_Bus_Matched] Added next stops (1..5): "
    f"{with_next}/{len(df_nearest)} rows have at least one future stop"
)

print(f"[Silver_Bus_Matched] Rows written to silver.bus_matched: {len(df_nearest)}")
df_nearest.to_sql("bus_matched", engine, schema="silver", if_exists="replace", index=False)
