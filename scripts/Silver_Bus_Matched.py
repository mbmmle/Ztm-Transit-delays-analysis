# Auto-generated from notebook: work/Silver/Silver_Bus_Matched.ipynb

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from config import DB_CONFIG

def haversine_vectorized(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return R * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))


def minimal_time_diff_seconds(arrival_time, gps_time):
    base = (arrival_time - gps_time).dt.total_seconds()
    minus_day = (arrival_time - pd.Timedelta(days=1) - gps_time).dt.total_seconds()
    plus_day = (arrival_time + pd.Timedelta(days=1) - gps_time).dt.total_seconds()

    stacked = np.vstack([base.to_numpy(), minus_day.to_numpy(), plus_day.to_numpy()])
    min_idx = np.abs(stacked).argmin(axis=0)
    return stacked[min_idx, np.arange(stacked.shape[1])]


def movement_stats_for_last_three(df_points):
    rows = []
    for vehicle_number, grp in df_points.groupby("vehicle_number"):
        grp = grp.sort_values("time_gps").copy()
        point_count = len(grp)
        moved_meters = 0.0
        geo_direction = "unknown"
        if point_count >= 2:
            seg_dist = haversine_vectorized(
                grp["gps_lat"].iloc[:-1].to_numpy(),
                grp["gps_lon"].iloc[:-1].to_numpy(),
                grp["gps_lat"].iloc[1:].to_numpy(),
                grp["gps_lon"].iloc[1:].to_numpy(),
            )
            moved_meters = float(np.nansum(seg_dist))
        if point_count >= 3 and moved_meters >= 80:
            d_lat = float(grp["gps_lat"].iloc[-1] - grp["gps_lat"].iloc[0])
            d_lon = float(grp["gps_lon"].iloc[-1] - grp["gps_lon"].iloc[0])
            if abs(d_lat) >= abs(d_lon):
                geo_direction = "north" if d_lat > 0 else "south"
            else:
                geo_direction = "east" if d_lon > 0 else "west"
        rows.append(
            {
                "vehicle_number": vehicle_number,
                "point_count": int(point_count),
                "moved_meters_last3": moved_meters,
                "geo_direction": geo_direction,
            }
        )
    return pd.DataFrame(rows)

engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
query_feed = """
        SELECT gps_id, trip_id, lat as gps_lat, lon as gps_lon, time_gps, vehicle_number
        FROM silver.bus_live_feed
        WHERE etl_timestamp >= NOW() - INTERVAL '5 minutes'
    """
query_schedules = """
        SELECT *
        FROM silver.master_schedule
        WHERE trip_id = ANY(%(trip_ids)s)
    """
query_feed_for_motion = """
        SELECT gps_id, trip_id, lat as gps_lat, lon as gps_lon, time_gps, vehicle_number
        FROM silver.bus_live_feed
        WHERE etl_timestamp >= NOW() - INTERVAL '20 minutes'
    """


df_feed = pd.read_sql(query_feed, engine)
df_feed = df_feed.sort_values("time_gps", ascending=False).drop_duplicates(subset=["vehicle_number"])
print(f"[Silver_Bus_Matched] Feed records after vehicle deduplication: {len(df_feed)}")

df_feed_motion = pd.read_sql(query_feed_for_motion, engine)
df_feed_motion["time_gps"] = pd.to_datetime(df_feed_motion["time_gps"])
df_feed_motion = (
    df_feed_motion.sort_values(["vehicle_number", "time_gps"], ascending=[True, False])
    .groupby("vehicle_number")
    .head(3)
    .copy()
)
df_motion_stats = movement_stats_for_last_three(df_feed_motion)

trip_ids = df_feed['trip_id'].dropna().unique().tolist()
if not trip_ids:
    print("[Silver_Bus_Matched] No trip_ids in feed, writing empty silver.bus_matched")
    df_empty = pd.DataFrame(
        columns=[
            "gps_id",
            "trip_id",
            "gps_lat",
            "gps_lon",
            "time_gps",
            "vehicle_number",
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
            "time_diff_seconds",
            "abs_time_diff_seconds",
            "point_count",
            "moved_meters_last3",
            "oldest_stop_sequence",
            "newest_stop_sequence",
            "stop_sequence_delta",
            "travel_direction",
            "is_moving",
        ]
    )
    df_empty.to_sql('bus_matched', engine, schema='silver', if_exists='replace', index=False)
    raise SystemExit(0)

df_schedules = pd.read_sql(query_schedules, engine, params={"trip_ids": trip_ids})
print(f"[Silver_Bus_Matched] Schedule records loaded for trip_ids: {len(df_schedules)}")

df_merged = pd.merge(df_feed, df_schedules, on='trip_id', how='inner')
df_merged['time_gps'] = pd.to_datetime(df_merged['time_gps'])
df_merged['arrival_time'] = pd.to_datetime(df_merged['arrival_time'])
df_merged['distance'] = haversine_vectorized(
    df_merged['gps_lat'], df_merged['gps_lon'],
    df_merged['stop_lat'], df_merged['stop_lon']
)

# Prefer stops with plausible schedule time near the observed GPS timestamp.
df_merged['time_diff_seconds'] = minimal_time_diff_seconds(df_merged['arrival_time'], df_merged['time_gps'])
df_merged['abs_time_diff_seconds'] = np.abs(df_merged['time_diff_seconds'])

df_candidates = df_merged[
    (df_merged['abs_time_diff_seconds'] <= 2 * 60 * 60)
    & (df_merged['distance'] <= 5000)
].copy()

if df_candidates.empty:
    print("[Silver_Bus_Matched] No candidates within +/-2h and <=5km, falling back to +/-3h and <=8km")
    df_candidates = df_merged[
        (df_merged['abs_time_diff_seconds'] <= 3 * 60 * 60)
        & (df_merged['distance'] <= 8000)
    ].copy()

if df_candidates.empty:
    print("[Silver_Bus_Matched] No distance-constrained candidates, falling back to +/-3h only")
    df_candidates = df_merged[df_merged['abs_time_diff_seconds'] <= 3 * 60 * 60].copy()

if df_candidates.empty:
    print("[Silver_Bus_Matched] No time-plausible candidates within +/-3h, using full candidate set")
    df_candidates = df_merged.copy()

# Avoid false terminal matches: stop_sequence 0/1 far from the matched terminal stop.
terminal_far_mask = (df_candidates["stop_sequence"] <= 1) & (df_candidates["distance"] > 3000)
terminal_far_removed = int(terminal_far_mask.sum())
if terminal_far_removed > 0:
    df_candidates_no_terminal_far = df_candidates[~terminal_far_mask].copy()
    if not df_candidates_no_terminal_far.empty:
        df_candidates = df_candidates_no_terminal_far
        print(
            "[Silver_Bus_Matched] Removed implausible terminal candidates "
            f"(stop_sequence<=1 and distance>3000m): {terminal_far_removed}"
        )
    else:
        print(
            "[Silver_Bus_Matched] Terminal filter would remove all candidates; "
            "keeping pre-filter candidate set"
        )

df_matched = (
    df_candidates.sort_values(
        ["vehicle_number", "abs_time_diff_seconds", "distance", "stop_sequence"],
        ascending=[True, True, True, False],
    )
    .groupby("vehicle_number")
    .head(1)
)

# Determine direction from the trend between oldest and newest matched stop_sequence in last 3 observations.
trip_ids_motion = df_feed_motion["trip_id"].dropna().unique().tolist()
if trip_ids_motion:
    df_schedules_motion = pd.read_sql(query_schedules, engine, params={"trip_ids": trip_ids_motion})
    df_motion_merged = pd.merge(df_feed_motion, df_schedules_motion, on="trip_id", how="inner")
    df_motion_merged["arrival_time"] = pd.to_datetime(df_motion_merged["arrival_time"])
    df_motion_merged["time_gps"] = pd.to_datetime(df_motion_merged["time_gps"])
    df_motion_merged["distance"] = haversine_vectorized(
        df_motion_merged["gps_lat"],
        df_motion_merged["gps_lon"],
        df_motion_merged["stop_lat"],
        df_motion_merged["stop_lon"],
    )
    df_motion_merged["time_diff_seconds"] = minimal_time_diff_seconds(
        df_motion_merged["arrival_time"],
        df_motion_merged["time_gps"],
    )
    df_motion_merged["abs_time_diff_seconds"] = np.abs(df_motion_merged["time_diff_seconds"])

    df_motion_best = (
        df_motion_merged.sort_values(
            ["vehicle_number", "time_gps", "abs_time_diff_seconds", "distance", "stop_sequence"],
            ascending=[True, False, True, True, False],
        )
        .groupby(["vehicle_number", "time_gps"]) 
        .head(1)
        .copy()
    )

    df_motion_time = df_motion_best.sort_values(["vehicle_number", "time_gps"]).copy()
    df_motion_dir = (
        df_motion_time.groupby("vehicle_number")
        .agg(
            oldest_stop_sequence=("stop_sequence", "first"),
            newest_stop_sequence=("stop_sequence", "last"),
        )
        .reset_index()
    )
    df_motion_dir["stop_sequence_delta"] = (
        df_motion_dir["newest_stop_sequence"] - df_motion_dir["oldest_stop_sequence"]
    )
    df_motion_dir["travel_direction"] = np.where(
        df_motion_dir["stop_sequence_delta"] > 0,
        "forward",
        np.where(df_motion_dir["stop_sequence_delta"] < 0, "backward", "unknown"),
    )
else:
    df_motion_dir = pd.DataFrame(
        columns=["vehicle_number", "oldest_stop_sequence", "newest_stop_sequence", "stop_sequence_delta", "travel_direction"]
    )

df_motion_state = pd.merge(df_motion_stats, df_motion_dir, on="vehicle_number", how="left")
df_motion_state["travel_direction"] = df_motion_state["travel_direction"].fillna("unknown")

# Stationary buses (e.g. waiting on loop terminals) are excluded until they start moving.
df_motion_state["is_moving"] = (
    (df_motion_state["point_count"] >= 3)
    & (df_motion_state["moved_meters_last3"] >= 80)
)
df_motion_state["travel_direction"] = np.where(
    (df_motion_state["travel_direction"] == "unknown") & df_motion_state["is_moving"],
    df_motion_state["geo_direction"],
    df_motion_state["travel_direction"],
)

df_matched = pd.merge(df_matched, df_motion_state, on="vehicle_number", how="left")
df_matched["is_moving"] = df_matched["is_moving"].fillna(False)
df_matched["travel_direction"] = df_matched["travel_direction"].fillna("unknown")

before_motion_filter = len(df_matched)
df_matched = df_matched[df_matched["is_moving"] & (df_matched["travel_direction"] != "unknown")].copy()
print(
    "[Silver_Bus_Matched] Records after movement/direction filter "
    f"(3 points, >=80m, known direction): {len(df_matched)} / {before_motion_filter}"
)

print(f"[Silver_Bus_Matched] Records after matching: {len(df_matched)}")
df_matched.to_sql('bus_matched', engine, schema='silver', if_exists='replace', index=False)
print(f"[Silver_Bus_Matched] Records written to silver.bus_matched: {len(df_matched)}")

if not df_matched.empty:
    df_history = df_matched.copy()
    df_history["match_timestamp"] = pd.Timestamp.now()
    df_history.to_sql('bus_matched_history', engine, schema='silver', if_exists='append', index=False)

