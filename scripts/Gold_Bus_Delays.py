# Auto-generated from notebook: work/Gold/Gold_Bus_Delays.ipynb

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from config import DB_CONFIG
import psycopg2


def minimal_time_diff_seconds(arrival_time, gps_time):
	base = (arrival_time - gps_time).dt.total_seconds()
	minus_day = (arrival_time - pd.Timedelta(days=1) - gps_time).dt.total_seconds()
	plus_day = (arrival_time + pd.Timedelta(days=1) - gps_time).dt.total_seconds()

	stacked = np.vstack([base.to_numpy(), minus_day.to_numpy(), plus_day.to_numpy()])
	min_idx = np.abs(stacked).argmin(axis=0)
	return stacked[min_idx, np.arange(stacked.shape[1])]

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
conn.commit()
conn.close()

engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
df_matched = pd.read_sql("SELECT * FROM silver.bus_matched", engine)

df_matched['arrival_time'] = pd.to_datetime(df_matched['arrival_time'])
df_matched['time_gps'] = pd.to_datetime(df_matched['time_gps'])

df_matched['delay_seconds'] = minimal_time_diff_seconds(df_matched['arrival_time'], df_matched['time_gps']).astype(int)
df_matched['delay_minutes'] = (df_matched['delay_seconds'] / 60).round(1)

# Guard against loop-terminal artifacts that can still slip through matching.
terminal_artifact_mask = (
	(df_matched['stop_sequence'] <= 1)
	& (df_matched['distance'] > 3000)
	& (df_matched['delay_minutes'] > 10)
)
terminal_artifact_removed = int(terminal_artifact_mask.sum())
if terminal_artifact_removed > 0:
	df_matched = df_matched[~terminal_artifact_mask].copy()
	print(
		"[Gold_Bus_Delays] Removed terminal-loop artifacts "
		f"(seq<=1, dist>3000m, delay>10m): {terminal_artifact_removed}"
	)

# At terminal stops, hold back large delay values until the bus progresses into the route.
terminal_large_delay_mask = (
	(df_matched['stop_sequence'] <= 1)
	& (df_matched['delay_minutes'] > 10)
)
terminal_large_delay_removed = int(terminal_large_delay_mask.sum())
if terminal_large_delay_removed > 0:
	df_matched = df_matched[~terminal_large_delay_mask].copy()
	print(
		"[Gold_Bus_Delays] Suppressed large terminal delays "
		f"(seq<=1, delay>10m): {terminal_large_delay_removed}"
	)

aux_cols = [
	'point_count',
	'moved_meters_last3',
	'geo_direction',
	'oldest_stop_sequence',
	'newest_stop_sequence',
	'stop_sequence_delta',
	'travel_direction',
	'is_moving',
]
df_matched = df_matched.drop(columns=[c for c in aux_cols if c in df_matched.columns])

# Drop implausible outliers so downstream aggregates remain stable.
df_matched = df_matched[df_matched['delay_minutes'].between(-15, 60)].copy()
print(f"[Gold_Bus_Delays] Records after delay outlier filter (-15..60 min): {len(df_matched)}")

df_gold = df_matched 
df_gold["gold_timestamp"] = pd.Timestamp.now()

df_gold.to_sql("bus_delays", engine, schema="gold", if_exists="append", index=False)

