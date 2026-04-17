# Auto-generated from notebook: work/Gold/Gold_Bus_Delays.ipynb

import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG
import psycopg2

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
conn.commit()
conn.close()

engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
df_matched = pd.read_sql("SELECT * FROM silver.bus_matched", engine)


df_matched['delay_seconds'] =  (df_matched['arrival_time'] - df_matched['time_gps']).dt.total_seconds().astype(int)
df_matched['delay_minutes'] =   df_matched['delay_seconds'] / 60 
df_matched.round({'delay_minutes': 1})

df_gold = df_matched 
df_gold["gold_timestamp"] = pd.Timestamp.now()

df_gold.to_sql("bus_delays", engine, schema="gold", if_exists="append", index=False)

