-- Lucidchart ERD import: SILVER + GOLD schema (logical model)

CREATE TABLE silver.routes (
  route_id TEXT PRIMARY KEY,
  agency_id INTEGER,
  route_name TEXT,
  route_desc TEXT,
  route_type INTEGER
);

CREATE TABLE silver.trips (
  trip_id TEXT PRIMARY KEY,
  route_id TEXT NOT NULL,
  service_id TEXT,
  trip_headsign TEXT,
  direction_id INTEGER
);

CREATE TABLE silver.stops (
  stop_id INTEGER PRIMARY KEY,
  stop_code TEXT,
  stop_name TEXT,
  stop_lat REAL,
  stop_lon REAL,
  district TEXT
);

CREATE TABLE silver.stop_times (
  trip_id TEXT NOT NULL,
  stop_id INTEGER NOT NULL,
  stop_sequence INTEGER NOT NULL,
  pickup_type INTEGER,
  drop_off_type INTEGER,
  shape_dist_traveled REAL,
  arrival_time TIMESTAMP,
  departure_time TIMESTAMP,
  PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE silver.master_schedule (
  schedule_id TEXT PRIMARY KEY,
  trip_id TEXT NOT NULL,
  stop_id INTEGER NOT NULL,
  route_id TEXT NOT NULL,
  route_name TEXT,
  stop_name TEXT,
  district TEXT,
  stop_sequence INTEGER,
  stop_lat REAL,
  stop_lon REAL,
  arrival_time TIMESTAMP,
  departure_time TIMESTAMP
);

CREATE TABLE silver.bus_live_feed (
  gps_id TEXT PRIMARY KEY,
  trip_id TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  vehicle_number TEXT,
  time_gps TIMESTAMP,
  etl_timestamp TIMESTAMP
);

CREATE TABLE silver.bus_matched (
  gps_id TEXT,
  vehicle_number TEXT,
  trip_id TEXT,
  route_id TEXT,
  lat TEXT,
  lon TEXT,
  time_gps TEXT,
  stop_id TEXT,
  arrival_time TEXT,
  stop_sequence TEXT,
  stop_lat TEXT,
  stop_lon TEXT,
  distance_meters TEXT,
  moved_meters_last3 TEXT,
  is_moving_last3 TEXT,
  next_stop_1_id TEXT,
  next_stop_1_sequence TEXT,
  next_stop_2_id TEXT,
  next_stop_2_sequence TEXT,
  next_stop_3_id TEXT,
  next_stop_3_sequence TEXT,
  next_stop_4_id TEXT,
  next_stop_4_sequence TEXT,
  next_stop_5_id TEXT,
  next_stop_5_sequence TEXT
);

CREATE TABLE gold.bus_delays (
  gps_id TEXT,
  trip_id TEXT,
  gps_lat DOUBLE PRECISION,
  gps_lon DOUBLE PRECISION,
  time_gps TIMESTAMP,
  vehicle_number TEXT,
  time_diff_seconds DOUBLE PRECISION,
  abs_time_diff_seconds DOUBLE PRECISION,
  delay_seconds BIGINT,
  delay_minutes DOUBLE PRECISION,
  schedule_id TEXT,
  stop_id BIGINT,
  route_id TEXT,
  route_name TEXT,
  stop_name TEXT,
  district TEXT,
  stop_sequence BIGINT,
  stop_lat DOUBLE PRECISION,
  stop_lon DOUBLE PRECISION,
  arrival_time TIMESTAMP,
  departure_time TIMESTAMP,
  distance DOUBLE PRECISION,
  gold_timestamp TIMESTAMP,
  PRIMARY KEY (gps_id, stop_id, time_gps)
);

CREATE TABLE gold.delays_by_stop (
  stop_id BIGINT,
  stop_name TEXT,
  stop_lat DOUBLE PRECISION,
  stop_lon DOUBLE PRECISION,
  district TEXT,
  average_delay_minutes DOUBLE PRECISION,
  average_delay_seconds DOUBLE PRECISION,
  max_delay_minutes DOUBLE PRECISION,
  min_delay_minutes DOUBLE PRECISION,
  Bus_on_stop_count BIGINT,
  average_weighted_delay_minutes DOUBLE PRECISION,
  average_weighted_delay_seconds DOUBLE PRECISION,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  PRIMARY KEY (stop_id, window_start)
);

CREATE TABLE gold.delays_by_district (
  district TEXT,
  average_delay_minutes DOUBLE PRECISION,
  average_delay_seconds DOUBLE PRECISION,
  max_delay_minutes DOUBLE PRECISION,
  min_delay_minutes DOUBLE PRECISION,
  Bus_on_stop_count BIGINT,
  average_weighted_delay_minutes DOUBLE PRECISION,
  average_weighted_delay_seconds DOUBLE PRECISION,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  PRIMARY KEY (district, window_start)
);

CREATE TABLE gold.fleet (
  vehicle_number TEXT PRIMARY KEY,
  route_id TEXT,
  trip_id TEXT,
  gps_lat DOUBLE PRECISION,
  gps_lon DOUBLE PRECISION,
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
);

ALTER TABLE silver.trips
  ADD FOREIGN KEY (route_id) REFERENCES silver.routes(route_id);

ALTER TABLE silver.stop_times
  ADD FOREIGN KEY (trip_id) REFERENCES silver.trips(trip_id);

ALTER TABLE silver.stop_times
  ADD FOREIGN KEY (stop_id) REFERENCES silver.stops(stop_id);

ALTER TABLE silver.master_schedule
  ADD FOREIGN KEY (trip_id) REFERENCES silver.trips(trip_id);

ALTER TABLE silver.master_schedule
  ADD FOREIGN KEY (stop_id) REFERENCES silver.stops(stop_id);

ALTER TABLE silver.master_schedule
  ADD FOREIGN KEY (route_id) REFERENCES silver.routes(route_id);

ALTER TABLE silver.bus_live_feed
  ADD FOREIGN KEY (trip_id) REFERENCES silver.trips(trip_id);

ALTER TABLE silver.bus_matched
  ADD FOREIGN KEY (trip_id) REFERENCES silver.trips(trip_id);

ALTER TABLE gold.bus_delays
  ADD FOREIGN KEY (trip_id) REFERENCES silver.trips(trip_id);

ALTER TABLE gold.bus_delays
  ADD FOREIGN KEY (route_id) REFERENCES silver.routes(route_id);

ALTER TABLE gold.bus_delays
  ADD FOREIGN KEY (stop_id) REFERENCES silver.stops(stop_id);

ALTER TABLE gold.delays_by_stop
  ADD FOREIGN KEY (stop_id) REFERENCES silver.stops(stop_id);

ALTER TABLE gold.fleet
  ADD FOREIGN KEY (trip_id) REFERENCES silver.trips(trip_id);

ALTER TABLE gold.fleet
  ADD FOREIGN KEY (route_id) REFERENCES silver.routes(route_id);

ALTER TABLE gold.fleet
  ADD FOREIGN KEY (stop_id) REFERENCES silver.stops(stop_id);
