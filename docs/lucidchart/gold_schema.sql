-- Lucidchart ERD import: GOLD schema (logical model)

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

CREATE TABLE gold.bus_delays_test_fix (
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
