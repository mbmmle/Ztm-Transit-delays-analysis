# Database schema documentation — `silver` and `gold`

This document provides a short description of the tables used in the bus data pipeline. It covers the *silver* schema (processed GTFS and live feed data) and the *gold* schema (computed delays and aggregations).

## Schema: silver

### silver.routes
- **Description**: Route metadata (GTFS `routes`).
- **Columns**:
  - **route_id**: TEXT — unique route identifier (PK).
  - **agency_id**: INTEGER — operator/agency identifier.
  - **route_name**: TEXT — route name.
  - **route_desc**: TEXT — route description.
  - **route_type**: INTEGER — route type (GTFS `route_type`).

### silver.trips
- **Description**: Trips assigned to routes (GTFS `trips`).
- **Columns**:
  - **trip_id**: TEXT — trip identifier (PK).
  - **route_id**: TEXT — route identifier (FK → `silver.routes.route_id`).
  - **service_id**: TEXT — service/calendar identifier (GTFS `service_id`).
  - **trip_headsign**: TEXT — headsign or displayed destination.
  - **direction_id**: INTEGER — direction (e.g., 0/1).

### silver.stops
- **Description**: Stop definitions.
- **Columns**:
  - **stop_id**: INTEGER — stop identifier (PK).
  - **stop_code**: TEXT — external stop code (optional).
  - **stop_name**: TEXT — stop name.
  - **stop_lat**: REAL — latitude.
  - **stop_lon**: REAL — longitude.
  - **district**: TEXT — district or area.

### silver.stop_times
- **Description**: Sequence of stops for a trip (GTFS `stop_times`).
- **Columns**:
  - **trip_id**: TEXT — reference to the trip (FK → `silver.trips.trip_id`).
  - **stop_id**: INTEGER — reference to the stop (FK → `silver.stops.stop_id`).
  - **stop_sequence**: INTEGER — order of the stop within the trip.
  - **pickup_type**: INTEGER — GTFS `pickup_type`.
  - **drop_off_type**: INTEGER — GTFS `drop_off_type`.
  - **shape_dist_traveled**: REAL — distance along the shape.
  - **arrival_time**: TIMESTAMP — scheduled arrival time.
  - **departure_time**: TIMESTAMP — scheduled departure time.
- **PK**: `(trip_id, stop_sequence)`.

### silver.master_schedule
- **Description**: Unified schedule (joined data from routes/trips/stop_times) used for matching GPS points to the planned schedule.
- **Columns**:
  - **schedule_id**: TEXT — schedule record identifier (PK).
  - **trip_id**: TEXT — FK → `silver.trips`.
  - **stop_id**: INTEGER — FK → `silver.stops`.
  - **route_id**: TEXT — FK → `silver.routes`.
  - **route_name**, **stop_name**, **district** — denormalized convenience fields.
  - **stop_sequence**: INTEGER, **stop_lat/stop_lon**, **arrival_time**, **departure_time**.

### silver.bus_live_feed
- **Description**: Raw GPS / vehicle telemetry feed.
- **Columns**:
  - **gps_id**: TEXT — GPS record identifier (PK).
  - **trip_id**: TEXT — (if available) assigned trip (FK → `silver.trips`).
  - **lat**, **lon**: DOUBLE PRECISION — GPS position.
  - **vehicle_number**: TEXT — vehicle identifier.
  - **time_gps**: TIMESTAMP — GPS timestamp.
  - **etl_timestamp**: TIMESTAMP — ingestion/ETL timestamp.

### silver.bus_matched
- **Description**: Result of matching GPS points to the schedule — enriched records (matched stop, distances, movement flags, upcoming stops).
- **Selected columns**:
  - **gps_id**, **vehicle_number**, **trip_id**, **route_id**.
  - **lat**, **lon**, **time_gps** — in the original file stored as TEXT.
  - **stop_id**, **arrival_time**, **stop_sequence**, **stop_lat**, **stop_lon** — matched stop and schedule data.
  - **distance_meters**, **moved_meters_last3**, **is_moving_last3** — movement metrics.
  - **next_stop_1_id..next_stop_N_sequence** — upcoming stop information.
  Amount of next stops depends on PARAMEER `NEXT_STOP_COUNT` used in the matching process.


## Schema: gold

### gold.bus_delays
- **Description**: Delay records — GPS points matched to schedule, with time differences and schedule attributes.
- **Selected columns**:
  - **gps_id**, **trip_id** (FK → `silver.trips`).
  - **gps_lat**, **gps_lon**, **time_gps** — position and measurement time.
  - **vehicle_number**.
  - **time_diff_seconds**, **abs_time_diff_seconds** — time differences in seconds.
  - **delay_seconds** (BIGINT), **delay_minutes** — computed delay.
  - **schedule_id**, **stop_id** (FK → `silver.stops`), **route_id** (FK → `silver.routes`).
  - **route_name**, **stop_name**, **district**, **stop_sequence**, **stop_lat/stop_lon**.
  - **arrival_time**, **departure_time** — scheduled times.
  - **distance** — distance in meters.
  - **gold_timestamp** — record generation timestamp.
- **PK**: `(gps_id, stop_id, time_gps)`.

### gold.delays_by_stop
- **Description**: Aggregated delay metrics per stop over time windows.
- **Columns**:
  - **stop_id**, **stop_name**, **stop_lat/stop_lon**, **district**.
  - **average_delay_minutes**, **average_delay_seconds**, **max_delay_minutes**, **min_delay_minutes**.
  - **Bus_on_stop_count** — number of measurements / vehicles observed at the stop.
  - **average_weighted_delay_minutes/seconds** — weighted delay metrics, for first 3 stops of bus sequence the weight is 0.5, otherwise 1.
  - **window_start**, **window_end** — aggregation window, in UTC time.
- **PK**: `(stop_id, window_start)`.

### gold.delays_by_district
- **Description**: Aggregated delay metrics per district (analogous to `delays_by_stop`).

### gold.fleet
- **Description**: Current snapshot of fleet state (vehicle assignments, last position, latest delay).
- **Columns**:
  - **vehicle_number**: TEXT — vehicle identifier (PK).
  - **route_id**, **trip_id** (FK → silver).
  - **gps_lat/gps_lon**, **time_gps**.
  - **window_start**, **window_end** — snapshot aggregation window.
  - **stop_id**, **stop_name**, **stop_district**, **stop_sequence**.
  - **delay_seconds**, **delay_minutes**, **is_delayed** (BOOLEAN).

## Relationships (foreign keys) — summary
- `silver.trips.route_id` → `silver.routes(route_id)`
- `silver.stop_times.trip_id` → `silver.trips(trip_id)`
- `silver.stop_times.stop_id` → `silver.stops(stop_id)`
- `silver.master_schedule.trip_id` → `silver.trips(trip_id)`
- `silver.master_schedule.stop_id` → `silver.stops(stop_id)`
- `silver.master_schedule.route_id` → `silver.routes(route_id)`
- `silver.bus_live_feed.trip_id` → `silver.trips(trip_id)`
- `silver.bus_matched.trip_id` → `silver.trips(trip_id)`
- `gold.bus_delays.trip_id` → `silver.trips(trip_id)`
- `gold.bus_delays.route_id` → `silver.routes(route_id)`
- `gold.bus_delays.stop_id` → `silver.stops(stop_id)`
- `gold.delays_by_stop.stop_id` → `silver.stops(stop_id)`
- `gold.fleet.trip_id` → `silver.trips(trip_id)`
- `gold.fleet.route_id` → `silver.routes(route_id)`
- `gold.fleet.stop_id` → `silver.stops(stop_id)`

## Notes and recommendations
- `bronze` is raw layer ;`silver`is processed layer; `gold` is the enriched and aggregated layer — design ETL processes to avoid overwriting raw data.
---
Generated automatically from `docs/lucidchart/silver_gold_schema.sql` and `docs/lucidchart/gold_schema.sql`. Rewieved by hand for accuracy.

