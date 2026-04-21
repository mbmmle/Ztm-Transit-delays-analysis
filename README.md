# Transit Delay Analysis - Warsaw Buses and Trams

##  Project Overview
This project focuses on creating a working data pipeline, capable of connecting live GPS data with the ZTM Warsaw schedule, to calculate delays for buses and trams in Warsaw. The delays are displayed and analyzed on real-time dashboards, providing on-map visualizations of delays by stop, as well as a fleet map showing the locations of vehicles and their delay status. 

---

## How to Use This Repository
1. Clone the repository and navigate to the project directory.
```bash
   git clone https://github.com/mbmmle/Ztm-Transit-delays-analysis.git
```
2. Use docker compose in cloned folder to build and run the containers:
```bash
   docker-compose up --build
```
3. Access Airflow UI at `http://localhost:8080` to monitor DAGs and trigger them manually all 3:
`warsaw_master_schedule_dag.py`, `warsaw_buses_live_dag.py`, `warsaw_minute_aggregation_dag.py`.
4. Open `visualizations.pbix` to view real-time visualizations. You may need to login to postgresql server to access the database, in order for visualizations to work.
#### Credentials:
   * Server: `localhost`
   * Database: `Warsaw_Bus_DB`
   * Username: `admin`
   * Password: `admin`
---

**WARNING:** Notebooks in work directory are deprecated, scripts are working app components.

---

##  Tech Stack
* **Docker** - for containerization
* **PostgreSQL** - for DB storage and Power BI DirectQuery streaming
* **Spark / PySpark** - for processing large GTFS data and aggregations
* **pandas** - for matching data and calculating delays
* **Geopandas** - for district mapping
* **SQLAlchemy** - database engine for pandas
* **Airflow** - for scheduling and orchestrating data pipelines
* **Power BI** - for real-time dashboards and visualizations

---

##  Dashboards and Maps

### Individual Stop Delay Map
<p align="center">
  <img src="docs/Images/Stops.jpg" alt="Delays by Stop Dashboard" width="600"/>
</p>

### Heatmap Delay Map
<p align="center">
  <img src="docs/Images/Heatmap.jpg" alt="Delays by District Dashboard" width="600"/>
</p>

### Fleet Map
<p align="center">
  <img src="docs/Images/Fleet.jpg" alt="Fleet Map Dashboard" width="600"/>
</p>

---

##  Data Architecture
The database is built with PostgreSQL using the Medallion architecture, consisting of three layers:
* **Bronze:** Raw data layer, storing unprocessed GTFS files.
* **Silver:** Processed data layer, storing cleaned and structured GTFS data, live feed data, and matched data.
* **Gold:** Data with business logic applied, storing calculated delays and aggregations by stop and district.

<details = "ER Diagram">
<summary><b>Silver_GTFS Data Storage Diagram </b></summary>

```mermaid
---
config:
  layout: elk
---
erDiagram
	direction TB
	ROUTES {
		TEXT route_id PK ""  
		INTEGER agency_id  ""  
		TEXT route_name  ""  
		TEXT route_desc  ""  
		INTEGER route_type  ""  
	}
	TRIPS {
		TEXT trip_id PK ""  
		TEXT route_id FK ""  
		TEXT service_id  ""  
		TEXT trip_headsign  ""  
		INTEGER direction_id  ""  
	}
	STOPS {
		INTEGER stop_id PK ""  
		TEXT stop_code  ""  
		TEXT stop_name  ""  
		REAL stop_lat  ""  
		REAL stop_lon  ""  
		TEXT district  ""  
	}
	STOP_TIMES {
		TEXT trip_id FK ""  
		INTEGER stop_id FK ""  
		INTEGER stop_sequence  ""  
		INTEGER pickup_type  ""  
		INTEGER drop_off_type  ""  
		REAL shape_dist_traveled  ""  
		TIMESTAMP arrival_time  ""  
		TIMESTAMP departure_time  ""  
	}
	MASTER_SCHEDULE {
		TEXT schedule_id PK ""  
		TEXT trip_id FK ""  
		INTEGER stop_id FK ""  
		TEXT route_id FK ""  
		TEXT route_name  ""  
		TEXT stop_name  ""  
		TEXT district  ""  
		INTEGER stop_sequence  ""  
		REAL stop_lat  ""  
		REAL stop_lon  ""  
		TIMESTAMP arrival_time  ""  
		TIMESTAMP departure_time  ""  
	}

	ROUTES||--o{TRIPS:"route_id"
	ROUTES||--o{MASTER_SCHEDULE:"route_id"
	TRIPS||--o{STOP_TIMES:"trip_id"
	TRIPS||--o{MASTER_SCHEDULE:"trip_id"
	STOPS||--o{STOP_TIMES:"stop_id"
	STOPS||--o{MASTER_SCHEDULE:"stop_id"
	STOP_TIMES}|--|{MASTER_SCHEDULE:"trip_id"
```

</details>

<details = "ER Diagram">
<summary><b>GPS and Delays Data Storage Diagram</b></summary>

```mermaid
%%{init: {"layout": "elk"}}%%
erDiagram
  BUS_LIVE_FEED {
    TEXT gps_id PK
    TEXT trip_id
    DOUBLE lat
    DOUBLE lon
    TEXT vehicle_number
    TIMESTAMP time_gps
    TIMESTAMP etl_timestamp
  }

  BUS_MATCHED {
    TEXT gps_id
    TEXT vehicle_number
    TEXT trip_id
    TEXT route_id
    TEXT lat
    TEXT lon
    TEXT time_gps
    TEXT stop_id
    TEXT arrival_time
    TEXT stop_sequence
    TEXT stop_lat
    TEXT stop_lon
    TEXT distance_meters
    TEXT moved_meters_last3
    TEXT is_moving_last3
	TEXT next_stop_N_id
	TEXT next_stop_N_sequence
  }

  BUS_DELAYS {
    TEXT gps_id PK
    TEXT trip_id
    BIGINT stop_id
    TEXT route_id
    TEXT route_name
    TEXT stop_name
    TEXT district
    DOUBLE gps_lat
    DOUBLE gps_lon
    TIMESTAMP time_gps PK
    BIGINT delay_seconds
    DOUBLE delay_minutes
    DOUBLE distance
    TIMESTAMP gold_timestamp
  }

  DELAYS_BY_STOP {
    BIGINT stop_id PK
    TEXT stop_name
    DOUBLE stop_lat
    DOUBLE stop_lon
    TEXT district
    DOUBLE average_delay_minutes
    DOUBLE average_delay_seconds
    DOUBLE max_delay_minutes
    DOUBLE min_delay_minutes
    BIGINT Bus_on_stop_count
    TIMESTAMP window_start PK
    TIMESTAMP window_end
  }

  DELAYS_BY_DISTRICT {
    TEXT district PK
    DOUBLE average_delay_minutes
    DOUBLE average_delay_seconds
    DOUBLE max_delay_minutes
    DOUBLE min_delay_minutes
    BIGINT Bus_on_stop_count
    TIMESTAMP window_start PK
    TIMESTAMP window_end
  }

  %% Color setup
  classDef teal stroke:#2dd4bf,fill:#f0fdfa
  classDef indigo stroke:#818cf8,fill:#eef2ff
  classDef orange stroke:#fb923c,fill:#fff7ed
  classDef gold stroke:#facc15,fill:#fefce8

  class BUS_LIVE_FEED, BUS_MATCHED indigo
  class BUS_DELAYS, DELAYS_BY_STOP, DELAYS_BY_DISTRICT gold
```

</details>

For more insight on database schema, see [database_schema.md](docs/database_schema.md).

---

## Data Pipeline
Orchestrated with Airflow, the data pipeline consists of three main DAGs:
1. `warsaw_master_schedule_dag.py` - responsible for ingesting and processing GTFS data to populate the `silver` schema with schedule and stop information. Interval: everyday at 2:00 AM.
1. `warsaw_buses_live_dag.py` - responsible for ingesting live GPS data, matching it to the schedule, and calculating delays. Interval: every minute.
2. `warsaw_minute_aggregation_dag.py` - responsible for aggregating delays by stop and district on a minute-level basis for dashboard visualizations. Interval: every minute watits for `warsaw_buses_live_dag.py` to finish.

### DAG Dependencies and Flow (LIVE FEED + DELAYS)

```mermaid
---
config:
  layout: dagre
---
flowchart TB
 subgraph SILVER["warsaw_buses_live_dag.py"]
		DB1[("PostgreSQL Instance<br>Warsaw_Bus_DB")]
        Net(["🌐 Internet (GPS API)"])
        A["Silver_Bus_Live_Feed.py"]
        B["Silver_Bus_Matched.py"]
        D["silver.Stop_Times"]
        X["silver.Stops"]
        C["silver.Master_Schedule"]
        E["Bus Delays (Per Trip & Stop)"]
        I["gold.Bus_Delays"]
        n1["silver.Bus_Live_Feed"]
        n2["silver.Bus_Matched"]
  end
 subgraph GOLD["warsaw_minute_aggregation_dag.py"]
        F["Gold_Delays_By_District.py"]
        s1["Gold_Delays_By_Stop.py"]
        s2["gold.Delays_By_District"]
        s3["gold.Delays_By_Stop"]
  end
        
    Net -- Raw GPS Data --> A
    C -. Schedule Data .-> E
    E -- Calculation of delays to DB --> I
    D -. Stop_Times Data .-> B
    I -. Historical Data .-> B
    X -. Stops Data .-> B
    A -- Formated Data to DB --> n1
    n1 -. Buses and Trams to Match .-> B
    B -- Save matches  to calculate Delay --> n2
    n2 -. Matches to calculate Delays .-> E
    I -. Delays Data .-> F & s1
    F -- Agreggations by District to DB --> s2
    s1 -- Agreggations by Stop to DB --> s3
    DB1 --> D & X & C

    n1@{ shape: rect}
    n2@{ shape: rect}
     Net:::violet
     A:::violet
     B:::violet
     D:::teal
     X:::teal
     C:::sky
     E:::orange
     I:::gold
     n1:::indigo
     n2:::indigo
     F:::orange
     s1:::orange
     s2:::gold
     s3:::gold
     DB1:::teal
    classDef violet stroke:#a78bfa
    classDef indigo stroke:#818cf8
    classDef teal stroke:#2dd4bf
    classDef orange stroke:#fb923c
    classDef sky stroke:#38bdf8
    classDef gold stroke:#facc15
```

---

## Data Sources
* **warszawa-dzielnice.geojson** - for mapping stops to districts
Source: https://github.com/andilabs/warszawa-dzielnice-geojson
* **GTFS data and live GPS data** - for schedule and route information
Source: https://mkuran.pl/gtfs/

---
