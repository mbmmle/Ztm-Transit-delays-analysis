'''mermaid
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
	ROUTES||--o{TRIPS:"1:N route_id"
	ROUTES||--o{MASTER_SCHEDULE:"1:N route_id"
	TRIPS||--o{STOP_TIMES:"1:N trip_id"
	TRIPS||--o{MASTER_SCHEDULE:"1:N trip_id"
	STOPS||--o{STOP_TIMES:"1:N stop_id"
	STOPS||--o{MASTER_SCHEDULE:"1:N stop_id"
	STOP_TIMES}|--|{MASTER_SCHEDULE:"trip_id: trip_id"
'''
