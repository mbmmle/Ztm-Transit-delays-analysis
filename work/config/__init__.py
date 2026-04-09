jdbc_url = "jdbc:postgresql://postgres:5432/Warsaw_Bus_DB"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver",
    "rewriteBatchedInserts": "true",
    "batchsize": "50000",
    "numPartitions": "16",
    "fetchsize": "10000"
}

DB_CONFIG = {
    "dbname": "Warsaw_Bus_DB",
    "user": "admin",
    "password": "admin",
    "host": "host.docker.internal",
    "port": "5432",
}
