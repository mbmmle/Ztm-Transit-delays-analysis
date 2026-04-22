"""
Shared runtime configuration for batch and streaming scripts.

This module centralizes connection settings for PostgreSQL and Spark-related
JDBC options used across Bronze, Silver, and Gold jobs. Values are intentionally
simple and environment-driven where needed (for example DB host) so the same
code can run in local and containerized environments.
"""

import os
DB_HOST = os.getenv("DB_HOST", "localhost")
jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/Warsaw_Bus_DB"

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
    "host": DB_HOST,
    "port": "5432",
}
SPARK_MEMORY = "3g"
