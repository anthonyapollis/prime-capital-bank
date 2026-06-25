# Databricks notebook source
# Prime Capital Bank -- Metastore Setup
# Uses hive_metastore (default catalog, always available without extra configuration).
# Creates bronze/silver/gold/sandbox databases and pipeline control tables.

# COMMAND ----------

import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("metastore_setup")

SCHEMAS = ["bronze", "silver", "gold", "sandbox"]
print(f"Starting metastore setup -- {datetime.utcnow().isoformat()}Z")
print(f"Default catalog: {spark.catalog.currentCatalog()}")

# COMMAND ----------
# -- 1: Create databases (schemas) in hive_metastore

schema_comments = {
    "bronze":  "Raw ingested data",
    "silver":  "Cleansed and conformed data",
    "gold":    "Star schema dimensions and facts",
    "sandbox": "Experimental work",
}
for schema in SCHEMAS:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema} COMMENT '{schema_comments[schema]}'")
    log.info(f"Database '{schema}' ready.")

# COMMAND ----------
# -- 2: Create pipeline control table in bronze

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.pipeline_control (
        run_id        STRING,
        notebook_name STRING,
        table_name    STRING,
        run_status    STRING,
        rows_ingested LONG,
        error_message STRING,
        started_at    TIMESTAMP,
        completed_at  TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")
log.info("bronze.pipeline_control table ready.")

# COMMAND ----------
# -- 3: Create schema change log

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.schema_change_log (
        log_id      STRING,
        table_name  STRING,
        change_type STRING,
        column_name STRING,
        old_type    STRING,
        new_type    STRING,
        detected_at TIMESTAMP
    )
    USING DELTA
""")
log.info("bronze.schema_change_log table ready.")

# COMMAND ----------
# -- 4: Validate and print summary

dbs     = [r[0] for r in spark.sql("SHOW DATABASES").collect()]
tables  = [r[1] for r in spark.sql("SHOW TABLES IN bronze").collect()]

print(f"\n=== Metastore Validation ===")
print(f"Databases : {dbs}")
print(f"Bronze tables: {tables}")
print(f"\n00_setup_unity_catalog -- COMPLETE at {datetime.utcnow().isoformat()}Z")
