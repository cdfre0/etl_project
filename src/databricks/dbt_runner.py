# Databricks notebook source
# MAGIC %md
# MAGIC # DBT Runner (Restart-Aware)
# MAGIC 
# MAGIC This notebook builds the Gold Layer.
# MAGIC 1. Installs dependencies.
# MAGIC 2. Restarts Python to refresh the environment.
# MAGIC 3. Executes dbt programmatically.

# COMMAND ----------

# MAGIC %pip install dbt-core dbt-databricks databricks-sql-connector

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment Refresh
# MAGIC We must restart Python for the kernel to see 'databricks.sql'.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

# 1. Setup Environment Variables (Must be done AFTER restart)
os.environ["DBT_LOG_PATH"] = "/tmp/dbt_logs"
os.environ["DBT_TARGET_PATH"] = "/tmp/dbt_target"

# Ensure host/token are available from the cluster env
db_host = os.environ.get("DBT_DATABRICKS_HOST")
db_token = os.environ.get("DATABRICKS_TOKEN")

if not db_host or not db_token:
    raise ValueError("CRITICAL: Missing Cluster Environment Variables (DBT_DATABRICKS_HOST, DATABRICKS_TOKEN)!")

# 2. Configure project paths
dbt_project_dir = "../../dbt_project"
storage_account_name = "stetldatamedallion" 

# 3. Explicitly initialize the gold schema in metastore to point to ADLS
# This prevents DELTA_TABLE_LOCATION_MISMATCH by defining the location at the schema level
gold_location = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold LOCATION '{gold_location}'")

print(f"Launching dbt run against host: {db_host}...")

from dbt.cli.main import dbtRunner, dbtRunnerResult

# 3. Trigger dbt cleanly through the natively attached Python programmatic API
dbt = dbtRunner()

# Explicitly pass the project directory to both configurations
result: dbtRunnerResult = dbt.invoke(
    ["run", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir]
)

# 4. Check for ungraceful failures
if not result.success:
    raise Exception("DBT Run Failed! Check the console output above for details.")

print("✅ DBT Models successfully built!")
