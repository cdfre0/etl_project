# Databricks notebook source
# MAGIC %md
# MAGIC # DBT Runner
# MAGIC 
# MAGIC This notebook can be scheduled via Azure Data Factory to trigger the DBT run that builds the Gold Layer.
# MAGIC It assumes the DBT profile is configured on the cluster or via environment variables.

# COMMAND ----------

# COMMAND ----------

# MAGIC %pip install dbt-core dbt-databricks databricks-sql-connector

# COMMAND ----------

import os
# 1. Grab the secure credentials from the cluster environment
db_host = os.environ.get("DBT_DATABRICKS_HOST")
db_token = os.environ.get("DATABRICKS_TOKEN")

if not db_host or not db_token:
    raise ValueError("CRITICAL: Databricks dbt variables (DBT_DATABRICKS_HOST, DATABRICKS_TOKEN) not found in the cluster environment!")

# 2. Configure dbt project location
dbt_project_dir = "../../dbt_project"

# 3. Prevent Databricks read-only workspace errors by forwarding dbt writes to ephemeral storage
os.environ["DBT_LOG_PATH"] = "/tmp/dbt_logs"
os.environ["DBT_TARGET_PATH"] = "/tmp/dbt_target"

print(f"Launching dbt run against host: {db_host}...")

from dbt.cli.main import dbtRunner, dbtRunnerResult

print(f"Launching dbt run programmatically against host: {db_host}...")

# 4. Trigger dbt cleanly through the natively attached Python programmatic API
dbt = dbtRunner()

result: dbtRunnerResult = dbt.invoke(
    ["run", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir]
)

# 4. Check for ungraceful failures
if not result.success:
    raise Exception("DBT Run Failed!")

print("✅ DBT Models successfully built!")
