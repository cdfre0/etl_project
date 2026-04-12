# Databricks notebook source
# MAGIC %md
# MAGIC # DBT Runner
# MAGIC 
# MAGIC This notebook can be scheduled via Azure Data Factory to trigger the DBT run that builds the Gold Layer.
# MAGIC It assumes the DBT profile is configured on the cluster or via environment variables.

# COMMAND ----------

# MAGIC %pip install dbt-core dbt-databricks

# COMMAND ----------

import os
import subprocess

# 1. Grab the secure credentials from the cluster environment
db_host = os.environ.get("DBT_DATABRICKS_HOST")
db_token = os.environ.get("DATABRICKS_TOKEN")

if not db_host or not db_token:
    raise ValueError("CRITICAL: Databricks dbt variables (DBT_DATABRICKS_HOST, DATABRICKS_TOKEN) not found in the cluster environment!")

# 2. Navigate to the dbt root folder (Python handles relative paths accurately)
# In standard repos, notebook runs in its folder.
dbt_project_dir = "../../dbt_project"

from dbt.cli.main import dbtRunner, dbtRunnerResult

print(f"Launching dbt run programmatically against host: {db_host}...")

# 3. Trigger dbt cleanly through the natively attached Python package programmatic API
dbt = dbtRunner()
result: dbtRunnerResult = dbt.invoke(["run", "--profiles-dir", ".", "--project-dir", "."], 
                                     project_dir=dbt_project_dir)

# 4. Check for ungraceful failures
if not result.success:
    raise Exception("DBT Run Failed!")

print("✅ DBT Models successfully built!")
