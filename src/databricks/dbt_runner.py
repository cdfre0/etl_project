# Databricks notebook source
# MAGIC %md
# MAGIC # DBT Runner
# MAGIC 
# MAGIC This notebook can be scheduled via Azure Data Factory to trigger the DBT run that builds the Gold Layer.
# MAGIC It assumes the DBT profile is configured on the cluster or via environment variables.

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

# 3. Create a secure environment mapping for dbt execution
secure_env = os.environ.copy()

import sys

print(f"Launching dbt run against host: {db_host}...")

# 4. Derive the absolute path of the dbt executable from the active environment
dbt_executable = os.path.join(os.path.dirname(sys.executable), "dbt")

# Trigger dbt securely with its absolute path
result = subprocess.run(
    [dbt_executable, "run", "--profiles-dir", "."],
    cwd=dbt_project_dir,
    env=secure_env,
    capture_output=True,
    text=True
)

# 5. Print the output gracefully
print(result.stdout)

if result.returncode != 0:
    print(result.stderr)
    raise Exception("DBT Run Failed!")

print("✅ DBT Models successfully built!")
