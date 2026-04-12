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

# 2. Configure dbt project location
dbt_project_dir = "../../dbt_project"

# 3. Create a secure environment mapping for dbt execution
secure_env = os.environ.copy()
secure_env["DBT_LOG_PATH"] = "/tmp/dbt_logs"
secure_env["DBT_TARGET_PATH"] = "/tmp/dbt_target"

print(f"Launching dbt run against host: {db_host}...")

# 4. Trigger dbt through the resilient Cluster Base Environment by sourcing Databricks profiles natively
bash_script = f"""
source /etc/profile
dbt run --profiles-dir {dbt_project_dir} --project-dir {dbt_project_dir}
"""

result = subprocess.run(
    bash_script,
    shell=True,
    executable="/bin/bash",
    env=secure_env,
    capture_output=True,
    text=True
)

print(result.stdout)

# 4. Check for ungraceful failures
if result.returncode != 0:
    print(result.stderr)
    raise Exception("DBT Run Failed!")

print("✅ DBT Models successfully built!")
