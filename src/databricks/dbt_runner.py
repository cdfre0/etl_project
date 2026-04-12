# Databricks notebook source
# MAGIC %md
# MAGIC # DBT Runner
# MAGIC 
# MAGIC This notebook can be scheduled via Azure Data Factory to trigger the DBT run that builds the Gold Layer.
# MAGIC It assumes the DBT profile is configured on the cluster or via environment variables.

# COMMAND ----------

# MAGIC %sh
# MAGIC # Navigate to the dbt project directory
# MAGIC cd /Workspace/Users/s36436@pjwstk.edu.pl/etl_project/dbt_project
# MAGIC 
# MAGIC # Install specific dbt adapter for databricks if needed
# MAGIC # pip install dbt-databricks
# MAGIC 
# MAGIC # Run dbt
# MAGIC dbt run --profiles-dir .
