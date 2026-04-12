# Databricks notebook source
# MAGIC %md
# MAGIC # SUDOP ETL: Bronze to Silver (PySpark)
# MAGIC 
# MAGIC This notebook reads raw JSON data from the Bronze ADLS container, cleans and conforms the data, and writes it out as Delta tables into the Silver container.
# MAGIC It ensures idempotency by overwriting the Delta tables on each run.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_date, input_file_name, regexp_extract
import os

# Configure ADLS Gen2 connection dynamically via widgets or fallback
try:
    dbutils.widgets.text("storage_account", "stetldatamedallion", "Storage Account Name")
    storage_account_name = dbutils.widgets.get("storage_account")
except Exception:
    # Fallback if run outside a standard Databricks interactive context
    storage_account_name = "stetldatamedallion"

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"

# Ensure the silver database exists in the metastore
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Process Cases (Przypadki Pomocy)

# COMMAND ----------

# Read JSON case files. For nested JSON with 'wyniki' array, we use explode.
from pyspark.sql.functions import explode

cases_raw_df = spark.read.option("multiline", "true").json(f"{bronze_path}cases/*.json")

if "wyniki" in cases_raw_df.columns:
    cases_df = cases_raw_df.select(explode(col("wyniki")).alias("wynik")).select("wynik.*")
    
    # Apply column renames based on metadata.json mapping
    rename_rules = {
        "nip-udzielajacego-pomocy": "nip_udzielajacego_pomocy", "nazwa-udzielajacego-pomocy": "nazwa_udzielajacego_pomocy",
        "srodek-pomocowy-numer": "srodek_pomocowy_numer", "srodek-pomocowy-nazwa": "srodek_pomocowy_nazwa",
        "podstawa-prawna-2a-kod": "podstawa_prawna_2a_kod", "podstawa-prawna-2a-nazwa": "podstawa_prawna_2a_nazwa",
        "podstawa-prawna-2b": "podstawa_prawna_2b", "podstawa-prawna-2c": "podstawa_prawna_2c",
        "podstawa-prawna-3a": "podstawa_prawna_3a", "podstawa-prawna-3b": "podstawa_prawna_3b",
        "symbol-aktu-ogolnego": "symbol_aktu_ogolnego", "dzien-udzielenia-pomocy": "dzien_udzielenia_pomocy",
        "nip-beneficjenta": "nip_beneficjenta", "nazwa-beneficjenta": "nazwa_beneficjenta",
        "wielkosc-beneficjenta-kod": "wielkosc_beneficjenta_kod", "wielkosc-beneficjenta-nazwa": "wielkosc_beneficjenta_nazwa",
        "sektor-dzialalnosci-kod": "sektor_dzialalnosci_kod", "sektor-dzialalnosci-wersja": "sektor_dzialalnosci_wersja",
        "sektor-dzialalnosci-nazwa": "sektor_dzialalnosci_nazwa", "gmina-siedziby-kod": "gmina_siedziby_kod",
        "gmina-siedziby-nazwa": "gmina_siedziby_nazwa", "przeznaczenie-pomocy-kod": "przeznaczenie_pomocy_kod",
        "przeznaczenie-pomocy-nazwa": "przeznaczenie_pomocy_nazwa", "forma-pomocy-kod": "forma_pomocy_kod",
        "forma-pomocy-nazwa": "forma_pomocy_nazwa", "wartosc-nominalna-pln": "wartosc_nominalna_pln",
        "wartosc-brutto-pln": "wartosc_brutto_pln", "wartosc-brutto-eur": "wartosc_brutto_eur",
    }
    
    for old_name, new_name in rename_rules.items():
        if old_name in cases_df.columns:
            cases_df = cases_df.withColumnRenamed(old_name, new_name)

    # Cast data types seamlessly
    cases_df = cases_df.withColumn("dzien_udzielenia_pomocy", to_date(col("dzien_udzielenia_pomocy")))
    cases_df = cases_df.withColumn("wartosc_nominalna_pln", col("wartosc_nominalna_pln").cast("double"))
    cases_df = cases_df.withColumn("wartosc_brutto_pln", col("wartosc_brutto_pln").cast("double"))
    cases_df = cases_df.withColumn("wartosc_brutto_eur", col("wartosc_brutto_eur").cast("double"))
    
    cases_df = cases_df.withColumn("_source_file", input_file_name())
    cases_df = cases_df.withColumn("_curated_at", current_timestamp())

    # Write idempotently to Delta and register as an external table
    print("Writing Cases to Silver Delta table...")
    cases_df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("path", f"{silver_path}przypadki_pomocy") \
      .saveAsTable("silver.przypadki_pomocy")
else:
    print("No valid cases found in Bronze layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Process Dictionaries (Slowniki)

# COMMAND ----------

# We extract dictionary names from the file names (e.g. slownik_gmina_siedziby_2024.json -> slownik_gmina_siedziby)
dict_raw_df = spark.read.option("multiline", "true").json(f"{bronze_path}dictionaries/*.json")

# Since the structure is [{"name": "...", "number": "..."}, ...], we handle it natively.
if "name" in dict_raw_df.columns and "number" in dict_raw_df.columns:
    dict_df = dict_raw_df \
        .withColumnRenamed("name", "nazwa") \
        .withColumnRenamed("number", "kod") \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_curated_at", current_timestamp())
    
    # We dynamically partition or map by file name
    # In Databricks, we can just process files via simple loop if there are different structures.
    # We will write all dictionaries out grouped by source name
    dict_df = dict_df.withColumn("dict_type", regexp_extract(col("_source_file"), r'(slownik_[^_]+_[^_]+)_', 1))
    
    dict_types = [row.dict_type for row in dict_df.select("dict_type").distinct().collect() if row.dict_type]
    
    for d_type in dict_types:
        d_df = dict_df.filter(col("dict_type") == d_type).drop("dict_type")
        print(f"Writing {d_type} to Silver Delta table...")
        # Write and register external table natively
        d_df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("path", f"{silver_path}{d_type}") \
          .saveAsTable(f"silver.{d_type}")

print("Bronze to Silver process complete.")
