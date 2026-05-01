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

# ---------------------------------------------------------------------------
# Read JSON case files — Kafka consumer format only.
#
# The consumer writes the full Kafka envelope per file:
#   {"type":"case","gmina_kod":"...","case":{...case fields with hyphen keys...}}
#
# Each file = one case. We unwrap the nested "case" struct and flatten it.
#
# NOTE: Photon's native SIMD JSON reader does NOT honour PERMISSIVE mode and
# will hard-crash on any malformed file. We disable Photon for just this read
# so the standard JVM reader handles bad records gracefully via
# columnNameOfCorruptRecord, then immediately re-enable it for the Delta write.
# ---------------------------------------------------------------------------

CORRUPT_COL = "_corrupt_record"

# Disable Photon so PERMISSIVE mode is handled by the JVM JSON reader
spark.conf.set("spark.databricks.photon.enabled", "false")

cases_raw_df = (
    spark.read
    .option("multiline", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", CORRUPT_COL)
    .json(f"{bronze_path}cases/*.json")
)

raw_case_count = cases_raw_df.count()
print(f"Bronze case rows read: {raw_case_count}")

# Filter out corrupt / partially-written records (lazy — no Spark action yet)
if CORRUPT_COL in cases_raw_df.columns:
    corrupt_case_count = cases_raw_df.filter(col(CORRUPT_COL).isNotNull()).count()
    print(f"Corrupt / partial bronze case rows dropped: {corrupt_case_count}")
    cases_raw_df = cases_raw_df.filter(col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
else:
    print("No corrupt-record column present in bronze case input.")

# Re-enable Photon for all downstream transforms and the Delta write
spark.conf.set("spark.databricks.photon.enabled", "true")

# Unwrap the nested "case" struct → flat top-level columns
case_struct_cols = [f"case.{f.name}" for f in cases_raw_df.schema["case"].dataType.fields]
cases_df = cases_raw_df.select([col(c).alias(c.replace("case.", "")) for c in case_struct_cols])
print(f"Silver case rows after cleanup: {cases_df.count()}")

# Rename hyphen-keyed API fields to underscore equivalents
rename_rules = {
    "nip-udzielajacego-pomocy": "nip_udzielajacego_pomocy",
    "nazwa-udzielajacego-pomocy": "nazwa_udzielajacego_pomocy",
    "srodek-pomocowy-numer": "srodek_pomocowy_numer",
    "srodek-pomocowy-nazwa": "srodek_pomocowy_nazwa",
    "podstawa-prawna-2a-kod": "podstawa_prawna_2a_kod",
    "podstawa-prawna-2a-nazwa": "podstawa_prawna_2a_nazwa",
    "podstawa-prawna-2b": "podstawa_prawna_2b",
    "podstawa-prawna-2c": "podstawa_prawna_2c",
    "podstawa-prawna-3a": "podstawa_prawna_3a",
    "podstawa-prawna-3b": "podstawa_prawna_3b",
    "symbol-aktu-ogolnego": "symbol_aktu_ogolnego",
    "dzien-udzielenia-pomocy": "dzien_udzielenia_pomocy",
    "nip-beneficjenta": "nip_beneficjenta",
    "nazwa-beneficjenta": "nazwa_beneficjenta",
    "wielkosc-beneficjenta-kod": "wielkosc_beneficjenta_kod",
    "wielkosc-beneficjenta-nazwa": "wielkosc_beneficjenta_nazwa",
    "sektor-dzialalnosci-kod": "sektor_dzialalnosci_kod",
    "sektor-dzialalnosci-wersja": "sektor_dzialalnosci_wersja",
    "sektor-dzialalnosci-nazwa": "sektor_dzialalnosci_nazwa",
    "gmina-siedziby-kod": "gmina_siedziby_kod",
    "gmina-siedziby-nazwa": "gmina_siedziby_nazwa",
    "przeznaczenie-pomocy-kod": "przeznaczenie_pomocy_kod",
    "przeznaczenie-pomocy-nazwa": "przeznaczenie_pomocy_nazwa",
    "forma-pomocy-kod": "forma_pomocy_kod",
    "forma-pomocy-nazwa": "forma_pomocy_nazwa",
    "wartosc-nominalna-pln": "wartosc_nominalna_pln",
    "wartosc-brutto-pln": "wartosc_brutto_pln",
    "wartosc-brutto-eur": "wartosc_brutto_eur",
}

for old, new in rename_rules.items():
    if old in cases_df.columns:
        cases_df = cases_df.withColumnRenamed(old, new)


# Guard: if we ended up with an empty or unrecognisable frame, bail gracefully
EXPECTED_COLS = {"nip_beneficjenta", "wartosc_brutto_pln", "dzien_udzielenia_pomocy"}
if not EXPECTED_COLS.intersection(set(cases_df.columns)):
    print("WARNING: No recognisable case columns found in Bronze layer. Skipping cases write.")
else:
    # Cast data types
    cases_df = cases_df.withColumn("dzien_udzielenia_pomocy", to_date(col("dzien_udzielenia_pomocy")))
    cases_df = cases_df.withColumn("wartosc_nominalna_pln", col("wartosc_nominalna_pln").cast("double"))
    cases_df = cases_df.withColumn("wartosc_brutto_pln", col("wartosc_brutto_pln").cast("double"))
    cases_df = cases_df.withColumn("wartosc_brutto_eur", col("wartosc_brutto_eur").cast("double"))

    cases_df = cases_df.withColumn("_source_file", input_file_name())
    cases_df = cases_df.withColumn("_curated_at", current_timestamp())

    print(f"Writing {cases_df.count()} case records to Silver Delta table...")
    cases_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", f"{silver_path}przypadki_pomocy") \
        .saveAsTable("silver.przypadki_pomocy")

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
