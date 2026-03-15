
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, to_date, current_timestamp, input_file_name
from datetime import date

# --- Configuration ---
BRONZE_BASE_PATH = "data/bronze/ceidg"
CURATED_TABLE_PATH = "data/curated/companies"

def create_spark_session() -> SparkSession:
    """Creates and returns a SparkSession with Delta Lake support."""
    return (
        SparkSession.builder
        .appName("CEIDG_Bronze_to_Curated")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def read_bronze_data(spark: SparkSession, processing_date: str) -> DataFrame:
    """
    Reads raw JSON data from the bronze layer for a specific processing date.
    
    Args:
        spark: The SparkSession object.
        processing_date: The date of the data to process (YYYY-MM-DD).
        
    Returns:
        A DataFrame containing the raw, nested data.
    """
    input_path = os.path.join(BRONZE_BASE_PATH, processing_date, "*.json")
    print(f"Reading raw data from: {input_path}")
    
    # Spark reads the JSON files, the main data is in the 'firmy' array
    df = spark.read.option("multiLine", "true").json(input_path)
    
    # Explode the 'firmy' array to get one row per company
    return df.select(explode(col("firmy")).alias("data"))

def transform_to_curated(df: DataFrame) -> DataFrame:
    """
    Transforms the raw DataFrame by flattening the structure, renaming columns
    to snake_case, casting data types, and adding audit columns.
    
    Args:
        df: The input DataFrame with raw company data.
        
    Returns:
        A transformed DataFrame ready to be saved in the curated layer.
    """
    print("Applying transformations: flattening, renaming, and type casting...")
    
    curated_df = df.select(
        col("data.id").alias("company_id"),
        col("data.nazwa").alias("company_name"),
        col("data.status").alias("status"),
        to_date(col("data.dataRozpoczecia"), "yyyy-MM-dd").alias("start_date"),
        
        # Owner details
        col("data.wlasciciel.imie").alias("owner_firstname"),
        col("data.wlasciciel.nazwisko").alias("owner_lastname"),
        col("data.wlasciciel.nip").alias("owner_nip"),
        col("data.wlasciciel.regon").alias("owner_regon"),
        
        # Address details
        col("data.adresDzialalnosci.ulica").alias("address_street"),
        col("data.adresDzialalnosci.budynek").alias("address_building"),
        col("data.adresDzialalnosci.miasto").alias("address_city"),
        col("data.adresDzialalnosci.kod").alias("address_postal_code"),
        col("data.adresDzialalnosci.wojewodztwo").alias("address_voivodeship"),
        col("data.adresDzialalnosci.powiat").alias("address_powiat"),
        col("data.adresDzialalnosci.gmina").alias("address_gmina"),
        col("data.adresDzialalnosci.terc").alias("address_terc_code"),
        col("data.adresDzialalnosci.simc").alias("address_simc_code"),
        col("data.adresDzialalnosci.ulic").alias("address_ulic_code"),
        
        # Other details
        col("data.link").alias("details_url")
    ).withColumn("_ingestion_date", current_timestamp()) \
     .withColumn("_source_file", input_file_name())

    return curated_df

def write_curated_data(df: DataFrame):
    """
    Writes the transformed DataFrame to the curated layer as a Delta table,
    partitioned by voivodeship.
    
    Args:
        df: The transformed DataFrame.
    """
    print(f"Writing data to Delta table: {CURATED_TABLE_PATH}")
    
    (
        df.write
        .format("delta")
        .partitionBy("address_voivodeship")
        .mode("overwrite")
        .option("overwriteSchema", "true") # Allows schema evolution
        .save(CURATED_TABLE_PATH)
    )
    print("Successfully wrote data to the curated layer.")

def main():
    """Main ETL function."""
    processing_date = date.today().isoformat()
    print(f"--- Starting Bronze to Curated transformation for date: {processing_date} ---")
    
    spark = create_spark_session()
    
    try:
        raw_df = read_bronze_data(spark, processing_date)
        transformed_df = transform_to_curated(raw_df)
        write_curated_data(transformed_df)
    except Exception as e:
        print(f"An error occurred during the transformation process: {e}")
    finally:
        spark.stop()
        
    print("--- Transformation finished ---")

if __name__ == "__main__":
    main()
