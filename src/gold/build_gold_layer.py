#!/usr/bin/env python3
"""
Reads curated data from the silver container, builds a star schema,
and saves the final dimension and fact tables to the gold container.
"""
import os
import io
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# --- CONFIGURATION ---

CONFIG = {
    "silver_container": "silver",
    "gold_container": "gold",
}

# --- LOGGING SETUP ---

def setup_logging():
    """Configures logging for the script."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('azure.storage.filedatalake').setLevel(logging.WARNING)

# --- AZURE DATA LAKE HELPER FUNCTIONS ---

def get_adls_client():
    """Authenticates and returns a DataLakeServiceClient."""
    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set.")
    logging.info("ADLS service client created successfully.")
    return DataLakeServiceClient.from_connection_string(connection_string)

def read_parquet_from_adls(service_client: DataLakeServiceClient, container: str, path: str) -> pd.DataFrame | None:
    """Reads a Parquet file from ADLS into a pandas DataFrame."""
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        file_client = fs_client.get_file_client(path)
        
        download = file_client.download_file()
        file_bytes = download.readall()
        
        table = pq.read_table(io.BytesIO(file_bytes))
        logging.info(f"Successfully read {container}/{path}")
        return table.to_pandas()
    except Exception as e:
        logging.error(f"Error reading Parquet file {container}/{path}: {e}")
        return None

def save_df_as_parquet_to_adls(service_client: DataLakeServiceClient, container: str, table_name: str, df: pd.DataFrame):
    """Saves a DataFrame as a Parquet file to the specified container."""
    if df.empty:
        logging.warning(f"DataFrame for {table_name} is empty, skipping save.")
        return
        
    file_path = f"{table_name}/{table_name}.parquet"
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        directory_path = Path(file_path).parent.as_posix()
        fs_client.create_directory(directory_path)
            
        file_client = fs_client.get_file_client(file_path)
        
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        
        file_client.upload_data(buf.getvalue(), overwrite=True)
        logging.info(f"Successfully saved {file_path} to container '{container}'.")
    except Exception as e:
        logging.error(f"Error saving {file_path} to ADLS: {e}")

# --- GOLD LAYER TRANSFORMATION LOGIC ---

def create_dimension_table(df: pd.DataFrame, key_columns: list, name: str) -> pd.DataFrame:
    """Creates a dimension table with a surrogate key."""
    dim_df = df[key_columns].drop_duplicates().reset_index(drop=True)
    dim_df[f'{name}_id'] = dim_df.index
    return dim_df

def create_date_dimension(dates: pd.Series) -> pd.DataFrame:
    """Creates a date dimension table from a series of dates."""
    unique_dates = dates.dropna().unique()
    dim_date = pd.DataFrame(data=unique_dates, columns=['date'])
    dim_date['date'] = pd.to_datetime(dim_date['date'])
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['day_of_week'] = dim_date['date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['date'].dt.weekday >= 5
    dim_date.rename(columns={'date': 'full_date'}, inplace=True)
    dim_date['date_id'] = dim_date.index
    return dim_date

# --- MAIN WORKFLOW ---

def main():
    """Main function to build the gold layer."""
    setup_logging()
    adls_client = get_adls_client()
    
    # 1. Read curated data from Silver
    logging.info("--- Reading data from Silver Layer ---")
    cases_df = read_parquet_from_adls(adls_client, CONFIG['silver_container'], 'przypadki_pomocy/przypadki_pomocy.parquet')
    gmina_df = read_parquet_from_adls(adls_client, CONFIG['silver_container'], 'slowniki/slownik_gmina_siedziby/slownik_gmina_siedziby.parquet')

    if cases_df is None:
        logging.critical("Could not read the main cases table. Aborting.")
        return

    # 2. Create Dimension Tables
    logging.info("--- Creating Dimension Tables ---")
    dim_beneficjent = create_dimension_table(cases_df, ['nip_beneficjenta', 'nazwa_beneficjenta', 'wielkosc_beneficjenta_nazwa'], 'beneficjent')
    dim_udzielajacy_pomocy = create_dimension_table(cases_df, ['nip_udzielajacego_pomocy', 'nazwa_udzielajacego_pomocy'], 'udzielajacy_pomocy')
    dim_charakterystyka = create_dimension_table(cases_df, ['forma_pomocy_nazwa', 'przeznaczenie_pomocy_nazwa', 'sektor_dzialalnosci_nazwa'], 'charakterystyka')
    
    # Rename gmina columns for clarity and add surrogate key
    if gmina_df is not None:
        gmina_df.rename(columns={'kod': 'gmina_kod', 'nazwa': 'gmina_nazwa'}, inplace=True)
        gmina_df['geografia_id'] = gmina_df.index
    
    dim_data = create_date_dimension(cases_df['dzien_udzielenia_pomocy'])
    dim_data.rename(columns={'full_date': 'dzien_udzielenia_pomocy'}, inplace=True)

    # 3. Build Fact Table
    logging.info("--- Building Fact Table ---")
    fact_df = cases_df.copy()

    # Merge with dimensions to get surrogate keys
    fact_df = fact_df.merge(dim_beneficjent, on=['nip_beneficjenta', 'nazwa_beneficjenta', 'wielkosc_beneficjenta_nazwa'], how='left')
    fact_df = fact_df.merge(dim_udzielajacy_pomocy, on=['nip_udzielajacego_pomocy', 'nazwa_udzielajacego_pomocy'], how='left')
    fact_df = fact_df.merge(dim_charakterystyka, on=['forma_pomocy_nazwa', 'przeznaczenie_pomocy_nazwa', 'sektor_dzialalnosci_nazwa'], how='left')
    
    if gmina_df is not None:
         fact_df = fact_df.merge(gmina_df, left_on='gmina_siedziby_kod', right_on='gmina_kod', how='left')

    fact_df['dzien_udzielenia_pomocy'] = pd.to_datetime(fact_df['dzien_udzielenia_pomocy'])
    fact_df = fact_df.merge(dim_data, on='dzien_udzielenia_pomocy', how='left')

    # Select only fact and key columns
    fact_columns = [
        'wartosc_nominalna_pln', 'wartosc_brutto_pln', 'wartosc_brutto_eur',
        'beneficjent_id', 'udzielajacy_pomocy_id', 'charakterystyka_id', 
        'geografia_id', 'date_id'
    ]
    fact_df = fact_df[fact_columns]

    # 4. Save Gold Tables
    logging.info("--- Saving Gold Layer Tables ---")
    save_df_as_parquet_to_adls(adls_client, CONFIG['gold_container'], 'fact_przypadki_pomocy', fact_df)
    save_df_as_parquet_to_adls(adls_client, CONFIG['gold_container'], 'dim_beneficjent', dim_beneficjent)
    save_df_as_parquet_to_adls(adls_client, CONFIG['gold_container'], 'dim_udzielajacy_pomocy', dim_udzielajacy_pomocy)
    save_df_as_parquet_to_adls(adls_client, CONFIG['gold_container'], 'dim_charakterystyka', dim_charakterystyka)
    if gmina_df is not None:
        save_df_as_parquet_to_adls(adls_client, CONFIG['gold_container'], 'dim_geografia', gmina_df)
    save_df_as_parquet_to_adls(adls_client, CONFIG['gold_container'], 'dim_data', dim_data)

    logging.info("--- Gold layer build process completed. ---")

if __name__ == "__main__":
    main()
