#!/usr/bin/env python3
"""
Reads SUDOP data from the bronze container, transforms it based on metadata,
and saves it as Parquet files in the silver container.
"""

import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# --- CONFIGURATION ---

CONFIG = {
    "bronze_container": "bronze",
    "silver_container": "silver",
    "bronze_folders": ["dictionaries", "cases"],
    "metadata_path": Path(__file__).parent / "metadata.json",
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

def list_bronze_files(service_client: DataLakeServiceClient, container: str, folders: list) -> list:
    """Lists all JSON files in the specified folders of the bronze container."""
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        all_paths = []
        for folder in folders:
            paths = fs_client.get_paths(path=folder)
            all_paths.extend([path.name for path in paths if path.name.endswith('.json')])
        logging.info(f"Found {len(all_paths)} JSON files in {container}/{folders}.")
        return all_paths
    except Exception as e:
        logging.error(f"Error listing files in container '{container}': {e}")
        return []

def read_json_from_adls(service_client: DataLakeServiceClient, container: str, file_path: str) -> dict | list | None:
    """Reads and parses a JSON file from ADLS."""
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        file_client = fs_client.get_file_client(file_path)
        download = file_client.download_file()
        return json.loads(download.readall())
    except ResourceNotFoundError:
        logging.error(f"File not found: {container}/{file_path}")
        return None
    except Exception as e:
        logging.error(f"Error reading {container}/{file_path}: {e}")
        return None

def save_df_as_parquet_to_adls(service_client: DataLakeServiceClient, container: str, file_path: str, df: pd.DataFrame):
    """Saves a DataFrame as a Parquet file to ADLS."""
    try:
        if df.empty:
            logging.warning(f"DataFrame for {file_path} is empty, skipping save.")
            return

        fs_client = service_client.get_file_system_client(file_system=container)
        directory_path = Path(file_path).parent.as_posix()
        if directory_path:
            fs_client.create_directory(directory_path)
            
        file_client = fs_client.get_file_client(file_path)
        
        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        
        file_client.upload_data(buf.getvalue(), overwrite=True)
        logging.info(f"Successfully saved {file_path} to container '{container}'.")
    except Exception as e:
        logging.error(f"Error saving {file_path} to ADLS: {e}")

# --- TRANSFORMATION LOGIC ---

def load_metadata(path: Path) -> dict:
    """Loads the metadata definition file."""
    with open(path, 'r') as f:
        return json.load(f)["table_definitions"]

def transform_dataframe(df: pd.DataFrame, table_def: dict, source_file_name: str) -> pd.DataFrame:
    """Applies typing and normalization to a DataFrame based on a table definition."""
    if df.empty:
        return df
        
    column_defs = {col['name']: col for col in table_def['columns']}
    
    # Rename source columns to target (snake_case)
    rename_map = {
        "name": "nazwa", "number": "kod", # Dictionaries
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
    df = df.rename(columns=rename_map)

    # Add metadata columns
    df['_source_file'] = source_file_name
    df['_curated_at'] = datetime.utcnow()
    
    # Cast to types defined in metadata
    for col_name, col_def in column_defs.items():
        if col_name in df.columns:
            if col_def['type'] == 'date':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
            elif col_def['type'] in ['double', 'integer']:
                 df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif col_def['type'] == 'timestamp':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            else:
                # Fill NaN with None to handle missing values gracefully before converting to string
                df[col_name] = df[col_name].fillna(value=pd.NA).astype(str)


    # Ensure all columns from metadata exist and are in order
    final_cols = [col['name'] for col in table_def['columns']]
    for col in final_cols:
        if col not in df.columns:
            df[col] = None

    return df[final_cols]

# --- MAIN WORKFLOW ---

def main():
    """Main function to run the full curated pipeline."""
    setup_logging()
    table_defs = load_metadata(CONFIG['metadata_path'])
    adls_client = get_adls_client()
    bronze_files = list_bronze_files(adls_client, CONFIG['bronze_container'], CONFIG['bronze_folders'])

    # Process and collect data for `przypadki_pomocy`
    przypadki_pomocy_dfs = []

    for file_path in bronze_files:
        file_name = Path(file_path).name
        content = read_json_from_adls(adls_client, CONFIG['bronze_container'], file_path)

        if not content:
            continue

        if file_name.startswith('slownik_'):
            table_def = table_defs['slownik']
            records = content
            df = pd.DataFrame(records)
            transformed_df = transform_dataframe(df, table_def, file_name)
            
            # Save each dictionary as a separate file
            table_name = file_name.split('_20')[0]
            output_path = f"slowniki/{table_name}/{table_name}.parquet"
            save_df_as_parquet_to_adls(adls_client, CONFIG['silver_container'], output_path, transformed_df)

        elif file_name.startswith('przypadki_pomocy_'):
            table_def = table_defs['przypadki_pomocy']
            records = content.get('wyniki', []) if isinstance(content, dict) else []
            df = pd.DataFrame(records)
            transformed_df = transform_dataframe(df, table_def, file_name)
            przypadki_pomocy_dfs.append(transformed_df)

    # Consolidate and save the `przypadki_pomocy` table
    if przypadki_pomocy_dfs:
        final_df = pd.concat(przypadki_pomocy_dfs, ignore_index=True)
        table_name = "przypadki_pomocy"
        output_path = f"{table_name}/{table_name}.parquet"
        save_df_as_parquet_to_adls(adls_client, CONFIG['silver_container'], output_path, final_df)
    else:
        logging.warning("No data processed for table 'przypadki_pomocy'.")
    
    logging.info("--- Curated load process completed. ---")

if __name__ == "__main__":
    main()
