#!/usr/bin/env python3
"""
This script is responsible for the Silver/Curated layer. It reads raw JSON data 
from the 'bronze' container, applies transformations based on a metadata file, 
and saves the cleaned, structured data as Parquet files in the 'silver' container.
"""

import json
import logging
import os
from pathlib import Path
import pandas as pd 
from datetime import datetime

from dotenv import load_dotenv

import constants as C
from common import (get_adls_client, list_adls_files, read_json_from_adls,
                    save_df_as_parquet_to_adls, setup_logging)

# --- CONFIGURATION ---

load_dotenv()

# --- TRANSFORMATION LOGIC ---

def load_metadata(path: str) -> dict:
    """Loads the table definitions from the metadata file."""
    with open(path, 'r') as f:
        return json.load(f)["table_definitions"]

def transform_dataframe(df: pd.DataFrame, table_def: dict, source_file_name: str) -> pd.DataFrame:
    """Applies typing and normalization to a DataFrame based on a table definition."""
    if df.empty:
        return df
        
    column_defs = {col['name']: col for col in table_def['columns']}
    
    rename_map = {
        "name": "nazwa", "number": "kod",  # Map source dictionary fields to curated schema
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

    df['_source_file'] = source_file_name
    df['_curated_at'] = datetime.utcnow()
    
    for col_name, col_def in column_defs.items():
        if col_name in df.columns:
            if col_def['type'] == 'date':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
            elif col_def['type'] in ['double', 'integer']:
                 df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif col_def['type'] == 'timestamp':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            else:
                df[col_name] = df[col_name].fillna(pd.NA).astype(str)

    final_cols = [col['name'] for col in table_def['columns']]
    for col in final_cols:
        if col not in df.columns:
            df[col] = None

    return df[final_cols]

# --- MAIN WORKFLOW ---

def main():
    """Main function to run the full curated pipeline."""
    setup_logging()
    table_defs = load_metadata(C.METADATA_FILE_PATH)
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    adls_client = get_adls_client(connection_string)

    bronze_dict_files = list_adls_files(adls_client, C.BRONZE_CONTAINER, C.BRONZE_DICTIONARIES_DIR)
    bronze_case_files = list_adls_files(adls_client, C.BRONZE_CONTAINER, C.BRONZE_CASES_DIR)

    # Process each dictionary file into its own table
    for file_path in bronze_dict_files:
        content = read_json_from_adls(adls_client, C.BRONZE_CONTAINER, file_path)
        if content:
            df = pd.DataFrame(content)
            transformed_df = transform_dataframe(df, table_defs['slownik'], Path(file_path).name)
            table_name = Path(file_path).name.split('_20')[0]
            save_df_as_parquet_to_adls(adls_client, C.SILVER_CONTAINER, table_name, transformed_df)

    # Process and consolidate all case files into one table
    all_cases_df = []
    for file_path in bronze_case_files:
        content = read_json_from_adls(adls_client, C.BRONZE_CONTAINER, file_path)
        if content and 'wyniki' in content:
            df = pd.DataFrame(content['wyniki'])
            transformed_df = transform_dataframe(df, table_defs[C.SILVER_PRZYPADKI_POMOCY_TABLE], Path(file_path).name)
            all_cases_df.append(transformed_df)
    
    if all_cases_df:
        final_cases_df = pd.concat(all_cases_df, ignore_index=True)
        save_df_as_parquet_to_adls(adls_client, C.SILVER_CONTAINER, C.SILVER_PRZYPADKI_POMOCY_TABLE, final_cases_df)
    else:
        logging.warning(f"No case data was processed for the '{C.SILVER_PRZYPADKI_POMOCY_TABLE}' table.")
    
    logging.info("--- Curated load process completed. ---")

if __name__ == "__main__":
    main()
