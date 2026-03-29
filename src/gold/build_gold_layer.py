#!/usr/bin/env python3
"""
This script is responsible for the Gold layer. It reads curated data from the 
'silver' container, builds a star schema (fact and dimension tables), and saves 
the final analytical tables as Parquet files in the 'gold' container.
"""

import logging
import os

from dotenv import load_dotenv

import constants as C
from common import (get_adls_client, read_parquet_from_adls,
                    save_df_as_parquet_to_adls, setup_logging)

# --- CONFIGURATION ---

load_dotenv()

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
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    adls_client = get_adls_client(connection_string)
    
    logging.info("--- Reading data from Silver Layer ---")
    cases_df = read_parquet_from_adls(adls_client, C.SILVER_CONTAINER, f"{C.SILVER_PRZYPADKI_POMOCY_TABLE}/{C.SILVER_PRZYPADKI_POMOCY_TABLE}.parquet")
    gmina_df = read_parquet_from_adls(adls_client, C.SILVER_CONTAINER, 'slownik_gmina_siedziby/slownik_gmina_siedziby.parquet')

    if cases_df is None:
        logging.critical("Could not read the main cases table from Silver. Aborting.")
        return

    logging.info("--- Creating Dimension Tables ---")
    dim_beneficjent = create_dimension_table(cases_df, ['nip_beneficjenta', 'nazwa_beneficjenta', 'wielkosc_beneficjenta_nazwa'], 'beneficjent')
    dim_udzielajacy_pomocy = create_dimension_table(cases_df, ['nip_udzielajacego_pomocy', 'nazwa_udzielajacego_pomocy'], 'udzielajacy_pomocy')
    dim_charakterystyka = create_dimension_table(cases_df, ['forma_pomocy_nazwa', 'przeznaczenie_pomocy_nazwa', 'sektor_dzialalnosci_nazwa'], 'charakterystyka')
    
    if gmina_df is not None:
        gmina_df.rename(columns={'kod': 'gmina_kod', 'nazwa': 'gmina_nazwa'}, inplace=True)
        gmina_df['geografia_id'] = gmina_df.index
    
    dim_data = create_date_dimension(cases_df['dzien_udzielenia_pomocy'])
    dim_data.rename(columns={'full_date': 'dzien_udzielenia_pomocy'}, inplace=True)

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

    fact_columns = [
        'wartosc_nominalna_pln', 'wartosc_brutto_pln', 'wartosc_brutto_eur',
        'beneficjent_id', 'udzielajacy_pomocy_id', 'charakterystyka_id', 
        'geografia_id', 'date_id'
    ]
    # Ensure all required columns exist before selecting
    for col in fact_columns:
        if col not in fact_df.columns:
            fact_df[col] = pd.NA

    fact_df = fact_df[fact_columns]

    logging.info("--- Saving Gold Layer Tables ---")
    save_df_as_parquet_to_adls(adls_client, C.GOLD_CONTAINER, C.GOLD_FACT_TABLE, fact_df)
    save_df_as_parquet_to_adls(adls_client, C.GOLD_CONTAINER, C.GOLD_DIM_BENEFICJENT, dim_beneficjent)
    save_df_as_parquet_to_adls(adls_client, C.GOLD_CONTAINER, C.GOLD_DIM_UDZIELAJACY_POMOCY, dim_udzielajacy_pomocy)
    save_df_as_parquet_to_adls(adls_client, C.GOLD_CONTAINER, C.GOLD_DIM_CHARAKTERYSTYKA, dim_charakterystyka)
    if gmina_df is not None:
        save_df_as_parquet_to_adls(adls_client, C.GOLD_CONTAINER, C.GOLD_DIM_GEOGRAFIA, gmina_df)
    save_df_as_parquet_to_adls(adls_client, C.GOLD_CONTAINER, C.GOLD_DIM_DATA, dim_data)

    logging.info("--- Gold layer build process completed. ---")

if __name__ == "__main__":
    main()
