#!/usr/bin/env python3
"""
This script provides a practical example of how to query the gold layer data.
It connects to the Azure Data Lake, loads the final fact and dimension tables,
and performs an analysis to find the total aid amount by municipality.
"""

import logging
import os

import pandas as pd
from dotenv import load_dotenv

# Since this script is in a different directory, we need to adjust the Python path
# to correctly import the 'common' module from the 'src' directory.
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import constants as C
from common import get_adls_client, read_parquet_from_adls, setup_logging


def main():
    """
    Connects to the gold layer, loads data, and runs a sample business analysis.
    """
    setup_logging()
    load_dotenv()

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    adls_client = get_adls_client(connection_string)

    # 1. Load the required tables from the Gold layer
    logging.info("--- Loading data from Gold Layer ---")
    fact_table_path = f"{C.GOLD_FACT_TABLE}/{C.GOLD_FACT_TABLE}.parquet"
    dim_geo_path = f"{C.GOLD_DIM_GEOGRAFIA}/{C.GOLD_DIM_GEOGRAFIA}.parquet"

    fact_df = read_parquet_from_adls(adls_client, C.GOLD_CONTAINER, fact_table_path)
    dim_geo_df = read_parquet_from_adls(adls_client, C.GOLD_CONTAINER, dim_geo_path)

    if fact_df is None or dim_geo_df is None:
        logging.critical("Could not load necessary tables from the Gold layer. Aborting analysis.")
        sys.exit(1)

    # 2. Perform the Analysis (Total Aid by Municipality)
    logging.info("--- Performing Analysis: Total Aid by Municipality ---")
    
    # Merge (JOIN) the fact table with the geography dimension
    merged_df = pd.merge(
        fact_df,
        dim_geo_df,
        on="geografia_id",
        how="left"
    )

    # Group by municipality name and aggregate the aid amount
    analysis_result = merged_df.groupby("gmina_nazwa")["wartosc_brutto_pln"].sum().reset_index()
    analysis_result = analysis_result.rename(columns={"wartosc_brutto_pln": "total_aid_pln"})

    # Sort the results to find the top municipalities
    top_20_municipalities = analysis_result.sort_values(by="total_aid_pln", ascending=False).head(20)

    # 3. Display the Results
    logging.info("--- Analysis Results: Top 20 Municipalities by Total Aid (PLN) ---")
    print(top_20_municipalities.to_string(index=False))


if __name__ == "__main__":
    main()
