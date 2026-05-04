#!/usr/bin/env python3
"""
demo_usage.py

This script demonstrates how ANY student can instantly query the SUDOP Data Product
out-of-the-box by connecting directly to the live Databricks Gold Layer (Delta Tables) 
in Azure Storage using a secure, read-only Shared Access Signature (SAS) token.

NO LOCAL DATA DOWNLOAD OR DATABRICKS CREDENTIALS REQUIRED!

Prerequisites:
    pip install duckdb pandas
"""

import duckdb
import pandas as pd

# The read-only SAS token for the 'gold' container (valid until June 2026)
ACCOUNT_NAME = "stetldatamedallion"
CONTAINER = "gold"
SAS_TOKEN = "sp=r&st=2026-05-01T13:49:19Z&se=2026-06-10T22:04:19Z&spr=https&sv=2025-11-05&sr=c&sig=T5ljxdPakXvnV%2Favgdrc41F6%2Bijn%2BtivHgKV1dU68n8%3D"

def main():
    print("Initializing local DuckDB engine and installing Azure/Delta extensions...")
    conn = duckdb.connect()
    
    # Install and load the necessary extensions to read Delta tables from Azure
    conn.execute("INSTALL azure;")
    conn.execute("LOAD azure;")
    conn.execute("INSTALL delta;")
    conn.execute("LOAD delta;")

    print("Configuring secure connection to Azure Data Lake...")
    # Create the Azure secret using the SAS token
    connection_string = f"BlobEndpoint=https://{ACCOUNT_NAME}.blob.core.windows.net/;SharedAccessSignature={SAS_TOKEN}"
    conn.execute(f"""
        CREATE SECRET azure_secret (
            TYPE AZURE,
            CONNECTION_STRING '{connection_string}'
        );
    """)

    print("\nConnected to live Azure Gold Layer! Querying Delta Tables over the internet...\n")

    print("=== Top 5 Municipalities by Total Aid ===")
    
    # Query the remote Delta tables directly! DuckDB is smart enough to push down
    # filters and only download the Parquet byte-ranges it actually needs.
    query = f"""
        SELECT 
            g.gmina_nazwa AS municipality_name,
            ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln,
            COUNT(*) AS total_cases
        FROM delta_scan('azure://{CONTAINER}/fact_przypadki_pomocy') f
        JOIN delta_scan('azure://{CONTAINER}/dim_gmina') g ON f.geografia_id = g.geografia_id
        GROUP BY g.gmina_nazwa
        ORDER BY total_aid_pln DESC
        LIMIT 5;
    """
    
    df = conn.execute(query).df()
    print(df.to_string(index=False))
    
    print("\n--------------------------------------------------\n")
    
    print("=== Number of Aid Cases by Business Sector ===")
    query_sector = f"""
        SELECT 
            c.sektor_dzialalnosci_nazwa AS sector,
            COUNT(*) AS number_of_cases
        FROM delta_scan('azure://{CONTAINER}/fact_przypadki_pomocy') f
        JOIN delta_scan('azure://{CONTAINER}/dim_charakterystyka') c ON f.charakterystyka_id = c.charakterystyka_id
        WHERE c.sektor_dzialalnosci_nazwa IS NOT NULL
        GROUP BY c.sektor_dzialalnosci_nazwa
        ORDER BY number_of_cases DESC
        LIMIT 5;
    """
    
    df_sector = conn.execute(query_sector).df()
    print(df_sector.to_string(index=False))
    
    conn.close()
    print("\nDemo complete!")

if __name__ == "__main__":
    main()
