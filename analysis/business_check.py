#!/usr/bin/env python3
"""
business_check.py

Connects to Databricks SQL via the databricks-sql-connector and runs a sample
business analysis against the Gold layer Delta tables.

The Gold layer tables live in the Databricks `gold` schema as Delta tables —
NOT as raw Parquet files on ADLS — so we query them through Databricks SQL,
not by reading ADLS directly.

Requirements (add to requirements.txt if missing):
    databricks-sql-connector

Environment variables (same as the rest of the project, set in .env):
    DBT_DATABRICKS_HOST        e.g. adb-1234567890.12.azuredatabricks.net
    DBT_DATABRICKS_HTTP_PATH   e.g. /sql/1.0/warehouses/abcdef1234567890
    DATABRICKS_TOKEN           personal access token
"""

import logging
import os
import sys

import pandas as pd
from databricks import sql
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    host       = os.getenv("DBT_DATABRICKS_HOST")
    http_path  = os.getenv("DBT_DATABRICKS_HTTP_PATH")
    token      = os.getenv("DATABRICKS_TOKEN")

    if not all([host, http_path, token]):
        raise EnvironmentError(
            "Missing one or more required env vars: "
            "DBT_DATABRICKS_HOST, DBT_DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN"
        )

    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


def run_query(conn, query: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info("Connecting to Databricks SQL...")
    try:
        conn = get_connection()
    except EnvironmentError as e:
        logging.critical(e)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Analysis 1: Total aid by municipality (Top 20)
    # ------------------------------------------------------------------
    logging.info("Running: Total aid by municipality")
    query_municipality = """
        SELECT
            g.gmina_nazwa                        AS municipality_name,
            COUNT(*)                             AS number_of_cases,
            ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
        FROM gold.fact_przypadki_pomocy f
        LEFT JOIN gold.dim_gmina g ON f.geografia_id = g.geografia_id
        GROUP BY g.gmina_nazwa
        ORDER BY total_aid_pln DESC
        LIMIT 20
    """
    df_municipality = run_query(conn, query_municipality)
    print("\n=== Top 20 Municipalities by Total Aid (PLN) ===")
    print(df_municipality.to_string(index=False))

    # ------------------------------------------------------------------
    # Analysis 2: Aid by business size
    # ------------------------------------------------------------------
    logging.info("Running: Aid by business size")
    query_size = """
        SELECT
            b.wielkosc_beneficjenta_nazwa        AS business_size,
            COUNT(*)                             AS number_of_cases,
            ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
        FROM gold.fact_przypadki_pomocy f
        LEFT JOIN gold.dim_beneficjent b ON f.beneficjent_id = b.beneficjent_id
        WHERE b.wielkosc_beneficjenta_nazwa IS NOT NULL
        GROUP BY b.wielkosc_beneficjenta_nazwa
        ORDER BY total_aid_pln DESC
    """
    df_size = run_query(conn, query_size)
    print("\n=== Aid by Business Size ===")
    print(df_size.to_string(index=False))

    conn.close()
    logging.info("Done.")


if __name__ == "__main__":
    main()
