#!/usr/bin/env python3
"""
This file contains constant values used across the ETL pipeline.
Centralizing them here makes the code cleaner and easier to maintain.
"""

# --- Azure Configuration ---
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"
GOLD_CONTAINER = "gold"

# --- Data Layer Directories ---
BRONZE_DICTIONARIES_DIR = "dictionaries"
BRONZE_CASES_DIR = "cases"

SILVER_SLOWNIKI_DIR = "slowniki"
SILVER_PRZYPADKI_POMOCY_TABLE = "przypadki_pomocy"

GOLD_FACT_TABLE = "fact_przypadki_pomocy"
GOLD_DIM_BENEFICJENT = "dim_beneficjent"
GOLD_DIM_UDZIELAJACY_POMOCY = "dim_udzielajacy_pomocy"
GOLD_DIM_CHARAKTERYSTYKA = "dim_charakterystyka"
GOLD_DIM_GEOGRAFIA = "dim_geografia"
GOLD_DIM_DATA = "dim_data"


# --- SUDOP API Configuration ---
SUDOP_BASE_URL = "https://api-sudop.uokik.gov.pl/sudop-api"
SUDOP_DICTIONARIES = [
    "forma_pomocy", 
    "przeznaczenie_pomocy", 
    "srodek_pomocowy",
    "sektor_dzialalnosci", 
    "gmina_siedziby"
]

# --- ETL Process Configuration ---
RATE_LIMIT_DELAY_SECONDS = 2
RETRY_BACKOFF_SECONDS = 15
MAX_RETRIES = 5
POLL_INTERVAL_SECONDS = 1
MAX_POLLS = 30

# --- File Paths ---
METADATA_FILE_PATH = "src/curated/metadata.json"
