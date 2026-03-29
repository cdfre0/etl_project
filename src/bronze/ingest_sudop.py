#!/usr/bin/env python3
"""
This script is responsible for the Bronze layer ingestion. It connects to the 
SUDOP API, downloads dictionary and case files, and uploads them as raw JSON
into the 'bronze' container in Azure Data Lake Storage.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

import constants as C
from common import get_adls_client, setup_logging

# --- CONFIGURATION ---

load_dotenv()

CONFIG = {
    "rate_limit_delay_seconds": C.RATE_LIMIT_DELAY_SECONDS,
    "retry_backoff_seconds": C.RETRY_BACKOFF_SECONDS,
    "max_retries": C.MAX_RETRIES,
    "poll_interval_seconds": C.POLL_INTERVAL_SECONDS,
    "max_polls": C.MAX_POLLS,
    "skip_if_exists_hours": int(os.getenv("SKIP_IF_EXISTS_HOURS", 24))
}

# --- AZURE & API HELPER FUNCTIONS ---

def save_json_to_adls(service_client, container_name, file_path, data):
    """Saves a JSON object to a specified path within an ADLS container."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        directory_path = os.path.dirname(file_path)
        if directory_path:
            file_system_client.create_directory(directory_path)

        file_client = file_system_client.get_file_client(file_path)
        json_data = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        
        file_client.upload_data(json_data, overwrite=True)
        logging.info(f"Successfully saved '{file_path}' to container '{container_name}'.")
    except HttpResponseError as e:
        logging.error(f"Azure Error saving '{file_path}': {e.message}")
    except Exception as e:
        logging.error(f"General Error saving '{file_path}': {e}")

def get_latest_file_timestamp(service_client, container_name, directory) -> datetime | None:
    """Finds the timestamp of the most recent file in an ADLS directory."""
    try:
        fs_client = service_client.get_file_system_client(file_system=container_name)
        paths = fs_client.get_paths(path=directory)
        latest_time = None

        for path in paths:
            if not path.is_directory and ".json" in path.name:
                try:
                    filename = os.path.basename(path.name)
                    # Parsing format: slownik_forma_pomocy_20240329_115311.json
                    timestamp_str = '_'.join(filename.split('_')[-2:]).split('.')[0]
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    if latest_time is None or file_time > latest_time:
                        latest_time = file_time
                except (IndexError, ValueError):
                    continue
        return latest_time
    except Exception as e:
        logging.error(f"Error finding latest file timestamp in '{directory}': {e}")
        return None

def make_request(url, allow_redirects=True):
    """Makes a request to the SUDOP API with built-in retry logic."""
    for attempt in range(CONFIG["max_retries"]):
        try:
            response = requests.get(url, allow_redirects=allow_redirects, timeout=30)
            if response.status_code == 429:
                logging.warning(f"Rate limit hit (429). Waiting for {CONFIG['retry_backoff_seconds']}s.")
                time.sleep(CONFIG["retry_backoff_seconds"])
                continue
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP Request failed on attempt {attempt + 1}: {e}")
            if attempt + 1 < CONFIG["max_retries"]:
                time.sleep(5)
    return None

# --- MAIN WORKFLOW FUNCTIONS ---

def ingest_dictionaries(service_client):
    """Downloads all SUDOP dictionaries."""
    logging.info("--- STAGE 1: Dictionary Ingestion ---")
    for dictionary_name in C.SUDOP_DICTIONARIES:
        url = f"{C.SUDOP_BASE_URL}/slownik/{dictionary_name.replace('_', '-')}"
        logging.info(f"Fetching data from: {url}")
        response = make_request(url)

        if response:
            try:
                data = response.json()
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"slownik_{dictionary_name}_{timestamp}.json"
                save_json_to_adls(service_client, C.BRONZE_CONTAINER, f"{C.BRONZE_DICTIONARIES_DIR}/{file_name}", data)
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON from {url}")
        time.sleep(CONFIG["rate_limit_delay_seconds"])

def main():
    """Main function to run the full ingestion pipeline."""
    setup_logging()
    
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        adls_client = get_adls_client(connection_string)

        # Smart Skip Logic
        if os.getenv("SKIP_IF_EXISTS", "false").lower() == "true":
            logging.info("SKIP_IF_EXISTS is enabled. Checking for recent files...")
            latest_ts = get_latest_file_timestamp(adls_client, C.BRONZE_CONTAINER, C.BRONZE_DICTIONARIES_DIR)
            if latest_ts and (datetime.utcnow() - latest_ts) < timedelta(hours=CONFIG["skip_if_exists_hours"]):
                logging.info(f"Recent files found (latest is {(datetime.utcnow() - latest_ts).total_seconds()/3600:.1f} hours old). Skipping ingestion.")
                sys.exit(0)
            logging.info("No recent files found. Proceeding with full ingestion.")
        
        ingest_dictionaries(adls_client)
        # Note: In a production scenario, case ingestion would be its own comprehensive module.
        # It is omitted here to keep the refactoring example focused.
        logging.info("--- Bronze Ingestion Process Completed ---")

    except ValueError as e:
        logging.critical(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
