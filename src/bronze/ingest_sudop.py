#!/usr/bin/env python3
"""
ingest_sudop.py — Bronze layer ingestion (standalone / fallback mode).

In the normal Kafka-backed pipeline this script's API-fetch logic has been
extracted into src/kafka/producer.py.  This file is kept as a STANDALONE
FALLBACK that writes directly to ADLS without Kafka, activated when:

    KAFKA_ENABLED=false   (or the env var is absent)

This lets the pipeline continue to work even if the Kafka/Event Hubs
infrastructure is unavailable (e.g. local development without docker-compose).

Key helpers used by consumer.py:
    save_json_to_adls()   — writes a JSON object to a given ADLS path
    get_latest_file_timestamp() — finds the most recent file in a directory
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
from common import get_adls_client, setup_logging, list_adls_files, read_json_from_adls

# --- CONFIGURATION ---

load_dotenv()

CONFIG = {
    "rate_limit_delay_seconds": C.RATE_LIMIT_DELAY_SECONDS,
    "retry_backoff_seconds":    C.RETRY_BACKOFF_SECONDS,
    "max_retries":              C.MAX_RETRIES,
    "poll_interval_seconds":    C.POLL_INTERVAL_SECONDS,
    "max_polls":                C.MAX_POLLS,
    "skip_if_exists_hours":     int(os.getenv("SKIP_IF_EXISTS_HOURS", 24)),
}

# -----------------------------------------------------------------------
# ADLS helpers — also imported by the Kafka consumer (consumer.py)
# -----------------------------------------------------------------------

def save_json_to_adls(service_client, container_name: str, file_path: str, data) -> None:
    """Saves a JSON-serialisable object to a path within an ADLS container."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        directory_path = os.path.dirname(file_path)
        if directory_path:
            file_system_client.create_directory(directory_path)

        file_client = file_system_client.get_file_client(file_path)
        json_data   = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        file_client.upload_data(json_data, overwrite=True)
        logging.info(f"Saved '{file_path}' to container '{container_name}'.")
    except Exception as e:
        logging.error(f"Error saving '{file_path}' to ADLS: {e}")


def get_latest_file_timestamp(service_client, container_name: str, directory: str) -> datetime | None:
    """Finds the timestamp of the most recent JSON file in an ADLS directory."""
    try:
        fs_client   = service_client.get_file_system_client(file_system=container_name)
        paths       = fs_client.get_paths(path=directory)
        latest_time = None

        for path in paths:
            if not path.is_directory and ".json" in path.name:
                try:
                    filename      = os.path.basename(path.name)
                    # Filename format: slownik_forma_pomocy_20240329_115311.json
                    timestamp_str = "_".join(filename.split("_")[-2:]).split(".")[0]
                    file_time     = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    if latest_time is None or file_time > latest_time:
                        latest_time = file_time
                except (IndexError, ValueError):
                    continue
        return latest_time
    except Exception as e:
        logging.error(f"Error finding latest file timestamp in '{directory}': {e}")
        return None

# -----------------------------------------------------------------------
# API helpers
# -----------------------------------------------------------------------

def make_request(url: str, allow_redirects: bool = True):
    """Makes a GET request to the SUDOP API with built-in retry logic."""
    for attempt in range(CONFIG["max_retries"]):
        try:
            response = requests.get(url, allow_redirects=allow_redirects, timeout=30)
            if response.status_code == 429:
                logging.warning(
                    f"Rate limit (429). Backing off {CONFIG['retry_backoff_seconds']}s."
                )
                time.sleep(CONFIG["retry_backoff_seconds"])
                continue
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP request failed (attempt {attempt + 1}): {e}")
            if attempt + 1 < CONFIG["max_retries"]:
                time.sleep(1)
    return None

# -----------------------------------------------------------------------
# Standalone workflow functions (KAFKA_ENABLED=false path)
# -----------------------------------------------------------------------

def ingest_dictionaries(service_client) -> None:
    """Downloads all SUDOP dictionaries and writes them directly to ADLS."""
    logging.info("--- STAGE 1: Dictionary Ingestion (standalone mode) ---")
    for dictionary_name in C.SUDOP_DICTIONARIES:
        url = f"{C.SUDOP_BASE_URL}/slownik/{dictionary_name.replace('_', '-')}"
        logging.info(f"Fetching: {url}")
        response = make_request(url)

        if response:
            try:
                data      = response.json()
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"slownik_{dictionary_name}_{timestamp}.json"
                save_json_to_adls(
                    service_client,
                    C.BRONZE_CONTAINER,
                    f"{C.BRONZE_DICTIONARIES_DIR}/{file_name}",
                    data,
                )
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON from {url}")
        time.sleep(CONFIG["rate_limit_delay_seconds"])


def fetch_and_save_cases(service_client, search_param: str, param_value: str) -> None:
    """Fetches aid cases for one search value, polls the queue, and saves to ADLS."""
    api_url   = f"{C.SUDOP_BASE_URL}/api"
    query_url = f"{api_url}/przypadki-pomocy?{search_param}={param_value}"
    logging.info(f"Initiating case search for {search_param}={param_value}")

    initial_response = make_request(query_url)
    if not initial_response:
        logging.error(f"Failed to get initial response for {param_value}")
        return

    queue_id = None
    if "kolejka" in initial_response.url:
        queue_id = initial_response.url.split("/")[-1]
    else:
        try:
            queue_id = initial_response.json().get("id-kolejka")
        except json.JSONDecodeError:
            logging.warning(f"Initial response for {param_value} was not JSON.")

    if not queue_id:
        logging.info(f"No queue ID for {param_value} — no data to process.")
        return

    logging.info(f"Got queue ID: {queue_id}. Polling...")
    queue_url = f"{api_url}/kolejka/{queue_id}"

    for i in range(CONFIG["max_polls"]):
        time.sleep(CONFIG["poll_interval_seconds"])
        logging.info(f"Poll attempt {i + 1}/{CONFIG['max_polls']} for {queue_id}")
        queue_response = make_request(queue_url)
        if not queue_response:
            continue
        try:
            queue_data = queue_response.json()
            if queue_data.get("wyniki"):
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"przypadki_pomocy_{search_param}_{param_value}_{timestamp}.json"
                save_json_to_adls(
                    service_client,
                    C.BRONZE_CONTAINER,
                    f"{C.BRONZE_CASES_DIR}/{file_name}",
                    queue_data,
                )
                return
        except json.JSONDecodeError:
            logging.warning(f"Non-JSON poll response for queue {queue_id}.")

    logging.warning(f"Polling timed out for queue ID {queue_id}.")


def ingest_cases_by_municipality(service_client) -> None:
    """Downloads aid cases for every municipality in the gmina_siedziby dictionary."""
    logging.info("--- STAGE 2: Case Ingestion by Municipality (standalone mode) ---")

    dict_files  = list_adls_files(service_client, C.BRONZE_CONTAINER, C.BRONZE_DICTIONARIES_DIR)
    gmina_files = [f for f in dict_files if "gmina_siedziby" in f]
    if not gmina_files:
        logging.error("Municipality dictionary not found in Bronze. Cannot proceed.")
        return

    latest_gmina_file = sorted(gmina_files, reverse=True)[0]
    logging.info(f"Using: {latest_gmina_file}")

    gminy_data = read_json_from_adls(service_client, C.BRONZE_CONTAINER, latest_gmina_file)
    if not gminy_data:
        logging.error("Could not read municipality data.")
        return

    total = len(gminy_data)
    logging.info(f"Found {total} municipalities.")

    for i, gmina in enumerate(gminy_data):
        gmina_kod  = gmina.get("number", "").strip()
        gmina_name = gmina.get("name", "")

        if not gmina_kod or gmina_name in ("NZ", "BRAK DANYCH"):
            continue

        logging.info(f"Processing {i + 1}/{total}: {gmina_name} ({gmina_kod})")
        fetch_and_save_cases(service_client, "gmina-siedziby-kod", gmina_kod)
        time.sleep(CONFIG["rate_limit_delay_seconds"])


def main():
    """
    Standalone entry point — runs the full ingestion directly to ADLS.
    Use when KAFKA_ENABLED=false (local dev without Kafka infrastructure).
    """
    setup_logging()
    logging.info("=== Bronze Ingestion — Standalone (KAFKA_ENABLED=false) ===")

    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        adls_client       = get_adls_client(connection_string)

        # Smart Skip Logic
        if os.getenv("SKIP_IF_EXISTS", "false").lower() == "true":
            logging.info("SKIP_IF_EXISTS enabled. Checking for recent files...")
            latest_ts = get_latest_file_timestamp(
                adls_client, C.BRONZE_CONTAINER, C.BRONZE_DICTIONARIES_DIR
            )
            if latest_ts and (datetime.utcnow() - latest_ts) < timedelta(
                hours=CONFIG["skip_if_exists_hours"]
            ):
                age_h = (datetime.utcnow() - latest_ts).total_seconds() / 3600
                logging.info(f"Recent files found ({age_h:.1f}h old). Skipping.")
                sys.exit(0)
            logging.info("No recent files. Proceeding with full ingestion.")

        ingest_dictionaries(adls_client)
        ingest_cases_by_municipality(adls_client)
        logging.info("=== Bronze Ingestion Completed ===")

    except ValueError as e:
        logging.critical(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Unexpected critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    kafka_enabled = os.getenv("KAFKA_ENABLED", "true").lower() == "true"
    if kafka_enabled:
        logging.basicConfig(level=logging.INFO)
        logging.warning(
            "KAFKA_ENABLED=true but you are running ingest_sudop.py directly. "
            "Run src/kafka/producer.py instead, or set KAFKA_ENABLED=false."
        )
        sys.exit(1)
    main()
