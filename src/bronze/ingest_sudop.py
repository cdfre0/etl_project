import os
import requests
import json
import time
import logging
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import sys
from azure.core.exceptions import HttpResponseError

# --- CONFIGURATION ---

CONFIG = {
    "adls_container_name": "bronze",
    "sudop_base_url": "https://api-sudop.uokik.gov.pl/sudop-api",
    "dictionaries": [
        "forma_pomocy",
        "przeznaczenie_pomocy",
        "srodek_pomocowy",
        "sektor_dzialalnosci",
        "gmina_siedziby"
    ],
    "rate_limit_delay_seconds": 5, # Safe delay between initial API calls
    "retry_backoff_seconds": 60,   # Longer delay after a 429 error
    "max_retries": 3,
    "poll_interval_seconds": 15,
    "max_polls": 12
}

# --- LOGGING SETUP ---

def setup_logging():
    """Configures logging for the script."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Enable detailed Azure SDK logging to stdout
    azure_logger = logging.getLogger('azure.storage.filedatalake')
    azure_logger.setLevel(logging.WARNING) # Set to DEBUG for very verbose output
    handler = logging.StreamHandler(stream=sys.stdout)
    azure_logger.addHandler(handler)

# --- AZURE DATA LAKE HELPER FUNCTIONS ---

def get_adls_client():
    """Authenticates and returns a DataLakeServiceClient using connection string."""
    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set.")
    
    logging.info("Successfully created ADLS service client.")
    return DataLakeServiceClient.from_connection_string(connection_string)

def save_json_to_adls(service_client, container_name, file_path, data):
    """Saves a JSON object to a specified path within an ADLS container."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        directory_path = os.path.dirname(file_path)
        if directory_path:
            # This will create the directory if it doesn't exist.
            # Using exists_ok=True is cleaner than try/except for this case.
            file_system_client.create_directory(directory_path)

        file_client = file_system_client.get_file_client(file_path)
        json_data = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        
        file_client.upload_data(json_data, overwrite=True)
        logging.info(f"Successfully saved '{file_path}' to container '{container_name}'.")
        
    except HttpResponseError as e:
        logging.error(f"Azure Error saving '{file_path}': {e.message}")
    except Exception as e:
        logging.error(f"General Error saving '{file_path}': {e}")

def read_json_from_adls(service_client, container_name, file_path):
    """Reads and parses a JSON file from ADLS."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(file_path)
        
        download = file_client.download_file()
        data = json.loads(download.readall())
        logging.info(f"Successfully read '{file_path}' from container '{container_name}'.")
        return data
    except Exception as e:
        logging.error(f"Error reading file '{file_path}' from ADLS: {e}")
        return None

def get_latest_dictionary_file(service_client, container_name, dictionary_name):
    """Finds the most recent dictionary file in ADLS."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        path_prefix = f"dictionaries/slownik_{dictionary_name}"
        paths = file_system_client.get_paths(path="dictionaries")
        
        latest_file = None
        latest_time = datetime.min

        for path in paths:
            if path.name.startswith(path_prefix) and path.is_directory == False:
                try:
                    filename = os.path.basename(path.name)
                    timestamp_str = '_'.join(filename.split('_')[-2:]).split('.')[0]
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    if file_time > latest_time:
                        latest_time = file_time
                        latest_file = path.name
                except (IndexError, ValueError):
                    continue 
        
        if latest_file:
            logging.info(f"Found latest dictionary for '{dictionary_name}': {latest_file}")
        return latest_file
    except Exception as e:
        logging.error(f"Error listing dictionary files: {e}")
        return None

# --- SUDOP API HELPER FUNCTIONS ---

def make_request(url, allow_redirects=True):
    """Makes a request to the SUDOP API with built-in retry logic for rate limiting."""
    for attempt in range(CONFIG["max_retries"]):
        try:
            response = requests.get(url, allow_redirects=allow_redirects, timeout=30)
            if response.status_code == 429:
                logging.warning(f"Rate limit hit (429). Waiting for {CONFIG['retry_backoff_seconds']} seconds before retrying.")
                time.sleep(CONFIG["retry_backoff_seconds"])
                continue # Retry the request
            
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP Request failed on attempt {attempt + 1}: {e}")
            if attempt + 1 == CONFIG["max_retries"]:
                return None # Failed all retries
            time.sleep(5) # Wait a bit before the next retry for general errors
    
    return None

def fetch_and_save_cases(service_client, search_param, param_value):
    """Fetches aid cases, handles the queue, and saves the result."""
    api_url = f"{CONFIG['sudop_base_url']}/api"
    query_url = f"{api_url}/przypadki-pomocy?{search_param}={param_value}"
    logging.info(f"Initiating search for {search_param}={param_value}")

    initial_response = make_request(query_url)
    if not initial_response:
        return # Failed to get a response

    queue_id = None
    if 'kolejka' in initial_response.url:
        queue_id = initial_response.url.split('/')[-1]
    else:
        try:
            queue_id = initial_response.json().get("id-kolejka")
        except json.JSONDecodeError:
            logging.warning("Initial response was not JSON. May indicate no results.")

    if not queue_id:
        logging.info(f"No queue ID received for {param_value}. It may have no data.")
        return

    logging.info(f"Got queue ID: {queue_id}. Starting to poll.")
    queue_url = f"{api_url}/kolejka/{queue_id}"
    
    for i in range(CONFIG["max_polls"]):
        time.sleep(CONFIG["poll_interval_seconds"])
        logging.info(f"Polling attempt {i+1}/{CONFIG['max_polls']}: {queue_url}")
        
        queue_response = make_request(queue_url)
        if not queue_response:
            continue # Try polling again after delay

        try:
            queue_data = queue_response.json()
            if queue_data.get("wyniki"):
                logging.info("Data is ready, saving results.")
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"przypadki_pomocy_{search_param}_{param_value}_{timestamp}.json"
                file_path = f"cases/{file_name}"
                save_json_to_adls(service_client, CONFIG["adls_container_name"], file_path, queue_data)
                return
        except json.JSONDecodeError:
            logging.warning("Polling response was not JSON. Data may not be ready yet.")
    
    logging.warning(f"Polling timed out for queue ID {queue_id}.")


# --- MAIN WORKFLOW FUNCTIONS ---

def ingest_dictionaries(service_client):
    """Downloads all SUDOP dictionaries if they are missing or older than 24 hours."""
    logging.info("--- STAGE 1: DICTIONARY INGESTION ---")
    for dictionary_name in CONFIG["dictionaries"]:
        latest_file = get_latest_dictionary_file(service_client, CONFIG["adls_container_name"], dictionary_name)
        
        if latest_file:
            try:
                filename = os.path.basename(latest_file)
                timestamp_str = '_'.join(filename.split('_')[-2:]).split('.')[0]
                file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                if datetime.utcnow() - file_time < timedelta(hours=24):
                    logging.info(f"Recent dictionary '{latest_file}' already exists. Skipping download.")
                    continue
            except (ValueError, IndexError):
                logging.warning(f"Could not parse timestamp from '{latest_file}'. Re-downloading.")
        
        url = f"{CONFIG['sudop_base_url']}/slownik/{dictionary_name.replace('_', '-')}"
        logging.info(f"Fetching data from: {url}")
        response = make_request(url)

        if response:
            try:
                data = response.json()
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"slownik_{dictionary_name}_{timestamp}.json"
                file_path = f"dictionaries/{file_name}"
                save_json_to_adls(service_client, CONFIG["adls_container_name"], file_path, data)
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON from {url}")
        
        time.sleep(CONFIG["rate_limit_delay_seconds"])

def ingest_cases_by_municipality(service_client):
    """Downloads aid cases for every municipality found in the dictionary."""
    logging.info("\n--- STAGE 2: CASE INGESTION BY MUNICIPALITY ---")
    latest_gminy_file = get_latest_dictionary_file(service_client, CONFIG["adls_container_name"], "gmina_siedziby")

    if not latest_gminy_file:
        logging.error("Municipality dictionary file not found. Cannot proceed with case ingestion.")
        return

    gminy_data = read_json_from_adls(service_client, CONFIG["adls_container_name"], latest_gminy_file)
    if not gminy_data:
        logging.error("Could not read municipality data. Skipping case ingestion.")
        return

    total_gminy = len(gminy_data)
    logging.info(f"Found {total_gminy} municipalities to process.")

    for i, gmina in enumerate(gminy_data):
        gmina_kod = gmina.get("number")
        gmina_name = gmina.get("name")
        logging.info(f"\nProcessing municipality {i+1}/{total_gminy}: {gmina_name} ({gmina_kod})")
        
        if gmina_kod:
            fetch_and_save_cases(service_client, "gmina-siedziby-kod", gmina_kod)
            logging.info(f"Waiting for {CONFIG['rate_limit_delay_seconds']} seconds...")
            time.sleep(CONFIG["rate_limit_delay_seconds"])

def main():
    """Main function to run the full ingestion pipeline."""
    setup_logging()
    try:
        adls_client = get_adls_client()
        ingest_dictionaries(adls_client)
        ingest_cases_by_municipality(adls_client)
        logging.info("--- ETL PROCESS COMPLETED ---")
    except ValueError as e:
        logging.critical(f"Configuration error: {e}")
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred: {e}")

if __name__ == "__main__":
    main()
