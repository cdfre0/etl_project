import os
import requests
import json
import time
import logging
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

def get_adls_client():
    """Authenticates and returns a DataLakeServiceClient."""
    load_dotenv()
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
        
    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    return service_client

import sys
from azure.core.exceptions import HttpResponseError

# Enable SDK logging to stdout
logger = logging.getLogger('azure.storage.file.datalake')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

def save_json_to_adls(service_client, container_name, file_path, data):
    """Saves a JSON object to a specified path within an ADLS container."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Ensure the directory exists
        directory_path = os.path.dirname(file_path)
        if directory_path:
            try:
                # This will create the directory if it doesn't exist.
                # If it exists, it does nothing.
                directory_client = file_system_client.create_directory(directory_path)
            except Exception as e:
                # It's possible for this to fail if the directory already exists
                # in a way that the SDK considers an error, so we can often ignore it.
                if "PathAlreadyExists" not in str(e):
                    raise e

        file_client = file_system_client.get_file_client(file_path)
        
        json_data = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        
        file_client.upload_data(json_data, overwrite=True, logging_enable=True)
        
        print(f"Successfully saved '{file_path}' to container '{container_name}'.")
        
    except HttpResponseError as e:
        # This prints the specific server-side error detail
        print(f"--- AZURE ERROR DETAIL ---")
        print(f"Status Code: {e.status_code}")
        print(f"Error Code: {e.error_code}")
        print(f"Message: {e.message}")
        if hasattr(e, 'response') and e.response.body():
            print(f"Response Body: {e.response.body().decode('utf-8')}")
    except Exception as e:
        print(f"General Error: {e}")

def fetch_and_save_dictionary(service_client, dictionary_name):
    """Fetches data from a SUDOP dictionary endpoint and saves it to ADLS."""
    base_url = "https://api-sudop.uokik.gov.pl/sudop-api/slownik/"
    endpoint = dictionary_name.replace("_", "-")
    url = f"{base_url}{endpoint}"
    
    print(f"Fetching data from: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = f"slownik_{dictionary_name}_{timestamp}.json"
        file_path = f"dictionaries/{file_name}"
        
        save_json_to_adls(service_client, "bronze", file_path, data)
        return True
    except requests.exceptions.HTTPError as errh:
        print(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Oops: Something Else: {err}")
    return False

def read_json_from_adls(service_client, container_name, file_name):
    """Reads and parses a JSON file from ADLS."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        file_client = file_system_client.get_file_client(file_name)
        
        download = file_client.download_file()
        data = json.loads(download.readall())
        print(f"Successfully read '{file_name}' from container '{container_name}'.")
        return data
    except Exception as e:
        print(f"Error reading file from ADLS: {e}")
        return None

def get_latest_dictionary_file(service_client, container_name, dictionary_prefix):
    """Finds the most recent dictionary file in ADLS."""
    try:
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        paths = file_system_client.get_paths()
        latest_file = None
        latest_time = None

        # We need to look inside the 'dictionaries' subdirectory
        paths = file_system_client.get_paths(path="dictionaries")

        for path in paths:
            if os.path.basename(path.name).startswith(dictionary_prefix):
                # Extract timestamp from filename, e.g., dictionaries/slownik_gmina_siedziby_20260329_123000.json
                try:
                    file_name_only = os.path.basename(path.name)
                    timestamp_str = file_name_only.split('_')[-2] + "_" + file_name_only.split('_')[-1].split('.')[0]
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    if latest_time is None or file_time > latest_time:
                        latest_time = file_time
                        latest_file = path.name # Keep the full path e.g. 'dictionaries/...'
                except (IndexError, ValueError):
                    continue # Ignore files that don't match the expected format
        
        return latest_file
    except Exception as e:
        print(f"Error listing files in ADLS: {e}")
        return None

def fetch_and_save_cases(service_client, search_param, param_value):
    """
    Fetches aid cases based on a search parameter, handles the queue, and saves the result.
    """
    base_url = "https://api-sudop.uokik.gov.pl/sudop-api/api/przypadki-pomocy"
    queue_base_url = "https://api-sudop.uokik.gov.pl/sudop-api/api/kolejka/"
    
    query_url = f"{base_url}?{search_param}={param_value}"
    print(f"Initiating search with URL: {query_url}")

    try:
        # Step 1: Initial request. Allow redirects to see where it leads.
        response = requests.get(query_url, allow_redirects=True)
        response.raise_for_status()

        queue_id = None

        # Check if the API redirected us directly to the queue
        if 'kolejka' in response.url:
            queue_id = response.url.split('/')[-1]
        else:
            # If not, try to parse the JSON response for the queue ID
            try:
                queue_id = response.json().get("id-kolejka")
            except json.JSONDecodeError:
                print("Initial response was not JSON. This can happen with this API. Continuing without queue ID.")


        if not queue_id:
            print(f"Could not get queue ID for {search_param}={param_value}.")
            # It might be that for some parameters there is no data, so it is not an error.
            try:
                if response.json().get("wyniki") is not None and len(response.json().get("wyniki")) == 0:
                    print("No results found for this parameter, skipping.")
                    return True
            except json.JSONDecodeError:
                 print("Response was not JSON and no queue ID was found. Skipping.")
                 return True
            return False

        print(f"Got queue ID: {queue_id}. Starting to poll.")
        queue_url = f"{queue_base_url}{queue_id}"
        
        # Step 2: Poll the queue URL until data is ready
        for i in range(15): # Poll a maximum of 10 times (e.g., 10 * 15s = 150s)
            time.sleep(10) # Wait before checking the queue status
            print(f"Polling attempt {i+1}/10: {queue_url}")
            queue_response = requests.get(queue_url)
            queue_response.raise_for_status()
            queue_data = queue_response.json()

            if queue_data.get("wyniki"):
                print("Data is ready, saving results.")
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"przypadki_pomocy_{search_param}_{param_value}_{timestamp}.json"
                file_path = f"cases/{file_name}"
                save_json_to_adls(service_client, "bronze", file_path, queue_data)
                return True
        
        print(f"Polling timed out for queue ID {queue_id}.")
        return False

    except requests.exceptions.HTTPError as errh:
        # A 404 here might just mean no data for the query, which is not a critical error.
        if errh.response.status_code == 404:
            print(f"No results found for {search_param}={param_value} (404). Skipping.")
            return True
        print(f"Http Error: {errh}")
    except Exception as e:
        print(f"An unexpected error occurred during case fetching: {e}")
    
    return False

def main():
    """Main function to ingest all SUDOP dictionary data."""

    # Enable detailed Azure SDK logging
    logger = logging.getLogger('azure')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    dictionaries = [
        "forma_pomocy",
        "przeznaczenie_pomocy",
        "srodek_pomocowy",
        "sektor_dzialalnosci",
        "gmina_siedziby"
    ]
    
    try:
        adls_client = get_adls_client()
        
        # --- Stage 1: Ingest Dictionaries (if they are old or missing) ---
        print("--- STARTING STAGE 1: DICTIONARY INGESTION ---")
        for dictionary in dictionaries:
            dictionary_prefix = f"slownik_{dictionary}"
            latest_file = get_latest_dictionary_file(adls_client, "bronze", dictionary_prefix)
            
            # Check if a recent file exists (e.g., within the last 24 hours)
            if latest_file:
                file_time_str = latest_file.split('_')[-2] + "_" + latest_file.split('_')[-1].split('.')[0]
                file_time = datetime.strptime(file_time_str, "%Y%m%d_%H%M%S")
                if datetime.utcnow() - file_time < timedelta(hours=24):
                    print(f"Recent dictionary '{latest_file}' already exists. Skipping download.")
                    continue

            fetch_and_save_dictionary(adls_client, dictionary)
            print("Waiting for 1 second...")
            time.sleep(1)
        
        # --- Stage 2: Ingest Cases by Municipality ---
        print("\n--- STARTING STAGE 2: CASE INGESTION BY MUNICIPALITY ---")
        latest_gminy_file = get_latest_dictionary_file(adls_client, "bronze", "slownik_gmina_siedziby")

        if latest_gminy_file:
            gminy_data = read_json_from_adls(adls_client, "bronze", latest_gminy_file)
            if gminy_data:
                for i, gmina in enumerate(gminy_data):
                    gmina_kod = gmina.get("number")
                    gmina_name = gmina.get("name")
                    print(f"\nProcessing municipality {i+1}/{len(gminy_data)}: {gmina_name} ({gmina_kod})")
                    if gmina_kod:
                        fetch_and_save_cases(adls_client, "gmina-siedziby-kod", gmina_kod)
                        # The fetch_and_save_cases function has its own internal delay,
                        # so no extra sleep is needed here to respect the rate limit.
        else:
            print("Could not find the municipality dictionary file. Skipping case ingestion.")

            
    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
