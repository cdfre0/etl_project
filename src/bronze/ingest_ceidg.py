
import os
import requests
import json
from datetime import date

# --- Configuration ---
API_BASE_URL = "https://dane.biznes.gov.pl/pl/portal/ceidg/v2/firmy"
API_KEY = os.environ.get("CEIDG_API_KEY")
VOIVODESHIPS_TO_FETCH = ["mazowieckie", "podlaskie"]
OUTPUT_BASE_PATH = "data/bronze/ceidg"

def fetch_ceidg_data(voivodeship: str, processing_date: str) -> dict:
    """
    Fetches company data for a given voivodeship and date from the CEIDG API.

    Args:
        voivodeship: The name of the voivodeship to filter by.
        processing_date: The start date for the data fetch (YYYY-MM-DD).

    Returns:
        A dictionary containing the JSON response from the API.
    
    Raises:
        requests.exceptions.HTTPError: If the API returns a non-200 status code.
        ValueError: If the API_KEY is not set.
    """
    if not API_KEY:
        raise ValueError("CEIDG_API_KEY environment variable not set.")

    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {
        "wojewodztwo": voivodeship,
        "dataod": processing_date
    }
    
    print(f"Fetching data for voivodeship: {voivodeship}, date: {processing_date}...")
    response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=60)
    response.raise_for_status()  # Raise an exception for bad status codes
    print(f"Successfully fetched data for {voivodeship}.")
    return response.json()

def save_raw_data(data: dict, voivodeship: str, processing_date: str):
    """
    Saves the raw JSON data to a file in the bronze layer.

    Args:
        data: The data to save (as a Python dictionary).
        voivodeship: The voivodeship the data belongs to.
        processing_date: The date the data was processed for.
    """
    output_path = os.path.join(OUTPUT_BASE_PATH, processing_date)
    os.makedirs(output_path, exist_ok=True)
    
    file_path = os.path.join(output_path, f"{voivodeship}.json")
    
    print(f"Saving data to {file_path}...")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Successfully saved data to {file_path}.")

def main():
    """
    Main function to run the daily ingestion process.
    """
    processing_date = date.today().isoformat()
    print(f"--- Starting CEIDG ingestion for date: {processing_date} ---")

    for voivodeship in VOIVODESHIPS_TO_FETCH:
        try:
            company_data = fetch_ceidg_data(voivodeship, processing_date)
            if company_data.get("firmy"): # Check if the response contains data
                 save_raw_data(company_data, voivodeship, processing_date)
            else:
                print(f"No new companies found for {voivodeship} on {processing_date}.")
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching data for {voivodeship}: {e}")
        except ValueError as e:
            print(f"Configuration error: {e}")
            break # Stop execution if API key is missing
        except Exception as e:
            print(f"An unexpected error occurred for {voivodeship}: {e}")
            
    print("--- CEIDG ingestion finished ---")

if __name__ == "__main__":
    main()
