#!/usr/bin/env python3
"""
Common, reusable functions for the ETL pipeline, including Azure Data Lake 
connectivity, file I/O, and logging setup.
"""

import io
import json
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

# --- LOGGING SETUP ---

def setup_logging():
    """Configures a standardized logger for the application."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Quiet down the verbose Azure SDK loggers
    logging.getLogger('azure.storage.filedatalake').setLevel(logging.WARNING)
    logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

# --- AZURE DATA LAKE HELPER FUNCTIONS ---

def get_adls_client(connection_string: str) -> DataLakeServiceClient:
    """Authenticates and returns a DataLakeServiceClient."""
    if not connection_string:
        raise ValueError("Azure Storage connection string is not set.")
    try:
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        logging.info("ADLS service client created successfully.")
        return service_client
    except (ValueError, TypeError) as e:
        logging.error(f"Invalid connection string. Please check your .env file. Error: {e}")
        raise

def list_adls_files(service_client: DataLakeServiceClient, container: str, directory: str, file_extension: str = ".json") -> list[str]:
    """Lists all files with a given extension in a specified ADLS directory."""
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        paths = fs_client.get_paths(path=directory)
        file_list = [path.name for path in paths if not path.is_directory and path.name.endswith(file_extension)]
        logging.info(f"Found {len(file_list)} '{file_extension}' files in '{container}/{directory}'.")
        return file_list
    except Exception as e:
        logging.error(f"Error listing files in '{container}/{directory}': {e}")
        return []

def read_json_from_adls(service_client: DataLakeServiceClient, container: str, file_path: str) -> dict | list | None:
    """Reads and parses a JSON file from ADLS."""
    logging.info(f"Reading JSON file: {container}/{file_path}")
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        file_client = fs_client.get_file_client(file_path)
        download = file_client.download_file()
        return json.loads(download.readall())
    except ResourceNotFoundError:
        logging.error(f"File not found: {container}/{file_path}")
        return None
    except Exception as e:
        logging.error(f"Error reading JSON from '{container}/{file_path}': {e}")
        return None

def read_parquet_from_adls(service_client: DataLakeServiceClient, container: str, path: str) -> pd.DataFrame | None:
    """Reads a Parquet file from ADLS into a pandas DataFrame."""
    logging.info(f"Reading Parquet file: {container}/{path}")
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        file_client = fs_client.get_file_client(path)
        
        download = file_client.download_file()
        file_bytes = download.readall()
        
        table = pq.read_table(io.BytesIO(file_bytes))
        return table.to_pandas()
    except ResourceNotFoundError:
        logging.error(f"Parquet file not found: {container}/{path}")
        return None
    except Exception as e:
        logging.error(f"Error reading Parquet from '{container}/{path}': {e}")
        return None

def save_df_as_parquet_to_adls(service_client: DataLakeServiceClient, container: str, table_name: str, df: pd.DataFrame):
    """Saves a DataFrame as a Parquet file to ADLS under a directory named after the table."""
    if df.empty:
        logging.warning(f"DataFrame for table '{table_name}' is empty, skipping save.")
        return
        
    file_path = f"{table_name}/{table_name}.parquet"
    logging.info(f"Saving DataFrame to '{container}/{file_path}'")
    try:
        fs_client = service_client.get_file_system_client(file_system=container)
        directory_path = Path(file_path).parent.as_posix()
        fs_client.create_directory(directory_path)
            
        file_client = fs_client.get_file_client(file_path)
        
        # Use pandas to_parquet with Arrow as the engine
        buf = io.BytesIO()
        df.to_parquet(buf, engine='pyarrow')
        
        file_client.upload_data(buf.getvalue(), overwrite=True)
        logging.info(f"Successfully saved '{file_path}' to container '{container}'.")
    except Exception as e:
        logging.error(f"Error saving Parquet to '{container}/{file_path}': {e}")
        raise
