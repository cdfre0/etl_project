#!/usr/bin/env python3
"""Mock SUDOP ETL pipeline for testing without Azure connectivity."""

import json
import os
from datetime import datetime

import pandas as pd


def create_mock_sudop_dictionary_data():
    """Create mock SUDOP dictionary data for testing."""
    data = [
        {"name": "NOWY WIŚNICZ", "number": "1201063"},
        {"name": "BOCHNIA", "number": "1201011"},
    ]
    return pd.DataFrame(data)


def mock_ingest_bronze(df):
    """Mock bronze layer ingestion."""
    print("Ingesting to bronze layer...")
    print(f"Processing {len(df)} records...")

    df["_ingested_at"] = datetime.now().isoformat()
    df["_source"] = "sudop_api_mock"

    bronze_path = "data/bronze/slownik_gmina_siedziby_mock.json"
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)

    with open(bronze_path, "w", encoding="utf-8") as file_handle:
        json.dump(df.to_dict("records"), file_handle, indent=2, ensure_ascii=False)

    print(f"Saved {len(df)} records to bronze layer")
    return bronze_path


def mock_transform_bronze_to_curated(bronze_path):
    """Mock transformation from bronze to curated."""
    print("Transforming bronze to curated...")

    with open(bronze_path, "r", encoding="utf-8") as file_handle:
        data = json.load(file_handle)

    df = pd.DataFrame(data)
    df["slownik"] = "gmina_siedziby"
    df["_curated_at"] = datetime.utcnow().isoformat()

    curated_path = "data/curated/slownik_gmina_siedziby_mock.json"
    os.makedirs(os.path.dirname(curated_path), exist_ok=True)

    with open(curated_path, "w", encoding="utf-8") as file_handle:
        json.dump(df.to_dict("records"), file_handle, indent=2, ensure_ascii=False)

    print(f"Transformed and saved {len(df)} records to curated layer")
    return curated_path


def run_mock_etl():
    """Run complete mock ETL pipeline."""
    print("Starting mock SUDOP ETL pipeline")
    print("=" * 50)

    try:
        raw_data = create_mock_sudop_dictionary_data()
        bronze_path = mock_ingest_bronze(raw_data)
        curated_path = mock_transform_bronze_to_curated(bronze_path)

        print("\n" + "=" * 50)
        print("Mock ETL pipeline completed successfully")
        print("=" * 50)
        print(f"Bronze data: {bronze_path}")
        print(f"Curated data: {curated_path}")

        with open(curated_path, "r", encoding="utf-8") as file_handle:
            sample = json.load(file_handle)[:1]

        print(f"\nSample curated record: {json.dumps(sample[0], indent=2, ensure_ascii=False)}")
        return True

    except Exception as e:
        print(f"ETL pipeline failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = run_mock_etl()
    exit(0 if success else 1)
