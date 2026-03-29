#!/usr/bin/env python3
"""
Mock ETL pipeline for testing without Azure connectivity
"""
import os
import pandas as pd
import json
from datetime import datetime

def create_mock_ceidg_data():
    """Create mock CEIDG data for testing"""
    data = [
        {
            "REGON": "123456789",
            "Nazwa": "Test Company Sp. z o.o.",
            "Adres": "ul. Testowa 1, 00-001 Warszawa",
            "DataPowstania": "2020-01-15",
            "Status": "Aktywny"
        },
        {
            "REGON": "987654321",
            "Nazwa": "Another Company S.A.",
            "Adres": "ul. Przykładowa 2, 30-001 Kraków",
            "DataPowstania": "2019-03-20",
            "Status": "Aktywny"
        }
    ]
    return pd.DataFrame(data)

def mock_ingest_bronze(df):
    """Mock bronze layer ingestion"""
    print("📥 INGESTING TO BRONZE LAYER...")
    print(f"Processing {len(df)} records...")

    # Add metadata
    df['_ingested_at'] = datetime.now().isoformat()
    df['_source'] = 'ceidg_api_mock'

    # Save to "bronze" (local file for now)
    bronze_path = "data/bronze/ceidg_data.json"
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)

    with open(bronze_path, 'w', encoding='utf-8') as f:
        json.dump(df.to_dict('records'), f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {len(df)} records to bronze layer")
    return bronze_path

def mock_transform_bronze_to_curated(bronze_path):
    """Mock transformation from bronze to curated"""
    print("🔄 TRANSFORMING BRONZE TO CURATED...")

    # Load bronze data
    with open(bronze_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Simple transformations
    df['WiekFirmy'] = (pd.Timestamp.now() - pd.to_datetime(df['DataPowstania'])).dt.days // 365
    df['Miasto'] = df['Adres'].str.extract(r'(\d{2}-\d{3}\s+[^,]+)', expand=False)
    df['StatusNormalized'] = df['Status'].str.lower()

    # Save to "curated"
    curated_path = "data/curated/ceidg_curated.json"
    os.makedirs(os.path.dirname(curated_path), exist_ok=True)

    with open(curated_path, 'w', encoding='utf-8') as f:
        json.dump(df.to_dict('records'), f, indent=2, ensure_ascii=False)

    print(f"✅ Transformed and saved {len(df)} records to curated layer")
    return curated_path

def run_mock_etl():
    """Run complete mock ETL pipeline"""
    print("🚀 STARTING MOCK ETL PIPELINE")
    print("=" * 50)

    try:
        # Bronze layer
        raw_data = create_mock_ceidg_data()
        bronze_path = mock_ingest_bronze(raw_data)

        # Curated layer
        curated_path = mock_transform_bronze_to_curated(bronze_path)

        print("\n" + "=" * 50)
        print("✅ MOCK ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        print(f"Bronze data: {bronze_path}")
        print(f"Curated data: {curated_path}")

        # Show sample results
        with open(curated_path, 'r', encoding='utf-8') as f:
            sample = json.load(f)[:1]

        print(f"\nSample curated record: {json.dumps(sample[0], indent=2, ensure_ascii=False)}")

        return True

    except Exception as e:
        print(f"❌ ETL Pipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = run_mock_etl()
    exit(0 if success else 1)