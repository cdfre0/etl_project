# High-Level Architecture

This document provides a high-level overview of the components and data flow in the SUDOP ETL pipeline.

## System Architecture Diagram

The architecture follows a classic ETL pattern orchestrated within a Docker environment and deployed on Azure infrastructure.

```mermaid
graph TD
    subgraph "Local Development / CI/CD"
        A[Developer] -- Interacts via --> C[Docker Compose];
        C -- Builds --> I[ETL Docker Image];
        I -- Pushed to --> ACR[Azure Container Registry];
    end

    subgraph "Azure Cloud Environment"
        subgraph "ETL Execution"
            ACA[Azure Container App] -- Pulls image from --> ACR;
            ACA -- Runs ETL jobs --> DLake;
        end

        subgraph "Data Storage (Medallion Architecture)"
            DLake[Azure Data Lake Storage Gen2];
            DLake -- Contains --> B[Bronze Container];
            DLake -- Contains --> S[Silver Container];
            DLake -- Contains --> G[Gold Container];
        end

        subgraph "External Source"
            API[SUDOP API];
        end
    end

    subgraph "Data Flow"
        API -- 1. Ingestion --> B;
        B -- "2. Transformation (Clean & Structure)" --> S;
        S -- "3. Transformation (Model & Aggregate)" --> G;
    end
    
    style B fill:#CD7F32,stroke:#333,stroke-width:2px
    style S fill:#C0C0C0,stroke:#333,stroke-width:2px
    style G fill:#FFD700,stroke:#333,stroke-width:2px
```

## Component Breakdown

1.  **Docker Environment:**
    *   **Docker Compose:** The primary tool for local development and orchestration. It defines three separate services (`bronze-ingest`, `curated-transform`, `gold-transform`) that can be run independently or together.
    *   **Dockerfile:** Defines the environment for the ETL application, installing Python, Azure CLI, Terraform, and all necessary Python libraries.

2.  **Azure Infrastructure (Managed by Terraform):**
    *   **Azure Data Lake Storage (ADLS) Gen2:** The core of our data platform, separated into three containers (`bronze`, `silver`, `gold`) that correspond to the layers of the Medallion Architecture.
    *   **Azure Container Registry (ACR):** A private Docker registry used to store the ETL application image, making it ready for cloud execution.
    *   **Azure Container App (ACA):** A serverless container runtime that executes our ETL jobs. It pulls the image from ACR and is configured with the necessary environment variables (like the storage connection string) to run.

3.  **ETL Scripts (Python):**
    *   **Bronze (`ingest_sudop.py`):** Connects to the external SUDOP API, handles rate limiting and retries, and lands the raw JSON data in the `bronze` container.
    *   **Silver (`build_curated_sudop.py`):** Reads from the bronze layer, cleans the data, casts data types, and structures it into separate, queryable Parquet tables in the `silver` container based on the `metadata.json` contract.
    *   **Gold (`build_gold_layer.py`):** Reads from the silver layer and builds a final, analytics-ready star schema, writing the `fact` and `dim` tables as Parquet files to the `gold` container.

## Data Flow Summary

1.  The **Bronze** job runs, pulling data from the SUDOP API and storing it as raw JSON in ADLS.
2.  The **Silver** job runs, reading the raw JSON, cleaning it, and saving it as structured Parquet files.
3.  The **Gold** job runs, reading the clean Parquet files and building the final dimensional model for analytics.
