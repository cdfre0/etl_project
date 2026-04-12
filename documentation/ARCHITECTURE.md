# High-Level Architecture

This document provides a high-level overview of the components and data flow in the SUDOP ETL pipeline.

## System Architecture Diagram

The architecture follows a classic ETL pattern orchestrated within a Docker environment and deployed on Azure infrastructure.

```mermaid
graph TD
    subgraph "Infrastructure Provisioning"
        TF[Terraform] -- Provisions --> AzureResources;
    end

    subgraph "AzureResources" [Azure Cloud Environment]
        subgraph "Orchestration"
            ADF[Azure Data Factory]
        end

        subgraph "Data Storage"
            DLake[Azure Data Lake Storage Gen2];
            B[Bronze: Raw JSON]
            S[Silver: Delta Tables]
            G[Gold: Star Schema Delta]
            DLake --> B & S & G
        end

        subgraph "Scalable Processing Engine"
            ADB[Azure Databricks]
            PYSPARK[PySpark Notebook]
            DBT[dbt Models]
            ADB --> PYSPARK
            ADB --> DBT
        end
    end

    subgraph "External Source"
        API[SUDOP API]
    end

    API -- "1. Ingest" --> B
    ADF -- "Triggers" --> PYSPARK
    PYSPARK -- "2. Clean & Transform" --> B
    PYSPARK -- "Writes" --> S
    ADF -- "Triggers" --> DBT
    DBT -- "3. Star Schema Modeling" --> S
    DBT -- "Writes" --> G

    style B fill:#CD7F32,stroke:#333,stroke-width:2px
    style S fill:#C0C0C0,stroke:#333,stroke-width:2px
    style G fill:#FFD700,stroke:#333,stroke-width:2px
    style ADF fill:#0078D4,stroke:#333,stroke-width:2px,color:#fff
    style ADB fill:#FF3621,stroke:#333,stroke-width:2px,color:#fff
```

## Component Breakdown

1.  **Orchestration (Azure Data Factory):** The central orchestrator that triggers and monitors the end-to-end data pipeline, kicking off Databricks notebooks.
2.  **Azure Infrastructure (Managed by Terraform):**
    *   **Azure Data Lake Storage (ADLS) Gen2:** The core of our Medallion Architecture.
    *   **Azure Databricks:** Scalable Apache Spark environment for transformation.
3.  **ETL Scripts:**
    *   **Bronze Ingestion (Python Container):** Connects to the external SUDOP API, handles rate limiting, and lands raw JSON in the `bronze` container.
    *   **Silver Transformation (PySpark notebook):** Reads from the bronze layer, cleans data idempotently, and structures it into Delta tables in the `silver` container.
    *   **Gold Transformation (dbt on Databricks):** Declarative SQL models run via dbt to build the final star schema (fact and dim tables) as Delta tables in `gold`.

1.  The **Bronze** job runs, pulling data from the SUDOP API and storing it as raw JSON in ADLS.
2.  The **Silver** job runs, reading the raw JSON, cleaning it, and saving it as structured Parquet files.
3.  The **Gold** job runs, reading the clean Parquet files and building the final dimensional model for analytics.
