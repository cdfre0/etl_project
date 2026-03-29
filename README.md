# ETL-Azure-Pipeline-SUDOP

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python, Docker, and Microsoft Azure services. The pipeline follows a Medallion Architecture (Bronze -> Silver -> Gold) to progressively refine data from the Polish SUDOP API into a structured, analytics-ready format.

## Medallion Architecture Overview

The pipeline is structured into three distinct layers, each serving a specific purpose:

```mermaid
graph TD
    A[SUDOP API] --> B[Bronze Layer];
    B --> C[Silver Layer];
    C --> D[Gold Layer];
    D --> E[Analytics / BI Tools];

    subgraph "Azure Data Lake Storage"
        B(Raw JSON Files);
        C(Cleaned Parquet Files);
        D(Star Schema Parquet);
    end

    style B fill:#CD7F32,stroke:#333,stroke-width:2px
    style C fill:#C0C0C0,stroke:#333,stroke-width:2px
    style D fill:#FFD700,stroke:#333,stroke-width:2px
```

1.  **Bronze Layer (Raw Data):**
    *   **Purpose:** Ingests raw, unaltered data directly from the source. This ensures data lineage and allows for rebuilding subsequent layers without re-fetching from the source API.
    *   **Format:** JSON files (one per dictionary, one per case file download).
    *   **Location:** `bronze` container in Azure Data Lake.

2.  **Silver Layer (Curated Data):**
    *   **Purpose:** Cleans, validates, and structures the raw data. This layer represents a single source of truth for clean, queryable data.
    *   **Format:** Columnar Parquet files for efficiency.
    *   **Structure:**
        *   `przypadki_pomocy.parquet`: A single, consolidated table of all aid cases.
        *   `slownik_gmina_siedziby.parquet`, `slownik_forma_pomocy.parquet`, etc.: Separate tables for each dictionary.

3.  **Gold Layer (Analytical Data):**
    *   **Purpose:** Creates a highly refined, aggregated data model optimized for business intelligence and analytics.
    *   **Format:** Columnar Parquet files.
    *   **Structure:** A **Star Schema** with a central fact table and multiple dimension tables, perfect for BI tools like Power BI or Tableau.
        *   `fact_przypadki_pomocy`: Contains numeric measures and keys.
        *   `dim_beneficjent`, `dim_data`, `dim_geografia`, etc.: Descriptive dimension tables.

---

## Getting Started

### Prerequisites

*   Docker and Docker Compose
*   An Azure subscription
*   An Azure Storage Account with Data Lake Storage Gen2 enabled.

### Environment Setup

1.  Clone the repository.
2.  Create a file named `.env` in the project root.
3.  Add your Azure Storage connection string to this file:
    ```
    AZURE_STORAGE_CONNECTION_STRING="your_storage_account_connection_string"
    ```
4. Make sure you have `bronze`, `silver`, and `gold` containers in your storage account.

---

## Running the ETL Pipeline

The entire pipeline is managed through Docker Compose and is divided into profiles, allowing you to run each stage independently.

### 1. Run Bronze Ingestion

This step connects to the SUDOP API, downloads the dictionary and case files, and uploads them as raw JSON to the `bronze` container.

```bash
docker compose --profile bronze up --build
```

**Smart Skip Feature:** To avoid re-downloading data you've recently fetched, you can set the `SKIP_IF_EXISTS=true` environment variable. It will skip the download if the newest file in the `bronze/dictionaries` folder is less than 24 hours old.

```bash
# Example for PowerShell
$env:SKIP_IF_EXISTS="true"; docker compose --profile bronze up

# Example for bash
SKIP_IF_EXISTS=true docker compose --profile bronze up
```

### 2. Run Silver (Curated) Transformation

This step reads the raw JSON from the `bronze` container, cleans and structures it according to the rules in `src/curated/metadata.json`, and saves the output as Parquet files in the `silver` container.

```bash
docker compose --profile curated up --build
```

### 3. Run Gold (Analytical) Transformation

This step reads the clean Parquet files from the `silver` container, builds the star schema (fact and dimension tables), and saves them as Parquet files in the `gold` container.

```bash
docker compose --profile gold up --build
```

### Run the Full Pipeline

To run all three stages in sequence (Bronze -> Silver -> Gold), use the `all` profile.

```bash
docker compose --profile all up --build
```

---

## Running the Analysis

After running the full pipeline, you can execute a sample analysis script that demonstrates how to query and use the gold layer tables.

This script connects to your data lake, loads the `fact_przypadki_pomocy` and `dim_geografia` tables, and calculates the total aid amount for the top 20 municipalities.

```bash
docker compose --profile analysis up --build
```

---

## Infrastructure as Code (Terraform)

The Azure infrastructure for this project is fully defined and managed using Terraform. The configuration files are located in the `infra/` directory.

### Deployed Resources

The Terraform script provisions the following key Azure resources:

*   **Resource Group:** A logical container for all project resources (`rg-etl-project-dev`).
*   **Azure Data Lake Storage Gen2:** The core storage for all data layers, with `bronze`, `silver`, and `gold` containers created automatically.
*   **Azure Container Registry:** To store and manage the Docker image for the ETL application.
*   **Azure Container App Environment:** A dedicated and isolated environment to run the containerized ETL jobs.
*   **Azure Container App:** The application that runs the ETL code from the Docker image.
*   **Azure Key Vault:** For securely storing secrets like the storage connection string (though not fully automated in this version).
*   **Azure Data Factory:** Included for future orchestration needs (currently not used by the Docker-based pipeline).

### How to Deploy the Infrastructure

#### Prerequisites

1.  [Install Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli).
2.  [Install Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) and authenticate with your Azure account:
    ```bash
    az login
    ```
3. Set your subscription
    ```bash
    az account set --subscription "My Subscription"
    ```

#### Deployment Steps

1.  **Navigate to the infra directory:**
    ```bash
    cd infra
    ```
2.  **Initialize Terraform:**
    This will download the necessary provider plugins.
    ```bash
    terraform init
    ```
3.  **Create a `terraform.tfvars` file:**
    Create this file in the `infra/` directory and add the required variable values. You can get these from your Azure subscription and Service Principal details:
    ```hcl
    subscription_id = "your-azure-subscription-id"
    client_id       = "your-service-principal-client-id"
    client_secret   = "your-service-principal-client-secret"
    tenant_id       = "your-azure-tenant-id"
    ```
4.  **Plan the deployment:**
    This will show you what resources Terraform will create.
    ```bash
    terraform plan
    ```
5.  **Apply the configuration:**
    This will create the resources in Azure.
    ```bash
    terraform apply
    ```

After applying, Terraform will output the names and details of the created resources.

---

## Project Documentation

For more detailed information, please refer to the documents in the `documentation/` directory:

*   **[Architecture Overview](./documentation/ARCHITECTURE.md):** A high-level diagram and description of the system components and data flow.
*   **[Data Quality Risks](./documentation/DATA_QUALITY_RISKS.md):** An analysis of potential data quality issues and the strategies used to mitigate them.

## Analytics

The final, analytics-ready tables are stored in the `gold` container. For sample queries that demonstrate how to join the fact and dimension tables to answer business questions, see the `analysis/` directory:

*   **[Sample Queries](./analysis/sample_queries.sql):** Example SQL queries for use in an analytics platform like Azure Synapse, Databricks, or DuckDB.
