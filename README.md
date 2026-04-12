# ETL-Azure-Pipeline-SUDOP

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python, Docker, and Microsoft Azure services. The pipeline follows a Medallion Architecture (Bronze -> Silver -> Gold) to progressively refine data from the Polish SUDOP API into a structured, analytics-ready format.

## Medallion Architecture Overview

The pipeline is structured into three distinct layers, each serving a specific purpose:

```mermaid
graph TD
    A[SUDOP API] -->|ingest_sudop.py| B[(Bronze Layer: Raw JSON)];
    B -->|PySpark Data Cleanse| C[(Silver Layer: Clean Parquet)];
    C -->|dbt Build Models| D[(Gold Layer: Star Schema)];
    
    subgraph "Azure Databricks Execution"
        E(sudop_etl_bronze_to_silver.py);
        F(dbt_runner.py);
    end

    E -.->|Reads ADLS| B
    E -.->|Writes & Registers Metastore| C
    F -.->|Compiles via Databricks SQL| C
    F -.->|Materializes External Tables| D

    subgraph "Azure Data Factory"
        trigger((Schedule/Trigger)) --> E
        E --> F
    end

    style B fill:#CD7F32,stroke:#333
    style C fill:#C0C0C0,stroke:#333
    style D fill:#FFD700,stroke:#333
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

*   An Azure subscription
*   An Azure Storage Account with Data Lake Storage Gen2 enabled (`bronze`, `silver`, `gold` containers).
*   Azure Databricks Workspace (Standard or Premium)
*   Azure Data Factory

### Environment Setup

1.  Clone the repository.
2.  Create a file named `.env` in the project root.
3.  Add your Azure Storage connection string and Databricks configuration:
    ```
    AZURE_STORAGE_CONNECTION_STRING="your_storage_account_connection_string"
    DBT_DATABRICKS_HOST="adb-xxxxxx.azuredatabricks.net"
    DBT_DATABRICKS_HTTP_PATH="sql/protocolv1/o/..."
    DATABRICKS_TOKEN="your-databricks-personal-access-token"
    ```
4. Set up a Databricks Repo inside your Databricks workspace targeting your GitHub fork of this repository.

---

## Running the ETL Pipeline

This project relies on Azure Data Factory to orchestrate Databricks Notebooks and dbt!

### 1. Configure the Orchestrator
Inside Azure Data Factory Studio, create a Linked Service to Azure Databricks (Compute tab) using your `DATABRICKS_TOKEN`. 

### 2. Set Up Cluster Environment Variables
For secure execution of the pipeline (especially `dbt`), you must configure credentials natively into your cluster:
1. Navigate to your Databricks Cluster -> **Edit** -> **Advanced options** -> **Environment variables**.
2. Add the following keys (without quotes):
   ```text
   DBT_DATABRICKS_HOST=adb-xxxxxx.azuredatabricks.net
   DBT_DATABRICKS_HTTP_PATH=sql/protocolv1/o/...
   DATABRICKS_TOKEN=your_pat_token
   ```

### 3. Set up the Pipeline
Create a new pipeline in Azure Data Factory with two **Notebook Activities**:

#### Activity 1: Bronze to Silver (Spark)
- **Path**: `/Workspace/Users/your.email@domain.com/etl_project/src/databricks/sudop_etl_bronze_to_silver`
- This step reads raw JSON from the `bronze` container, applies data cleaning, outputs Delta Parquet tables into the `silver` container, and automatically registers them into the **Databricks Metastore** so downstream SQL engines can inherently discover them.

#### Activity 2: Silver to Gold (dbt)
- **Path**: `/Workspace/Users/your.email@domain.com/etl_project/src/databricks/dbt_runner`
- To maintain security and execution context in a scalable cloud setting, this notebook executes a Python `subprocess` to natively pull securely-passed cluster environment variables, and executes `dbt run` to dynamically build the analytical Star Schema as Delta tables!

### 4. Run It!
Click **Debug** or **Trigger Now** on the pipeline in Azure Data Factory to watch the highly scalable cluster process your data into ready-to-query models.

---

## Operational Showcase

Here are live examples of the pipeline executing beautifully in the cloud environment:

### Azure Data Factory Orchestration
Data Factory seamlessly triggers the two-notebook lineage (Bronze to Silver -> Silver to Gold).
![ADF Pipeline Run](assets/adf_pipeline.png)

### Databricks Execution
The exact jobs spun up correctly in the Databricks pipeline viewer without failure.
![Databricks Jobs Screen](assets/databricks_jobs.png)

### Gold Layer Analytics Ready
Once successfully built, the Databricks SQL catalog natively exposes the generated Star Schema to powerful queries!
![Databricks SQL Table View](assets/databricks_sql.png)

## Data Quality & Business Checks (Testing)

Our robust Databricks implementation uses Databricks-native constraints and dbt validation!

### Automated Validation (dbt test)
Since our Databricks python execution runner utilizes an enterprise `subprocess` execution model, you can safely enforce schema data quality in Azure Data Factory simply by appending a `dbt test` command inside `src/databricks/dbt_runner.py`:

```python
from dbt.cli.main import dbtRunner, dbtRunnerResult

# Run data quality tests synchronously using Databricks programmatic Python API
dbt_project_dir = "../../dbt_project"
dbt = dbtRunner()
result: dbtRunnerResult = dbt.invoke(["test", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir])
```
This guarantees foreign keys, unique row integrity, and null checks on your Gold layer models!

### Business Checks (Databricks SQL)
To definitively verify that the Gold pipeline processed your data accurately, open the **Databricks SQL Editor** and execute the following Business Verification checks:

**1. Verification of the Central Fact Table Aggregation:**
Ensure millions of rows correctly aggregated to the dimension mappings without null leakage.
```sql
SELECT 
    COUNT(*) as Total_Facts,
    SUM(wartosc_brutto_pln) as Total_Financial_Aid_Disbursed
FROM default.fact_przypadki_pomocy;
```

**2. Verifying the Business Value (Top Beneficiaries Check):**
Validate that the dimension relationships accurately surface real Polish corporate data.
```sql
SELECT 
    b.nazwa_beneficjenta,
    b.wielkosc_beneficjenta_nazwa as Company_Size,
    COUNT(f.beneficjent_id) as Number_Of_Aid_Cases,
    CAST(SUM(f.wartosc_brutto_pln) AS DECIMAL(18,2)) as Total_Aid_PLN
FROM default.fact_przypadki_pomocy f
JOIN default.dim_beneficjent b ON f.beneficjent_id = b.beneficjent_id
GROUP BY b.nazwa_beneficjenta, b.wielkosc_beneficjenta_nazwa
ORDER BY Total_Aid_PLN DESC
LIMIT 10;
```

If these queries immediately return calculated data, your PySpark Databricks pipeline is successfully registering tables dynamically and producing optimal Business Intelligence!

---

## Infrastructure as Code (Terraform)

The Azure infrastructure for this project is fully defined and managed using Terraform. The configuration files are located in the `infra/` directory.

### Deployed Resources

The Terraform script provisions the following key Azure resources:

*   **Resource Group:** A logical container for all project resources (`rg-etl-project-dev`).
*   **Azure Data Lake Storage Gen2:** The core storage for all data layers, with `bronze`, `silver`, and `gold` containers.
*   **Azure Data Factory:** Visual orchestrator spanning Databricks workflows.
*   **Azure Databricks Workspace:** A scalable Spark compute engine dynamically provisioned for data modeling.

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

