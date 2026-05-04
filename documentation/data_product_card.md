# Data Product Card: SUDOP Public Aid Data Product

## 1. What the Product Is
The **SUDOP Public Aid Data Product** is a highly structured, analytical data mart (Star Schema) representing public financial aid disbursed to enterprises in Poland. It processes raw data from the Polish Office of Competition and Consumer Protection (UOKiK) into a query-optimized Gold layer format.

## 2. What Problem it Solves
Tracking public subsidies is historically difficult due to fragmented data sources, nested JSON structures, and inconsistent reporting. This data product brings **transparency to state subsidies**. It allows BI analysts, journalists, and government auditors to instantly answer critical questions such as:
- *Which companies receive the most state aid?*
- *How are subsidies distributed geographically (by municipality)?*
- *What is the breakdown of aid by business size and economic sector?*

## 3. Where the Data Comes From
- **Source:** The official UOKiK SUDOP REST API (*System Udostępniania Danych o Pomocy Publicznej*).
- **Lineage:** 
  1. **Bronze:** Raw JSON events ingested via a Python/Kafka pipeline into Azure Data Lake.
  2. **Silver:** Cleaned, tabular Parquet files processed by PySpark on Databricks.
  3. **Gold:** Analytical Star Schema (Delta Tables) built via `dbt`.

## 4. How to Access It (Frictionless out-of-the-box!)
No local downloads or Databricks credentials are required! You can query the live Databricks Delta Tables directly over the internet.

- **Platform:** Direct Azure Storage Access (via DuckDB)
- **Location:** `azure://gold@stetldatamedallion.blob.core.windows.net/`
- **Connection Method:** Use DuckDB with the `azure` and `delta` extensions to query the data remotely using the provided read-only Shared Access Signature (SAS) token.
  - See the included `demo_usage.py` script for a plug-and-play example! DuckDB is smart enough to push down your filters and only download the bytes it needs over the network.

**Git Repository & Contract:** [Link to your GitHub Repo] / `data_product_contract.yaml`

---

## 5. Schema Overview
The product follows a standard Dimensional Modeling (Star Schema) approach:

### Fact Table: `fact_przypadki_pomocy`
Contains the numeric measures (financial aid values) and foreign keys connecting to descriptive dimensions.
- `wartosc_nominalna_pln`: Nominal aid value (PLN)
- `wartosc_brutto_pln`: Gross aid value (PLN)
- `wartosc_brutto_eur`: Gross aid value (EUR)

### Dimension Tables
- **`dim_beneficjent`**: Who received the aid (Name, NIP, Business Size).
- **`dim_udzielajacy_pomocy`**: Who granted the aid (Entity Name, NIP).
- **`dim_gmina`**: Geography (Municipality Code, Name).
- **`dim_charakterystyka`**: What kind of aid (Form, Purpose, Sector).
- **`dim_data`**: When the aid was granted (Date Hierarchy).

---

## 6. Key Data Quality Metrics

| Metric Name | Definition | Current Value | Expected Threshold | Update Cadence |
| :--- | :--- | :--- | :--- | :--- |
| **Completeness of Financials** | % of rows where `wartosc_brutto_pln` is not null | ~100% | > 99.9% | Every pipeline run |
| **Uniqueness of Beneficiaries** | % of unique `beneficjent_id` values in `dim_beneficjent` | 100% | 100% | Every pipeline run |
| **Referential Consistency** | % of fact rows with a valid `geografia_id` matching `dim_gmina` | ~100% | > 99.0% | Every pipeline run |

---

## 7. Known Limitations
- **API Delays:** Historical SUDOP API data may experience delays or retroactive corrections by reporting entities. The Gold layer reflects the state of the API at the time of the daily ingestion run.
- **Null Metadata:** Beneficiary sizing metadata (e.g., categorizing an entity as "micro" or "large") is dependent on the granting entity's report and may occasionally contain NULLs or "BRAK DANYCH" (No Data).

## 8. Contact & Ownership
- **Product Owner:** Jan (ETL Project Team)
- **Support Channel:** Data Engineering MS Teams Channel
- **Feedback:** Please raise an Issue in the project Git repository.

---
*Created for the MS Teams Data Marketplace.*
