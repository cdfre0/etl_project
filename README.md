# Azure ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline using a medallion architecture on Azure.

## Architecture

The data is processed through three layers:

-   **Bronze**: Raw data ingested from source systems. No transformations are applied.
-   **Silver**: Cleaned, validated, and enriched data.
-   **Gold**: Aggregated data ready for analytics and reporting.

## Directory Structure

-   `data/`: Contains sample data for each layer.
    -   `bronze/`
    -   `silver/`
    -   `gold/`
-   `src/`: Contains the source code for the ETL jobs.
    -   `bronze/`: Scripts for ingesting data into the bronze layer.
    -   `silver/`: Scripts for transforming data from bronze to silver.
    -   `gold/`: Scripts for aggregating data from silver to gold.
