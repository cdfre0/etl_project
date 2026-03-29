# Data Quality Risks and Mitigation Strategies

This document outlines potential data quality risks identified in the SUDOP ETL pipeline and the strategies implemented to mitigate them.

### 1. Risk: Source API Unavailability and Rate Limiting

*   **Description:** The primary source of data is the external SUDOP API. This API could become unavailable due to maintenance, network issues, or other external factors. Furthermore, making too many requests in a short period can lead to `429 (Too Many Requests)` errors, which would result in incomplete or missing data if not handled correctly.
*   **Impact:** High. If the API is down or rate-limiting our requests, the entire ingestion process can fail, leading to stale or incomplete datasets.
*   **Mitigation Strategy:**
    *   **Resilience and Retries:** The `ingest_sudop.py` script has a built-in retry mechanism in its `make_request` function.
    *   **Backoff Strategy:** When a `429` error is detected, the script automatically waits for a configurable period (`RETRY_BACKOFF_SECONDS`) before retrying the request. This respects the API's limits and allows the process to recover gracefully.
    *   **Configurable Retries:** The number of retries (`MAX_RETRIES`) is configurable, allowing for tuning the process based on API stability.

### 2. Risk: Inconsistent or Invalid Source Data

*   **Description:** The data coming from the API is not guaranteed to be perfectly clean. During analysis, we identified that the `gmina_siedziby` (municipality) dictionary contains entries with invalid or non-applicable names, such as "NZ" or "BRAK DANYCH". Using this data as-is would lead to incorrect groupings and failed lookups in the Gold layer.
*   **Impact:** Medium. While this won't break the pipeline, it would corrupt the analytical value of the data, leading to inaccurate reports.
*   **Mitigation Strategy:**
    *   **Explicit Filtering:** The `ingest_cases_by_municipality` function in the bronze script now contains logic to explicitly filter out and skip any municipality records where the name is "NZ" or "BRAK DANYCH".
    *   **Defensive Transformations:** During the Silver layer transformation, data types are explicitly cast (e.g., to numeric, date), and errors are coerced, meaning that if a value cannot be converted, it is gracefully set to `null` instead of causing the entire process to fail.

### 3. Risk: Schema Drift

*   **Description:** The SUDOP API is an external system, and its owners could change the structure of the JSON response at any time without notice. They might add, remove, or rename fields. This is known as schema drift.
*   **Impact:** High. If a field we rely on is renamed or removed (e.g., `wartosc_brutto_pln`), the Silver and Gold transformation scripts would fail, breaking the entire pipeline downstream.
*   **Mitigation Strategy:**
    *   **Schema-on-Read and Metadata-Driven Pipeline:** Our Silver transformation (`build_curated_sudop.py`) is driven by the `metadata.json` file. This file acts as a contract for the expected schema. The script explicitly selects and renames columns based on this metadata.
    *   **Graceful Failure and Logging:** The script will fail with clear errors if a key column is missing, but for non-critical columns, it will gracefully continue. The column selection process ensures that unexpected new columns from the source are simply dropped, preventing them from breaking the pipeline, while missing columns are added as `null`. This makes the pipeline resilient to non-breaking schema changes.
    *   **Future Improvement (Monitoring):** For a production system, schema validation tests could be added that compare the incoming data structure against the metadata contract and send an alert if a breaking change is detected.

### 4. Risk: AI-Generated Code and Lack of Human Oversight

*   **Description:** The entire ETL pipeline, from ingestion to the final gold layer, was developed by an AI assistant. While AI can accelerate development significantly, it can also introduce subtle logical errors, miss edge cases, or generate code that is not fully optimized or secure without rigorous human review.
*   **Impact:** Medium to High. A subtle bug in the transformation logic could lead to silent data corruption that is not immediately obvious. For example, an incorrect join condition or a misinterpretation of a business rule could lead to inaccurate aggregations in the gold layer.
*   **Mitigation Strategy:**
    *   **Human-in-the-Loop:** Every code change and architectural decision made by the AI was reviewed and approved by a human developer. This iterative feedback loop is crucial for catching errors.
    *   **Unit and Integration Testing (Future Improvement):** A robust testing suite should be developed to validate the output of each ETL stage against known inputs. This would programmatically verify that the transformations are correct.
    *   **Code Audits:** The generated code should be periodically audited by experienced data engineers to check for performance bottlenecks, security vulnerabilities, and adherence to best practices.
    *   **Data Profiling and Monitoring:** Automated data profiling on the silver and gold layers can help detect anomalies (e.g., a sudden drop in record counts, unexpected null values) that might indicate an underlying issue with the AI-generated code.
