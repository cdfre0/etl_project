-- Sample SQL Queries for Gold Layer Analysis
--
-- These queries are designed to run on a modern data platform that can query
-- Parquet files directly from a data lake (e.g., Azure Synapse Analytics,
-- Databricks, or a local environment with DuckDB).
--
-- For these examples, assume the Parquet files from the 'gold' container have
-- been registered as tables named `fact_przypadki_pomocy`, `dim_geografia`, etc.

-- ==============================================================================
-- Query 1: Total Aid Amount by Municipality
-- ==============================================================================
--
-- Problem: We want to identify which municipalities have received the highest
--          total aid amount in PLN.
--
-- Join: This query joins the fact table with the geography dimension to enrich
--       the data with municipality names.
-- Aggregation: It calculates the sum of `wartosc_brutto_pln` for each municipality.

SELECT
    dg.gmina_nazwa AS municipality_name,
    SUM(fpp.wartosc_brutto_pln) AS total_aid_pln
FROM
    fact_przypadki_pomocy AS fpp
LEFT JOIN
    dim_geografia AS dg ON fpp.geografia_id = dg.geografia_id
GROUP BY
    dg.gmina_nazwa
ORDER BY
    total_aid_pln DESC
LIMIT 20;

-- ==============================================================================
-- Query 2: Aid Distribution by Business Size and Sector
-- ==============================================================================
--
-- Problem: We want to understand how aid is distributed across different business
--          sizes (micro, small, large) and business sectors.
--
-- Join: This query joins the fact table with the beneficiary and aid
--       characteristic dimensions.
-- Aggregation: It counts the number of aid instances and sums the total amount.

SELECT
    db.wielkosc_beneficjenta_nazwa AS business_size,
    dc.sektor_dzialalnosci_nazwa AS business_sector,
    COUNT(*) AS number_of_grants,
    SUM(fpp.wartosc_brutto_pln) AS total_aid_pln
FROM
    fact_przypadki_pomocy AS fpp
LEFT JOIN
    dim_beneficjent AS db ON fpp.beneficjent_id = db.beneficjent_id
LEFT JOIN
    dim_charakterystyka AS dc ON fpp.charakterystyka_id = dc.charakterystyka_id
WHERE
    db.wielkosc_beneficjenta_nazwa IS NOT NULL
    AND dc.sektor_dzialalnosci_nazwa IS NOT NULL
GROUP BY
    db.wielkosc_beneficjenta_nazwa,
    dc.sektor_dzialalnosci_nazwa
ORDER BY
    business_size,
    total_aid_pln DESC;
