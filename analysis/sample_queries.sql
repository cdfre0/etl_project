-- Sample SQL Queries for Gold Layer Analysis
-- Run in Databricks SQL Editor.
-- All tables are in the `gold` schema.

-- ==============================================================================
-- Query 1: Total Aid Amount by Municipality
-- ==============================================================================
SELECT
    g.gmina_nazwa                        AS municipality_name,
    SUM(f.wartosc_brutto_pln)            AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
LEFT JOIN gold.dim_gmina g ON f.geografia_id = g.geografia_id
GROUP BY g.gmina_nazwa
ORDER BY total_aid_pln DESC
LIMIT 20;

-- ==============================================================================
-- Query 2: Aid Distribution by Business Size and Sector
-- ==============================================================================
SELECT
    b.wielkosc_beneficjenta_nazwa        AS business_size,
    c.sektor_dzialalnosci_nazwa          AS business_sector,
    COUNT(*)                             AS number_of_grants,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
LEFT JOIN gold.dim_beneficjent     b ON f.beneficjent_id     = b.beneficjent_id
LEFT JOIN gold.dim_charakterystyka c ON f.charakterystyka_id = c.charakterystyka_id
WHERE b.wielkosc_beneficjenta_nazwa IS NOT NULL
  AND c.sektor_dzialalnosci_nazwa   IS NOT NULL
GROUP BY
    b.wielkosc_beneficjenta_nazwa,
    c.sektor_dzialalnosci_nazwa
ORDER BY business_size, total_aid_pln DESC;
