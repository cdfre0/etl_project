-- Gold Layer — Top 10 Beneficiaries by Total Aid (PLN)
-- Identifies which companies/organisations received the most aid.

SELECT
    b.nazwa_beneficjenta,
    b.wielkosc_beneficjenta_nazwa  AS company_size,
    COUNT(*)                       AS number_of_cases,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
JOIN gold.dim_beneficjent b ON f.beneficjent_id = b.beneficjent_id
GROUP BY
    b.nazwa_beneficjenta,
    b.wielkosc_beneficjenta_nazwa
ORDER BY total_aid_pln DESC
LIMIT 10;
