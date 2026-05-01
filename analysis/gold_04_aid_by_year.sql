-- Gold Layer — Aid Volume by Year
-- Shows how the number of cases and total disbursed amount trends over time.

SELECT
    YEAR(d.dzien_udzielenia_pomocy)      AS rok,
    COUNT(*)                             AS number_of_cases,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
JOIN gold.dim_data d ON f.date_id = d.date_id
GROUP BY rok
ORDER BY rok DESC;
