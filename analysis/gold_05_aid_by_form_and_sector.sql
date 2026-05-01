-- Gold Layer — Aid by Form and Sector
-- Breaks down aid by forma_pomocy (aid type) and sektor_dzialalnosci (business sector).

SELECT
    c.forma_pomocy_nazwa,
    c.sektor_dzialalnosci_nazwa,
    COUNT(*)                             AS number_of_cases,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
JOIN gold.dim_charakterystyka c ON f.charakterystyka_id = c.charakterystyka_id
WHERE c.forma_pomocy_nazwa IS NOT NULL
GROUP BY
    c.forma_pomocy_nazwa,
    c.sektor_dzialalnosci_nazwa
ORDER BY total_aid_pln DESC;
