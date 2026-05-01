
-- Gold Layer Overview Queries
-- Run these in Databricks SQL Editor to inspect all gold tables.

-- 1. Row counts per table
SELECT 'fact_przypadki_pomocy'     AS table_name, COUNT(*) AS row_count FROM gold.fact_przypadki_pomocy
UNION ALL
SELECT 'dim_beneficjent',                          COUNT(*) FROM gold.dim_beneficjent
UNION ALL
SELECT 'dim_gmina',                                COUNT(*) FROM gold.dim_gmina
UNION ALL
SELECT 'dim_data',                                 COUNT(*) FROM gold.dim_data
UNION ALL
SELECT 'dim_charakterystyka',                      COUNT(*) FROM gold.dim_charakterystyka
UNION ALL
SELECT 'dim_udzielajacy_pomocy',                   COUNT(*) FROM gold.dim_udzielajacy_pomocy;


-- 2. Full fact table joined to all dimensions (sample 100 rows)
SELECT
    f.wartosc_nominalna_pln,
    f.wartosc_brutto_pln,
    f.wartosc_brutto_eur,

    b.nip_beneficjenta,
    b.nazwa_beneficjenta,
    b.wielkosc_beneficjenta_nazwa,

    u.nip_udzielajacego_pomocy,
    u.nazwa_udzielajacego_pomocy,

    c.forma_pomocy_nazwa,
    c.przeznaczenie_pomocy_nazwa,
    c.sektor_dzialalnosci_nazwa,

    g.gmina_kod,
    g.gmina_nazwa,

    d.dzien_udzielenia_pomocy

FROM gold.fact_przypadki_pomocy f
LEFT JOIN gold.dim_beneficjent        b ON f.beneficjent_id        = b.beneficjent_id
LEFT JOIN gold.dim_udzielajacy_pomocy u ON f.udzielajacy_pomocy_id = u.udzielajacy_pomocy_id
LEFT JOIN gold.dim_charakterystyka    c ON f.charakterystyka_id    = c.charakterystyka_id
LEFT JOIN gold.dim_gmina              g ON f.geografia_id          = g.geografia_id
LEFT JOIN gold.dim_data               d ON f.date_id               = d.date_id
LIMIT 100;


-- 3. Top 10 beneficiaries by total aid disbursed
SELECT
    b.nazwa_beneficjenta,
    b.wielkosc_beneficjenta_nazwa  AS company_size,
    COUNT(*)                       AS number_of_cases,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
JOIN gold.dim_beneficjent b ON f.beneficjent_id = b.beneficjent_id
GROUP BY b.nazwa_beneficjenta, b.wielkosc_beneficjenta_nazwa
ORDER BY total_aid_pln DESC
LIMIT 10;


-- 4. Aid by year
SELECT
    YEAR(d.dzien_udzielenia_pomocy) AS rok,
    COUNT(*)                        AS number_of_cases,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
JOIN gold.dim_data d ON f.date_id = d.date_id
GROUP BY rok
ORDER BY rok DESC;


-- 5. Aid by aid type (forma pomocy)
SELECT
    c.forma_pomocy_nazwa,
    COUNT(*)                        AS number_of_cases,
    ROUND(SUM(f.wartosc_brutto_pln), 2) AS total_aid_pln
FROM gold.fact_przypadki_pomocy f
JOIN gold.dim_charakterystyka c ON f.charakterystyka_id = c.charakterystyka_id
GROUP BY c.forma_pomocy_nazwa
ORDER BY total_aid_pln DESC;


-- 6. Null key check — how many facts have unmatched dimension keys
SELECT
    COUNT(*) AS total_facts,
    SUM(CASE WHEN f.beneficjent_id        IS NULL THEN 1 ELSE 0 END) AS unmatched_beneficjent,
    SUM(CASE WHEN f.udzielajacy_pomocy_id IS NULL THEN 1 ELSE 0 END) AS unmatched_udzielajacy,
    SUM(CASE WHEN f.charakterystyka_id    IS NULL THEN 1 ELSE 0 END) AS unmatched_charakterystyka,
    SUM(CASE WHEN f.geografia_id          IS NULL THEN 1 ELSE 0 END) AS unmatched_geografia,
    SUM(CASE WHEN f.date_id               IS NULL THEN 1 ELSE 0 END) AS unmatched_date
FROM gold.fact_przypadki_pomocy f;
