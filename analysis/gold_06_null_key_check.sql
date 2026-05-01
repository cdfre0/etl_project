-- Gold Layer — Null Key Check (Data Quality)
-- Counts fact rows where a dimension join produced no match.
-- Any non-zero value in the unmatched_* columns indicates a data quality issue.

SELECT
    COUNT(*)                                                          AS total_facts,
    SUM(CASE WHEN f.beneficjent_id        IS NULL THEN 1 ELSE 0 END) AS unmatched_beneficjent,
    SUM(CASE WHEN f.udzielajacy_pomocy_id IS NULL THEN 1 ELSE 0 END) AS unmatched_udzielajacy,
    SUM(CASE WHEN f.charakterystyka_id    IS NULL THEN 1 ELSE 0 END) AS unmatched_charakterystyka,
    SUM(CASE WHEN f.geografia_id          IS NULL THEN 1 ELSE 0 END) AS unmatched_geografia,
    SUM(CASE WHEN f.date_id               IS NULL THEN 1 ELSE 0 END) AS unmatched_date
FROM gold.fact_przypadki_pomocy f;
