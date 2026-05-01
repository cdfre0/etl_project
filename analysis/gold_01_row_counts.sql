-- Gold Layer — Table Row Counts
-- Quick sanity check: confirms all tables were populated after the pipeline run.

SELECT 'fact_przypadki_pomocy'   AS table_name, COUNT(*) AS row_count FROM gold.fact_przypadki_pomocy
UNION ALL
SELECT 'dim_beneficjent',                        COUNT(*) FROM gold.dim_beneficjent
UNION ALL
SELECT 'dim_gmina',                              COUNT(*) FROM gold.dim_gmina
UNION ALL
SELECT 'dim_data',                               COUNT(*) FROM gold.dim_data
UNION ALL
SELECT 'dim_charakterystyka',                    COUNT(*) FROM gold.dim_charakterystyka
UNION ALL
SELECT 'dim_udzielajacy_pomocy',                 COUNT(*) FROM gold.dim_udzielajacy_pomocy
ORDER BY table_name;
