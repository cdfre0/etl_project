{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH source AS (
    SELECT DISTINCT
        TO_DATE(dzien_udzielenia_pomocy) as full_date
    FROM {{ source('silver', 'przypadki_pomocy') }}
    WHERE dzien_udzielenia_pomocy IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER(ORDER BY full_date) as date_id,
    full_date as dzien_udzielenia_pomocy,
    YEAR(full_date) as year,
    MONTH(full_date) as month,
    DAY(full_date) as day,
    QUARTER(full_date) as quarter,
    -- Spark SQL specific functions
    DATE_FORMAT(full_date, 'EEEE') as day_of_week,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN true ELSE false END as is_weekend
FROM source
