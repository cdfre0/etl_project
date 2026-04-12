{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH source AS (
    SELECT
        nip_beneficjenta,
        nazwa_beneficjenta,
        wielkosc_beneficjenta_nazwa
    FROM {{ source('silver', 'przypadki_pomocy') }}
),

unique_beneficjenci AS (
    SELECT DISTINCT
        nip_beneficjenta,
        nazwa_beneficjenta,
        wielkosc_beneficjenta_nazwa
    FROM source
)

SELECT
    ROW_NUMBER() OVER(ORDER BY nip_beneficjenta, nazwa_beneficjenta) as beneficjent_id,
    nip_beneficjenta,
    nazwa_beneficjenta,
    wielkosc_beneficjenta_nazwa
FROM unique_beneficjenci
