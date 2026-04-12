{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH source AS (
    SELECT
        nip_udzielajacego_pomocy,
        nazwa_udzielajacego_pomocy
    FROM {{ source('silver', 'przypadki_pomocy') }}
),

unique_udzielajacy AS (
    SELECT DISTINCT
        nip_udzielajacego_pomocy,
        nazwa_udzielajacego_pomocy
    FROM source
)

SELECT
    ROW_NUMBER() OVER(ORDER BY nip_udzielajacego_pomocy) as udzielajacy_pomocy_id,
    nip_udzielajacego_pomocy,
    nazwa_udzielajacego_pomocy
FROM unique_udzielajacy
