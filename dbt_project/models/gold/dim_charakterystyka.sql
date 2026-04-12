{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH source AS (
    SELECT
        forma_pomocy_nazwa,
        przeznaczenie_pomocy_nazwa,
        sektor_dzialalnosci_nazwa
    FROM {{ source('silver', 'przypadki_pomocy') }}
),

unique_charakterystyka AS (
    SELECT DISTINCT
        forma_pomocy_nazwa,
        przeznaczenie_pomocy_nazwa,
        sektor_dzialalnosci_nazwa
    FROM source
)

SELECT
    ROW_NUMBER() OVER(ORDER BY forma_pomocy_nazwa, przeznaczenie_pomocy_nazwa) as charakterystyka_id,
    forma_pomocy_nazwa,
    przeznaczenie_pomocy_nazwa,
    sektor_dzialalnosci_nazwa
FROM unique_charakterystyka
