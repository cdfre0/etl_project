{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('silver', 'przypadki_pomocy') }}
),

dim_beneficjent AS (
    SELECT * FROM {{ ref('dim_beneficjent') }}
),

dim_udzielajacy_pomocy AS (
    SELECT * FROM {{ ref('dim_udzielajacy_pomocy') }}
),

dim_charakterystyka AS (
    SELECT * FROM {{ ref('dim_charakterystyka') }}
),

dim_gmina AS (
    SELECT * FROM {{ ref('dim_gmina') }}
),

dim_data AS (
    SELECT * FROM {{ ref('dim_data') }}
)

SELECT
    s.wartosc_nominalna_pln,
    s.wartosc_brutto_pln,
    s.wartosc_brutto_eur,
    
    b.beneficjent_id,
    u.udzielajacy_pomocy_id,
    c.charakterystyka_id,
    g.geografia_id,
    d.date_id
FROM source s
LEFT JOIN dim_beneficjent b 
    ON s.nip_beneficjenta = b.nip_beneficjenta 
    AND s.nazwa_beneficjenta = b.nazwa_beneficjenta 
    AND s.wielkosc_beneficjenta_nazwa = b.wielkosc_beneficjenta_nazwa
LEFT JOIN dim_udzielajacy_pomocy u 
    ON s.nip_udzielajacego_pomocy = u.nip_udzielajacego_pomocy 
    AND s.nazwa_udzielajacego_pomocy = u.nazwa_udzielajacego_pomocy
LEFT JOIN dim_charakterystyka c 
    ON s.forma_pomocy_nazwa = c.forma_pomocy_nazwa 
    AND s.przeznaczenie_pomocy_nazwa = c.przeznaczenie_pomocy_nazwa 
    AND s.sektor_dzialalnosci_nazwa = c.sektor_dzialalnosci_nazwa
LEFT JOIN dim_gmina g 
    ON s.gmina_siedziby_kod = g.gmina_kod
LEFT JOIN dim_data d 
    ON TO_DATE(s.dzien_udzielenia_pomocy) = d.dzien_udzielenia_pomocy
