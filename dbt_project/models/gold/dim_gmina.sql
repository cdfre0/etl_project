{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT
    kod as gmina_kod,
    nazwa as gmina_nazwa,
    ROW_NUMBER() OVER(ORDER BY kod) as geografia_id
FROM {{ source('silver', 'slownik_gmina_siedziby') }}
