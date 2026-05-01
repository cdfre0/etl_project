-- Gold Layer — Full Join Sample (100 rows)
-- Joins all 5 dimensions to the fact table so you can see denormalised records end-to-end.

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

FROM gold.fact_przypadki_pomocy      f
LEFT JOIN gold.dim_beneficjent        b ON f.beneficjent_id        = b.beneficjent_id
LEFT JOIN gold.dim_udzielajacy_pomocy u ON f.udzielajacy_pomocy_id = u.udzielajacy_pomocy_id
LEFT JOIN gold.dim_charakterystyka    c ON f.charakterystyka_id    = c.charakterystyka_id
LEFT JOIN gold.dim_gmina              g ON f.geografia_id          = g.geografia_id
LEFT JOIN gold.dim_data               d ON f.date_id               = d.date_id
LIMIT 100;
