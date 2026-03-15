# Architektura ETL dla Azure

Ten projekt implementuje proces ETL (Extract, Transform, Load) z wykorzystaniem Azure.

## Warstwy danych

- **Bronze**: Surowe dane, pobrane bezpośrednio ze źródeł. Dane są niezmienione.
- **Silver**: Oczyszczone i zwalidowane dane z warstwy bronze. Na tej warstwie odbywa się transformacja i standaryzacja danych.
- **Gold**: Zagregowane dane, gotowe do analizy i raportowania.
