# Architektura ETL dla Azure

Ten projekt implementuje proces ETL (Extract, Transform, Load) z wykorzystaniem Azure.

## Warstwy danych

- **Bronze**: Surowe dane, pobrane bezpośrednio ze źródeł. Dane są niezmienione.
- **Silver**: Oczyszczone i zwalidowane dane z warstwy bronze. Na tej warstwie odbywa się transformacja i standaryzacja danych.
- **Gold**: Zagregowane dane, gotowe do analizy i raportowania.

## Infrastruktura (IaC)

Infrastruktura została wdrożona przy użyciu Terraform. Wszystkie zasoby znajdują się w grupie zasobów `rg-etl-project-dev`.

**Szczegóły wdrożonych zasobów (nazwy, endpointy) znajdują się w pliku `infra_outputs.json`.**

### Kluczowe komponenty:
- **Azure Data Lake Storage Gen2**: Konto magazynu z kontenerami `bronze`, `silver` i `gold`.
- **Azure Data Factory**: Instancja do orkiestracji potoków ETL.
- **Azure Key Vault**: Magazyn do bezpiecznego przechowywania sekretów, takich jak klucze API.

### Dostęp i uprawnienia:
Tożsamość zarządzana (Managed Identity) instancji Azure Data Factory posiada uprawnienia `Storage Blob Data Contributor` do konta magazynu oraz uprawnienia do odczytu (`Get/List`) sekretów w Key Vault.

## Następne kroki (ETL)

Agent ETL powinien wykorzystać informacje z `infra_outputs.json` do konfiguracji połączeń i potoków danych.
