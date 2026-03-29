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

## Konfiguracja ręczna (Uprawnienia)

Ze względu na ograniczenia subskrypcji "Azure for Students", następujące przypisania ról muszą zostać wykonane ręcznie w portalu Azure po wdrożeniu infrastruktury przez Terraform.

**Wymagane identyfikatory (Principal IDs):**
- **Container App (`aca-etl-app`) Principal ID**: `3d94e9ab-c27a-4715-a264-2b32097f8c63`
- **Data Factory (`adf-etl-medallion-processor`) Principal ID**: Należy pobrać z portalu Azure:
  1. Przejdź do grupy zasobów `rg-etl-project-dev`.
  2. Otwórz fabrykę danych `adf-etl-medallion-processor`.
  3. W menu po lewej stronie, w sekcji "Ustawienia", kliknij **Tożsamość**.
  4. Skopiuj **Identyfikator obiektu (jednostki usługi)**.

### 1. Dostęp Container App do Storage Account
- **Zasób**: Konto magazynu `stetldatamedallion`
- **Rola**: `Storage Blob Data Contributor`
- **Przypisz do**: Tożsamość zarządzana (Managed Identity) -> Container App -> `aca-etl-app`

### 2. Dostęp Container App do Key Vault
- **Zasób**: Key Vault `kv-medallion-sec`
- **Rola**: `Key Vault Secrets User`
- **Przypisz do**: Tożsamość zarządzana (Managed Identity) -> Container App -> `aca-etl-app`

### 3. Dostęp Data Factory do Storage Account
- **Zasób**: Konto magazynu `stetldatamedallion`
- **Rola**: `Storage Blob Data Contributor`
- **Przypisz do**: Tożsamość zarządzana (Managed Identity) -> Data Factory -> `adf-etl-medallion-processor`
