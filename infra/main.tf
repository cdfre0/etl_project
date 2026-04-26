data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_storage_container" "datalake_containers" {
  for_each              = toset(["bronze", "silver", "gold"])
  name                  = each.key
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_data_factory" "adf" {
  name                = var.data_factory_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_databricks_workspace" "dbw" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_data_factory_linked_service_azure_databricks" "adf_to_dbw" {
  name            = "ADFLinkedServiceAzureDatabricks"
  data_factory_id = azurerm_data_factory.adf.id
  description     = "Linked service for Databricks."
  adb_domain      = "https://${azurerm_databricks_workspace.dbw.workspace_url}"

  # Connect to the manually created Databricks 
  access_token = "databricks_env_token"

  existing_cluster_id = "0411-104634-ir763chs"
}

resource "azurerm_key_vault" "kv" {
  name                       = var.key_vault_name
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7

  access_policy {
    tenant_id           = data.azurerm_client_config.current.tenant_id
    object_id           = data.azurerm_client_config.current.object_id
    key_permissions     = ["Get"]
    secret_permissions  = ["Get", "List", "Set", "Delete"]
    storage_permissions = ["Get"]
  }

  access_policy {
    tenant_id          = data.azurerm_client_config.current.tenant_id
    object_id          = azurerm_data_factory.adf.identity[0].principal_id
    secret_permissions = ["Get", "List"]
  }

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_key_vault_secret" "api_key" {
  name         = var.api_key_secret_name
  value        = "dummy-api-key-to-be-set-manually"
  key_vault_id = azurerm_key_vault.kv.id
}

/*
resource "azurerm_role_assignment" "adf_to_adls" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}
*/
