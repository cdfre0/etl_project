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

resource "azurerm_key_vault" "kv" {
  name                        = var.key_vault_name
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
  
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    key_permissions     = ["Get"]
    secret_permissions  = ["Get", "List", "Set", "Delete"]
    storage_permissions = ["Get"]
  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_data_factory.adf.identity[0].principal_id
    secret_permissions = ["Get", "List"]
  }

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_container_registry" "acr" {
  name                = var.container_registry_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Basic"
  admin_enabled       = true

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_container_app_environment" "aca_env" {
  name                = var.container_app_environment_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

resource "azurerm_container_app" "etl_app" {
  name                         = var.container_app_name
  container_app_environment_id = azurerm_container_app_environment.aca_env.id
  resource_group_name          = azurerm_resource_group.rg.name
  revision_mode                = "Single"

  secret {
    name  = "acr-password"
    value = azurerm_container_registry.acr.admin_password
  }

  registry {
    server               = azurerm_container_registry.acr.login_server
    username             = azurerm_container_registry.acr.admin_username
    password_secret_name = "acr-password"
  }

  template {
    container {
      name   = "etl-container"
      image  = "${azurerm_container_registry.acr.login_server}/etl-app:latest"
      cpu    = 0.25
      memory = "0.5Gi"
    }
  }

  ingress {
    external_enabled = true
    target_port      = 80
    traffic_weight {
      percentage = 100
      latest_revision = true
    }
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = "dev"
    project     = "etl_medallion"
  }
}

/*
resource "azurerm_role_assignment" "storage_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_container_app.etl_app.identity[0].principal_id
}

resource "azurerm_role_assignment" "kv_secrets_user" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_container_app.etl_app.identity[0].principal_id
}
*/

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
