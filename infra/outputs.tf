output "storage_account_name" {
  description = "The name of the Azure Data Lake Storage account."
  value       = azurerm_storage_account.adls.name
}

output "storage_account_endpoint" {
  description = "The primary DFS endpoint of the ADLS account."
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
}

output "datalake_container_names" {
  description = "A map of the Data Lake container names."
  value       = { for k, v in azurerm_storage_container.datalake_containers : k => v.name }
}

output "data_factory_name" {
  description = "The name of the Azure Data Factory."
  value       = azurerm_data_factory.adf.name
}

output "key_vault_name" {
  description = "The name of the Azure Key Vault."
  value       = azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  description = "The URI of the Azure Key Vault."
  value       = azurerm_key_vault.kv.vault_uri
}
