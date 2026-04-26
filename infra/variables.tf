variable "resource_group_name" {
  description = "Name of the resource group."
  type        = string
  default     = "rg-etl-project-dev"
}

variable "location" {
  description = "Azure region where resources will be deployed."
  type        = string
  default     = "germanywestcentral"
}

variable "storage_account_name" {
  description = "Name of the Azure Storage Account for ADLS Gen2."
  type        = string
  default     = "stetldatamedallion" # Note: This will be suffixed with a random string to ensure uniqueness
}

variable "data_factory_name" {
  description = "Name of the Azure Data Factory."
  type        = string
  default     = "adf-etl-medallion-processor" # Note: This will be suffixed with a random string to ensure uniqueness
}

variable "databricks_workspace_name" {
  description = "Name of the Azure Databricks workspace."
  type        = string
  default     = "dbw-etl-medallion"
}

variable "key_vault_name" {
  description = "Name of the Azure Key Vault."
  type        = string
  default     = "kv-medallion-sec" # Note: This will be suffixed with a random string to ensure uniqueness
}

variable "api_key_secret_name" {
  description = "Name of the secret for the CEIDG API key."
  type        = string
  default     = "CEIDG-API-KEY"
}

variable "container_registry_name" {
  description = "Name of the Azure Container Registry."
  type        = string
  default     = "acretlmedallion"
}

variable "container_app_environment_name" {
  description = "The name of the container app environment (kept for potential future use)."
  type        = string
  default     = "aca-env-etl"
}

variable "subscription_id" {
  description = "The Azure subscription ID."
  type        = string
}

variable "client_id" {
  description = "The client ID for the service principal."
  type        = string
}

variable "client_secret" {
  description = "The client secret for the service principal."
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "The Azure tenant ID."
  type        = string
}
