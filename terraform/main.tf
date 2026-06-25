terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-prime-capital-tfstate"
    storage_account_name = "pcbtfstate"
    container_name       = "tfstate"
    key                  = "prime-capital-bank.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

variable "subscription_id"   { type = string }
variable "location"          { type = string; default = "southafricanorth" }
variable "env"               { type = string; default = "prod" }
variable "admin_password"    { type = string; sensitive = true }
variable "alert_email"       { type = string }

locals {
  prefix = "prime-capital"
  tags = {
    project     = "Prime Capital Bank"
    environment = var.env
    managed_by  = "Terraform"
  }
}

# ─── Resource Group ───────────────────────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "rg-${local.prefix}-${var.env}"
  location = var.location
  tags     = local.tags
}

# ─── ADLS Gen2 Storage ────────────────────────────────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = "primecapitaladls"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical namespace = Data Lake Gen2
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "containers" {
  for_each           = toset(["raw", "bronze", "silver", "gold", "ml-models", "archive", "regulatory-reports"])
  name               = each.key
  storage_account_id = azurerm_storage_account.adls.id
}

# ─── Key Vault ────────────────────────────────────────────────────────────────
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "kv-${local.prefix}-${var.env}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"
  soft_delete_retention_days = 90
  tags                = local.tags
}

resource "azurerm_key_vault_access_policy" "deployer" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id
  secret_permissions = ["Get", "List", "Set", "Delete", "Recover", "Backup", "Restore"]
}

resource "azurerm_key_vault_secret" "storage_key" {
  name         = "adls-storage-key"
  value        = azurerm_storage_account.adls.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_key_vault_access_policy.deployer]
}

# ─── Azure Databricks Workspace ───────────────────────────────────────────────
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.prefix}-${var.env}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"
  tags                = local.tags

  custom_parameters {
    no_public_ip       = true
    storage_account_name = "dbstorage${local.prefix}"
  }
}

# ─── Azure SQL Database (for Power BI DirectQuery) ────────────────────────────
resource "azurerm_mssql_server" "reporting" {
  name                         = "sql-${local.prefix}-reporting"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = "pcbadmin"
  administrator_login_password = var.admin_password
  tags                         = local.tags
}

resource "azurerm_mssql_database" "reporting" {
  name           = "pcb-reporting"
  server_id      = azurerm_mssql_server.reporting.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  sku_name       = "BC_Gen5_8"
  zone_redundant = true
  tags           = local.tags
}

# ─── Log Analytics (monitoring) ───────────────────────────────────────────────
resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${local.prefix}-${var.env}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 90
  tags                = local.tags
}

# ─── Diagnostic Settings ──────────────────────────────────────────────────────
resource "azurerm_monitor_diagnostic_setting" "databricks" {
  name               = "diag-databricks"
  target_resource_id = azurerm_databricks_workspace.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  enabled_log { category = "accounts" }
  enabled_log { category = "clusters" }
  enabled_log { category = "jobs" }
  enabled_log { category = "notebook" }
  enabled_log { category = "sqlPermissions" }
}

# ─── Outputs ──────────────────────────────────────────────────────────────────
output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.main.workspace_url}"
}
output "storage_account_name" {
  value = azurerm_storage_account.adls.name
}
output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}
output "sql_server_fqdn" {
  value = azurerm_mssql_server.reporting.fully_qualified_domain_name
}
output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}
