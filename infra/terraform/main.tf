# =============================================================================
# Healthcare RCM Lakehouse — declarative infrastructure (Terraform / azurerm)
#
# Terraform equivalent of infra/main.bicep & infra/provision.ps1. Creates a
# resource group and the core resources: ADLS Gen2 (+ containers), Key Vault,
# Azure SQL serverless, Databricks, Data Factory.
#
# Usage:
#   cd infra/terraform
#   terraform init
#   terraform apply -var="sql_admin_password=<strong-pwd>"
#
# NOT covered (kept imperative / out-of-band): resource-provider registration,
# Key Vault secret VALUES, role assignments, budget alerts, client-IP firewall.
# =============================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
  }
}

provider "azurerm" {
  features {}
}

data "azurerm_client_config" "current" {}

locals {
  storage_name    = "strcmdemo${var.suffix}"
  key_vault_name  = "kv-rcm-demo-${var.suffix}"
  sql_server_name = "sqlrcm${var.suffix}"
  databricks_name = "dbw-rcm-demo"
  adf_name        = "adf-rcm-demo-${var.suffix}"
  containers      = ["landing", "bronze", "silver", "gold", "quarantine", "checkpoints", "metadata"]
  tenant_id       = var.tenant_id != "" ? var.tenant_id : data.azurerm_client_config.current.tenant_id
  kv_admin_oid    = var.kv_admin_object_id != "" ? var.kv_admin_object_id : data.azurerm_client_config.current.object_id
}

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
}

# --------------------------------------------------------------------------- ADLS Gen2
resource "azurerm_storage_account" "this" {
  name                     = local.storage_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # hierarchical namespace = ADLS Gen2
  min_tls_version          = "TLS1_2"
}

resource "azurerm_storage_container" "containers" {
  for_each              = toset(local.containers)
  name                  = each.value
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

# --------------------------------------------------------------------------- Key Vault (access-policy model)
resource "azurerm_key_vault" "this" {
  name                      = local.key_vault_name
  resource_group_name       = azurerm_resource_group.this.name
  location                  = azurerm_resource_group.this.location
  tenant_id                 = local.tenant_id
  sku_name                  = "standard"
  enable_rbac_authorization = false

  access_policy {
    tenant_id          = local.tenant_id
    object_id          = local.kv_admin_oid
    secret_permissions = ["Get", "List", "Set", "Delete"]
  }
}

# --------------------------------------------------------------------------- Azure SQL (serverless, auto-pause)
resource "azurerm_mssql_server" "this" {
  name                         = local.sql_server_name
  resource_group_name          = azurerm_resource_group.this.name
  location                     = var.sql_location
  version                      = "12.0"
  administrator_login          = var.sql_admin_user
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"
}

resource "azurerm_mssql_firewall_rule" "allow_azure" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.this.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_database" "emr" {
  name                        = "emr_source"
  server_id                   = azurerm_mssql_server.this.id
  sku_name                    = "GP_S_Gen5_1" # General Purpose serverless, 1 vCore
  auto_pause_delay_in_minutes = 60            # strict: 60 = Azure minimum
  min_capacity                = 0.5
}

# --------------------------------------------------------------------------- Databricks workspace
resource "azurerm_databricks_workspace" "this" {
  name                = local.databricks_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "premium" # required for Unity Catalog + secret scopes
}

# --------------------------------------------------------------------------- Data Factory
resource "azurerm_data_factory" "this" {
  name                = local.adf_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }
}
