output "storage_account" {
  value = azurerm_storage_account.this.name
}

output "key_vault_name" {
  value = azurerm_key_vault.this.name
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.this.fully_qualified_domain_name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "data_factory_principal_id" {
  value = azurerm_data_factory.this.identity[0].principal_id
}
