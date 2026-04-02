###############################################################
# FinBank S.A. — Outputs de Terraform
# Exporta ARNs, URLs y nombres de todos los recursos creados
# para que los scripts del pipeline puedan consumirlos.
###############################################################

output "resource_group_name" {
  description = "Nombre del Resource Group principal."
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Nombre del Storage Account ADLS Gen2."
  value       = module.adls.storage_account_name
}

output "storage_account_id" {
  description = "ID del Storage Account ADLS Gen2."
  value       = module.adls.storage_account_id
}

output "adls_dfs_endpoint" {
  description = "Endpoint DFS del ADLS Gen2 (para montaje en Databricks)."
  value       = module.adls.dfs_endpoint
}

output "bronze_container_name" {
  description = "Nombre del contenedor Bronze."
  value       = module.adls.bronze_container_name
}

output "silver_container_name" {
  description = "Nombre del contenedor Silver."
  value       = module.adls.silver_container_name
}

output "gold_container_name" {
  description = "Nombre del contenedor Gold."
  value       = module.adls.gold_container_name
}

output "databricks_workspace_url" {
  description = "URL del workspace de Databricks."
  value       = module.databricks.workspace_url
}

output "databricks_workspace_id" {
  description = "ID del workspace de Databricks."
  value       = module.databricks.workspace_id
}

output "key_vault_name" {
  description = "Nombre del Key Vault."
  value       = module.keyvault.key_vault_name
}

output "key_vault_uri" {
  description = "URI del Key Vault para referencia de secretos."
  value       = module.keyvault.key_vault_uri
}

output "log_analytics_workspace_id" {
  description = "ID del Log Analytics Workspace."
  value       = module.monitoring.log_analytics_workspace_id
}

output "sql_server_fqdn" {
  description = "FQDN del servidor Azure SQL (fuente origen)."
  value       = azurerm_mssql_server.source.fully_qualified_domain_name
}

output "sql_database_name" {
  description = "Nombre de la base de datos SQL origen."
  value       = azurerm_mssql_database.source.name
}
