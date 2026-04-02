output "storage_account_name"  { value = azurerm_storage_account.adls.name }
output "storage_account_id"    { value = azurerm_storage_account.adls.id }
output "dfs_endpoint"          { value = azurerm_storage_account.adls.primary_dfs_endpoint }
output "bronze_container_name" { value = azurerm_storage_data_lake_gen2_filesystem.bronze.name }
output "silver_container_name" { value = azurerm_storage_data_lake_gen2_filesystem.silver.name }
output "gold_container_name"   { value = azurerm_storage_data_lake_gen2_filesystem.gold.name }
output "errors_container_name" { value = azurerm_storage_data_lake_gen2_filesystem.errors.name }
