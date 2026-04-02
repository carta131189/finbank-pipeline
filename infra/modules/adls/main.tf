###############################################################
# Módulo: ADLS Gen2
# Crea el Storage Account con jerarquía de namespaces habilitada
# y los tres contenedores de la arquitectura Medallón.
###############################################################

resource "azurerm_storage_account" "adls" {
  name                     = "stfinbank${var.environment}${random_string.adls_suffix.result}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  # Habilita ADLS Gen2 (Hierarchical Namespace)
  is_hns_enabled = true

  # Seguridad: solo HTTPS, TLS 1.2 mínimo, sin acceso anónimo
  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false

  blob_properties {
   versioning_enabled  = false
    change_feed_enabled = true

    delete_retention_policy {
      days = 30
    }

    container_delete_retention_policy {
      days = 30
    }
  }

  tags = var.tags
}

resource "random_string" "adls_suffix" {
  length  = 6
  special = false
  upper   = false
}

###############################################################
# Contenedores Medallón: Bronze, Silver, Gold
###############################################################

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.adls.id

  properties = {
    layer = "bronze"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.adls.id

  properties = {
    layer = "silver"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.adls.id

  properties = {
    layer = "gold"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "errors" {
  name               = "errors"
  storage_account_id = azurerm_storage_account.adls.id

  properties = {
    layer = "errors"
  }
}

###############################################################
# Diagnostic settings → Log Analytics
###############################################################
resource "azurerm_monitor_diagnostic_setting" "adls" {
  count = var.log_analytics_workspace_id != "" ? 1 : 0

  name                       = "diag-adls-${var.environment}"
  target_resource_id         = "${azurerm_storage_account.adls.id}/blobServices/default"
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category = "StorageRead"
  }
  enabled_log {
    category = "StorageWrite"
  }
  enabled_log {
    category = "StorageDelete"
  }

  metric {
    category = "Transaction"
    enabled  = true
  }
}
