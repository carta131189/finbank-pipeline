###############################################################
# Módulo: Monitoreo Operacional
# - Log Analytics Workspace
# - Action Group (notificaciones por email)
# - Alertas: fallo de pipeline, anomalía de volumen
###############################################################

###############################################################
# Log Analytics Workspace
###############################################################
resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-finbank-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = 90

  tags = var.tags
}

###############################################################
# Action Group — destinatarios de alertas
###############################################################
resource "azurerm_monitor_action_group" "pipeline_alerts" {
  name                = "ag-finbank-pipeline-${var.environment}"
  resource_group_name = var.resource_group_name
  short_name          = "fbpipeline"

  email_receiver {
    name                    = "DataEngineeringTeam"
    email_address           = var.alert_email
    use_common_alert_schema = true
  }

  tags = var.tags
}

###############################################################
# Alerta: disponibilidad del Storage Account (ADLS)
###############################################################
resource "azurerm_monitor_metric_alert" "storage_availability" {
  name                = "alert-adls-availability-${var.environment}"
  resource_group_name = var.resource_group_name
  scopes              = [var.storage_account_id]
  description         = "Alerta cuando la disponibilidad del ADLS Gen2 cae por debajo del 99%."
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "Availability"
    aggregation      = "Average"
    operator         = "LessThan"
    threshold        = 99
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }

  tags = var.tags
}

###############################################################
# Alerta: errores en transacciones del Storage
###############################################################
resource "azurerm_monitor_metric_alert" "storage_errors" {
  name                = "alert-adls-errors-${var.environment}"
  resource_group_name = var.resource_group_name
  scopes              = [var.storage_account_id]
  description         = "Alerta ante errores en operaciones del Storage Account del pipeline."
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "Transactions"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0

    dimension {
      name     = "ResponseType"
      operator = "Include"
      values   = ["ServerError", "ClientError"]
    }
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }

  tags = var.tags
}

###############################################################
# Diagnostic settings del Databricks Workspace → Log Analytics
###############################################################
#resource "azurerm_monitor_diagnostic_setting" "databricks" {
#  name                       = "diag-databricks-${var.environment}"
#  target_resource_id         = var.databricks_workspace_id
#  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id

  #enabled_log { category = "dbfs" }
  #enabled_log { category = "clusters" }
  #enabled_log { category = "accounts" }
  #nabled_log { category = "jobs" }
  #enabled_log { category = "ssh" }
  #enabled_log { category = "workspace" }
#}
