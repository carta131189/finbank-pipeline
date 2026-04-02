###############################################################
# Módulo: Azure Databricks Workspace
###############################################################

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-finbank-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.sku

  custom_parameters {
    no_public_ip = true
  }

  tags = var.tags
}
