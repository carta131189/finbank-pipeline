###############################################################
# FinBank S.A. - Infraestructura como Código (Terraform)
# Plataforma: Microsoft Azure
# Backend: Azure Storage Account (remoto)
###############################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Backend remoto en Azure Storage — NUNCA confirmar terraform.tfstate en Git
  backend "azurerm" {
    resource_group_name  = "rg-finbank-tfstate"
    storage_account_name = "stfinbanktfstate"
    container_name       = "tfstate"
    key                  = "finbank.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

###############################################################
# Data source: cliente de Azure activo
###############################################################
data "azurerm_client_config" "current" {}

###############################################################
# Resource Group principal
###############################################################
resource "azurerm_resource_group" "main" {
  name     = "rg-finbank-${var.environment}"
  location = var.location

  tags = local.common_tags
}

###############################################################
# Módulo: ADLS Gen2 + contenedores Medallón
###############################################################
module "adls" {
  source = "./modules/adls"

  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  project             = var.project
  tags                = local.common_tags
}

###############################################################
# Módulo: Azure Databricks Workspace
###############################################################
module "databricks" {
  source = "./modules/databricks"

  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  project             = var.project
  sku                 = var.databricks_sku
  tags                = local.common_tags
}

###############################################################
# Módulo: Azure Key Vault
###############################################################
module "keyvault" {
  source = "./modules/keyvault"

  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  environment         = var.environment
  project             = var.project
  tenant_id           = data.azurerm_client_config.current.tenant_id
  object_id           = data.azurerm_client_config.current.object_id
  tags                = local.common_tags
}

###############################################################
# Módulo: Monitoreo — Log Analytics + Action Group + Alertas
###############################################################
module "monitoring" {
  source = "./modules/monitoring"

  resource_group_name    = azurerm_resource_group.main.name
  location               = var.location
  environment            = var.environment
  project                = var.project
  alert_email            = var.alert_email
  storage_account_id     = module.adls.storage_account_id
  databricks_workspace_id = module.databricks.workspace_id
  tags                   = local.common_tags
}

###############################################################
# Azure SQL Database — fuente origen del pipeline
###############################################################
resource "azurerm_mssql_server" "source" {
  name                         = "sql-finbank-src-${var.environment}-${random_string.suffix.result}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_user
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"

  azuread_administrator {
    login_username = "AzureAD Admin"
    object_id      = data.azurerm_client_config.current.object_id
  }

  tags = local.common_tags
}

resource "azurerm_mssql_database" "source" {
  name           = "finbank-source-${var.environment}"
  server_id      = azurerm_mssql_server.source.id
  collation      = "SQL_Latin1_General_CP1_CI_AS"
  sku_name       = var.sql_sku
  max_size_gb    = var.sql_max_size_gb
  zone_redundant = false

  tags = local.common_tags
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.source.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

###############################################################
# Suffix aleatorio para nombres únicos globales
###############################################################
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

###############################################################
# Tags comunes
###############################################################
locals {
  common_tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    owner       = "data-engineering"
    cost_center = "finbank-data"
  }
}
