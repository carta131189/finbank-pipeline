###############################################################
# Módulo: Azure Key Vault
# Almacena todos los secretos del pipeline:
# credenciales SQL, tokens de Databricks, claves de Storage.
# NINGUNA credencial aparece en el código fuente.
###############################################################

resource "azurerm_key_vault" "main" {
  name                       = "kv-finbank-${var.environment}-${random_string.kv_suffix_v2.result}"
  resource_group_name        = var.resource_group_name
  location                   = var.location
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 30
  purge_protection_enabled   = true

  # Auditoría: habilitar logs de acceso
  enable_rbac_authorization = true

  network_acls {
    default_action = "Allow"
    bypass         = "AzureServices"
  }

  tags = var.tags
}

resource "random_string" "kv_suffix_v2" {
  length  = 6
  special = false
  upper   = false
}

###############################################################
# Política de acceso para el identity que ejecuta Terraform
###############################################################
resource "azurerm_role_assignment" "kv_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = var.object_id
}

###############################################################
# Secretos placeholder — los valores reales se cargan
# desde el pipeline CI/CD mediante TF_VAR_* o az keyvault secret set
###############################################################
resource "azurerm_key_vault_secret" "sql_admin_user" {
  name         = "sql-admin-user"
  value        = "REPLACE_VIA_PIPELINE"
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]

  lifecycle {
    ignore_changes = [value]
  }
}

resource "azurerm_key_vault_secret" "sql_admin_password" {
  name         = "sql-admin-password"
  value        = "REPLACE_VIA_PIPELINE"
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]

  lifecycle {
    ignore_changes = [value]
  }
}

resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "storage-account-key"
  value        = "REPLACE_VIA_PIPELINE"
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]

  lifecycle {
    ignore_changes = [value]
  }
}
