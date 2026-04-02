###############################################################
# FinBank S.A. — Variables de Terraform
# Todos los valores sensibles vienen de archivos .tfvars
# o desde pipelines CI/CD — NUNCA hardcodeados aquí.
###############################################################

variable "project" {
  description = "Nombre del proyecto. Se usa como prefijo en todos los recursos."
  type        = string
  default     = "finbank"
}

variable "environment" {
  description = "Entorno de despliegue: dev o prod."
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "El entorno debe ser 'dev' o 'prod'."
  }
}

variable "location" {
  description = "Región de Azure donde se despliegan los recursos."
  type        = string
  default     = "eastus2"
}

variable "databricks_sku" {
  description = "SKU del workspace de Databricks: standard, premium o trial."
  type        = string
  default     = "premium"
  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "El SKU de Databricks debe ser standard, premium o trial."
  }
}

variable "sql_admin_user" {
  description = "Usuario administrador de Azure SQL Server. Referenciado desde Key Vault en producción."
  type        = string
  sensitive   = true
}

variable "sql_admin_password" {
  description = "Contraseña del administrador de Azure SQL Server. NUNCA incluir en código fuente."
  type        = string
  sensitive   = true
}

variable "sql_sku" {
  description = "SKU de Azure SQL Database."
  type        = string
  default     = "S2"
}

variable "sql_max_size_gb" {
  description = "Tamaño máximo de Azure SQL Database en GB."
  type        = number
  default     = 50
}

variable "alert_email" {
  description = "Correo electrónico para alertas operacionales del pipeline."
  type        = string
}

variable "log_analytics_workspace_id" {
  description = "ID del Log Analytics Workspace para diagnostic settings"
  type        = string
  default     = ""  # para dev puede estar vacío
}