# FinBank S.A. — Variables para entorno PROD
# Este archivo SÍ se versiona en Git (no contiene secretos).

project        = "finbank"
environment    = "prod"
location       = "eastus2"
databricks_sku = "premium"
sql_sku        = "S4"
sql_max_size_gb = 250
alert_email    = "data-engineering@finbank.com"

# log_analytics_workspace_id = "log_analytics_workspace_id = "/subscriptions/<subs_id>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<workspace_name>""
# sql_admin_user     → inyectar desde variable de entorno TF_VAR_sql_admin_user
# sql_admin_password → inyectar desde variable de entorno TF_VAR_sql_admin_password
