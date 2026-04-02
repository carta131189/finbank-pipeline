# FinBank S.A. — Variables para entorno DEV
# Este archivo SÍ se versiona en Git (no contiene secretos).
# Las variables sensibles (sql_admin_user, sql_admin_password)
# se inyectan desde Azure DevOps Pipeline o GitHub Actions.

resource_group_name = "rg-finbank-tfstate"
location            = "eastus"
environment         = "dev"
project             = "finbank"
tags = {
  Owner = "Team Data"
  Env   = "dev"
}
log_analytics_workspace_id = ""
