#!/usr/bin/env bash
# ============================================================
# bootstrap_backend.sh
# Crea el Storage Account para el estado remoto de Terraform.
# Ejecutar UNA SOLA VEZ antes de terraform init.
#
# Requisitos: az CLI autenticado (az login)
# Uso: bash bootstrap_backend.sh [dev|prod]
# ============================================================

set -euo pipefail

ENVIRONMENT="${1:-dev}"
LOCATION="eastus2"
RG_NAME="rg-finbank-tfstate"
SA_NAME="stfinbanktfstate"
CONTAINER_NAME="tfstate"

echo "==> Creando Resource Group para estado Terraform: $RG_NAME"
az group create \
  --name "$RG_NAME" \
  --location "$LOCATION" \
  --tags project=finbank managed_by=bootstrap

echo "==> Creando Storage Account: $SA_NAME"
az storage account create \
  --name "$SA_NAME" \
  --resource-group "$RG_NAME" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --https-only true \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false

echo "==> Creando contenedor: $CONTAINER_NAME"
az storage container create \
  --name "$CONTAINER_NAME" \
  --account-name "$SA_NAME" \
  --auth-mode login

echo ""
echo "✅ Backend remoto listo."
echo "   Ejecuta ahora:"
echo "   cd infra && terraform init -backend-config=environments/${ENVIRONMENT}/backend.conf"
