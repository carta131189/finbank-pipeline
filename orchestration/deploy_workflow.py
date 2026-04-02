#!/usr/bin/env python3
"""
FinBank S.A. — Despliegue del Workflow en Databricks
=====================================================
Crea o actualiza el job de Databricks Workflows desde finbank_workflow.json.
Usa la API REST de Databricks 2.1.

Variables de entorno requeridas:
    DATABRICKS_HOST  — ej: https://adb-1234567890.azuredatabricks.net
    DATABRICKS_TOKEN — token de acceso personal o service principal token

Uso:
    python deploy_workflow.py
    python deploy_workflow.py --workflow finbank_workflow.json
"""

import os
import sys
import json
import argparse
import logging
import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("deploy")


def get_env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        log.error(f"Variable de entorno requerida: {key}")
        sys.exit(1)
    return val


def get_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


def find_existing_job(host: str, token: str, job_name: str) -> int | None:
    """Busca si ya existe un job con el mismo nombre. Retorna el job_id o None."""
    url  = f"{host}/api/2.1/jobs/list"
    resp = requests.get(url, headers=get_headers(token), params={"limit": 100})
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])
    for job in jobs:
        if job.get("settings", {}).get("name") == job_name:
            return job["job_id"]
    return None


def create_job(host: str, token: str, job_def: dict) -> int:
    url  = f"{host}/api/2.1/jobs/create"
    resp = requests.post(url, headers=get_headers(token), json=job_def)
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    log.info(f"Job creado con ID: {job_id}")
    return job_id


def update_job(host: str, token: str, job_id: int, job_def: dict):
    url  = f"{host}/api/2.1/jobs/reset"
    body = {"job_id": job_id, "new_settings": job_def}
    resp = requests.post(url, headers=get_headers(token), json=body)
    resp.raise_for_status()
    log.info(f"Job actualizado. ID: {job_id}")


def main():
    parser = argparse.ArgumentParser(description="Despliega el workflow de FinBank en Databricks")
    parser.add_argument("--workflow", default="finbank_workflow.json")
    parser.add_argument("--run-now", action="store_true",
                        help="Ejecutar el pipeline inmediatamente tras desplegar")
    args = parser.parse_args()

    host  = get_env("DATABRICKS_HOST").rstrip("/")
    token = get_env("DATABRICKS_TOKEN")

    with open(args.workflow) as f:
        job_def = json.load(f)

    job_name   = job_def["name"]
    existing_id = find_existing_job(host, token, job_name)

    if existing_id:
        log.info(f"Job '{job_name}' ya existe (ID: {existing_id}). Actualizando...")
        update_job(host, token, existing_id, job_def)
        job_id = existing_id
    else:
        log.info(f"Creando nuevo job: '{job_name}'...")
        job_id = create_job(host, token, job_def)

    log.info(f"\nWorkflow listo: {host}/#job/{job_id}")

    if args.run_now:
        log.info("Disparando ejecución inmediata...")
        url  = f"{host}/api/2.1/jobs/run-now"
        resp = requests.post(
            url,
            headers=get_headers(token),
            json={"job_id": job_id}
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]
        log.info(f"Run iniciado. Run ID: {run_id}")
        log.info(f"Monitoreo: {host}/#job/{job_id}/run/{run_id}")


if __name__ == "__main__":
    main()
