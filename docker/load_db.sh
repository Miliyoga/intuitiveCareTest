#!/usr/bin/env bash
set -euo pipefail

# Variáveis esperadas (docker-compose define):
# DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
# PGPASSWORD é usado pelo psql automaticamente.

source /app/docker/wait_for_db.sh

echo "[loader] Running full pipeline to generate CSV artifacts..."
python -c "from app.jobs.full_pipeline_job import run_full_pipeline; run_full_pipeline()"
echo "[loader] Pipeline done."

export PGPASSWORD="${DB_PASSWORD}"

echo "[loader] Applying schema..."
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -f /app/sql/postgres/01_schema.sql
echo "[loader] Schema applied."

echo "[loader] Importing CSV data..."
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -f /app/sql/postgres/02_import.psql
echo "[loader] Import done."

echo "[loader] Quick sanity checks:"
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT COUNT(*) AS operadoras_cadop FROM ans.operadoras_cadop;"
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT COUNT(*) AS despesas_trimestre FROM ans.consolidated_validated;"
psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT COUNT(*) AS despesas_agregadas FROM ans.despesas_agregadas;"


echo "[loader] Done."
