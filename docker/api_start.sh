#!/usr/bin/env bash
set -euo pipefail

export RUN_PIPELINE_ON_STARTUP="${RUN_PIPELINE_ON_STARTUP:-0}"

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
