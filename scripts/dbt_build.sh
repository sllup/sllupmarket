#!/usr/bin/env bash
set -euo pipefail
pip install --no-cache-dir dbt-postgres
cd dbt_project
dbt deps || true
dbt build --fail-fast
