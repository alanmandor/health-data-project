#!/usr/bin/env bash
set -euo pipefail

airflow db migrate
airflow pools set duckdb_access 1 'Serialize access to the shared analytical database'

# Compose initialization runs before schedulers start and cannot overlap DAG writes.
/opt/airflow/dbt-venv/bin/python -m health_pipeline.raw_ingestion initialize

# Preserve an existing user's password when initialization is repeated.
airflow users list --output json > /tmp/project-users.json
if ! python -c 'import json, os, sys; users = json.load(open("/tmp/project-users.json")); sys.exit(0 if any(u["username"] == os.environ["AIRFLOW_ADMIN_USERNAME"] for u in users) else 1)'; then
    airflow users create \
        --username "$AIRFLOW_ADMIN_USERNAME" \
        --password "$AIRFLOW_ADMIN_PASSWORD" \
        --firstname Project \
        --lastname Administrator \
        --role Admin \
        --email admin@example.test
fi
