#!/usr/bin/env bash
# Run evaluator-facing checks without source deliveries or a persistent warehouse.
set -euo pipefail

cd /opt/airflow/project
DBT_PYTHON=/opt/airflow/dbt-venv/bin/python

echo "Checking generated DuckDB and Snowflake DDL"
"$DBT_PYTHON" scripts/generate_ddl.py --check

echo "Checking foundation constraints, migrations, and catalog equivalences"
"$DBT_PYTHON" scripts/verify_ddl.py

echo "Checking calendar and delivery contracts"
"$DBT_PYTHON" -m unittest discover -s tests -p test_controls.py -v

echo "Checking raw ingestion recovery and replay with synthetic inputs"
"$DBT_PYTHON" -m unittest discover -s tests -p test_raw_ingestion.py -v

echo "Checking complete Airflow DAG imports"
python scripts/verify_airflow_phase_one.py --dag-folder /opt/airflow/project/dags
