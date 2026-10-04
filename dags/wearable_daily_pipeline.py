"""Capture deltas, preserve Vault history, and publish validated daily analytics."""

import os
import json
import logging
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from health_pipeline.airflow_controls import DeliverySensor

from health_pipeline.control import daily_scope
from health_pipeline.raw_ingestion import available, capture, archive

STUDY_TIMEZONE = os.getenv("STUDY_TIMEZONE", "America/Bogota")
INCOMING = os.getenv("PROJECT_INCOMING_DIR", "/opt/airflow/project-data/incoming")
REGISTRY = os.getenv("PROJECT_REGISTRY_PATH", "/opt/airflow/project-data/control/deliveries.sqlite")
RAW = os.getenv("PROJECT_RAW_DIR", "/opt/airflow/project-data/raw")
ARCHIVE = os.getenv("PROJECT_ARCHIVE_DIR", "/opt/airflow/project-data/archive")


def warehouse_command(action, registration=None):
    command = ["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.raw_ingestion", action]
    if registration is not None:
        command.extend(["--registration", json.dumps(registration)])
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
        for line in process.stderr:
            message = line.rstrip()
            if message.startswith("INFO "):
                logging.getLogger(__name__).info(message[5:])
            else:
                logging.getLogger(__name__).error(message)
        output = process.stdout.read()
        if process.wait() != 0:
            raise subprocess.CalledProcessError(process.returncode, command)
    return json.loads(output)

with DAG(
    "wearable_daily_pipeline",
    description="Raw intake, quality, Data Vault, dimensional marts, and validated daily analytics",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=STUDY_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["wearable", "raw-ingestion", "manual"],
    params={
        "target_date": Param(type="string", format="date"),
        "manifest_key": Param(type="string", minLength=1),
        "sensor_timeout_seconds": Param(type="integer", minimum=1),
    },
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5), "retry_exponential_backoff": True},
    render_template_as_native_obj=True,
    doc_md=__doc__,
) as dag:
    @task
    def resolve_scope():
        context = get_current_context()
        return daily_scope(target_date=context["params"]["target_date"], study_timezone=STUDY_TIMEZONE)

    scope = resolve_scope()
    wait = DeliverySensor(
        task_id="wait_for_delivery",
        python_callable=available,
        op_kwargs={"root": INCOMING, "manifest_key": "{{ params.manifest_key }}",
                   "target_date": "{{ params.target_date }}", "study_timezone": STUDY_TIMEZONE, "raw": RAW},
        mode="reschedule",
        poke_interval=300,
        timeout=1,
        retries=0,
    )

    @task(pool="duckdb_access")
    def initialize_warehouse():
        return warehouse_command("initialize")

    @task(pool="duckdb_access")
    def capture_delivery(scope):
        context = get_current_context()
        return capture(INCOMING, context["params"]["manifest_key"], scope, RAW, REGISTRY, run_key=context["run_id"])

    @task(pool="duckdb_access")
    def load_raw(registration):
        return warehouse_command("load", registration)

    @task(pool="duckdb_access")
    def archive_delivery(registration):
        return archive(INCOMING, ARCHIVE, REGISTRY, registration)

    @task(pool="duckdb_access")
    def build_staging_quality(registration):
        command = ["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.dbt_quality",
                   "--registration", json.dumps(registration)]
        subprocess.run(command, check=True)
        return {"batch_id": registration["batch_id"], "status": "staging_quality_validated",
                "consumer_published": False}

    @task(pool="duckdb_access")
    def load_data_vault(validation):
        subprocess.run(["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.vault_loading"], check=True)
        return {"batch_id": validation["batch_id"], "status": "vault_loaded", "consumer_published": False}

    @task(pool="duckdb_access")
    def reconcile_quality_issues(vault):
        subprocess.run(["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.quality_issues"], check=True)
        return {"batch_id": vault["batch_id"], "status": "quality_issues_reconciled", "consumer_published": False}

    @task(pool="duckdb_access")
    def queue_affected_dates(quality):
        subprocess.run(["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.affected_dates"], check=True)
        return {"batch_id": quality["batch_id"], "status": "affected_dates_queued", "consumer_published": False}

    @task(pool="duckdb_access")
    def load_dimensional_marts(impact):
        subprocess.run(["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.mart_loading"], check=True)
        return {"batch_id": impact["batch_id"], "status": "marts_loaded", "consumer_published": False}

    @task(pool="duckdb_access")
    def publish_daily_analytics(marts):
        subprocess.run(["/opt/airflow/dbt-venv/bin/python", "-m", "health_pipeline.daily_loading"], check=True)
        return {"batch_id": marts["batch_id"], "status": "daily_analytics_validated", "consumer_published": True}

    initialized = initialize_warehouse()
    registration = capture_delivery(scope)
    scope >> wait >> registration
    initialized >> registration
    publish_daily_analytics(load_dimensional_marts(queue_affected_dates(reconcile_quality_issues(load_data_vault(build_staging_quality(archive_delivery(load_raw(registration))))))))
