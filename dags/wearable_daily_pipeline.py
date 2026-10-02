"""Phase 1: validate and register deliveries; analytical readiness is not emitted."""

import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from health_pipeline.airflow_controls import DeliverySensor

from health_pipeline.control import daily_scope, delivery_available, register_delivery

STUDY_TIMEZONE = os.getenv("STUDY_TIMEZONE", "America/Bogota")
INCOMING = os.getenv("PROJECT_INCOMING_DIR", "/opt/airflow/project-data/incoming")
REGISTRY = "/opt/airflow/project-data/control/deliveries.sqlite"

with DAG(
    "wearable_daily_pipeline",
    description="Phase 1 delivery intake only; no analytics output yet",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=STUDY_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["wearable", "phase-1", "intake-only"],
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
        python_callable=delivery_available,
        op_kwargs={"root": INCOMING, "manifest_key": "{{ params.manifest_key }}",
                   "target_date": "{{ params.target_date }}", "study_timezone": STUDY_TIMEZONE},
        mode="reschedule",
        poke_interval=300,
        timeout=1,
        retries=0,
    )

    @task(pool="duckdb_access")
    def register_manifest(scope):
        context = get_current_context()
        return register_delivery(INCOMING, context["params"]["manifest_key"], scope, REGISTRY)

    registration = register_manifest(scope)
    scope >> wait >> registration
