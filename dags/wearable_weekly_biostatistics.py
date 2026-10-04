"""Wait for daily readiness, freeze the cutoff, and publish weekly biostatistics."""

import os
import json
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from health_pipeline.control import weekly_scope
from health_pipeline.airflow_controls import DeliverySensor
from health_pipeline.weekly_controls import readiness

STUDY_TIMEZONE = os.getenv("STUDY_TIMEZONE", "America/Bogota")

# Explicit delivery identities are required until a producer inventory is configured.
with DAG(
    "wearable_weekly_biostatistics",
    description="Validated weekly readiness, immutable input freeze, and revisioned biostatistics",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=STUDY_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["wearable", "biostatistics", "manual"],
    params={"week_start": Param(type="string", format="date"),
            "expected_source_system": Param(type="string",minLength=1),
            "expected_delivery_id": Param(type="string",minLength=1),
            "sensor_timeout_seconds": Param(type="integer",minimum=1)},
    default_args={"retries":3,"retry_delay":timedelta(minutes=5),"retry_exponential_backoff":True},
    render_template_as_native_obj=True,
    doc_md=__doc__,
) as dag:
    @task
    def resolve_scope():
        context = get_current_context()
        return weekly_scope(week_start=context["params"]["week_start"], study_timezone=STUDY_TIMEZONE)

    scope=resolve_scope()
    wait=DeliverySensor(task_id='wait_for_daily_readiness',python_callable=readiness,
        op_kwargs={'scope':scope,'expected_source_system':'{{ params.expected_source_system }}',
                   'expected_delivery_id':'{{ params.expected_delivery_id }}','request_id':'{{ run_id }}'},
        mode='reschedule',poke_interval=300,timeout=1,retries=0)

    @task(pool='duckdb_access')
    def freeze_week(scope):
        context=get_current_context()
        command=['/opt/airflow/dbt-venv/bin/python','-m','health_pipeline.weekly_publication','freeze',
            '--week-start',scope['week_start'],'--request-id',context['run_id'],
            '--expected-source-system',context['params']['expected_source_system'],
            '--expected-delivery-id',context['params']['expected_delivery_id']]
        return json.loads(subprocess.check_output(command,text=True))

    @task(pool='duckdb_access')
    def publish_week(frozen):
        command=['/opt/airflow/dbt-venv/bin/python','-m','health_pipeline.weekly_publication','publish',
                 '--publication-id',frozen['publication_id']]
        # dbt emits progress before the final small JSON response.
        result=subprocess.run(command,check=True,text=True,stdout=subprocess.PIPE)
        print(result.stdout)
        return json.loads(result.stdout.strip().splitlines()[-1])

    frozen=freeze_week(scope)
    scope >> wait >> frozen
    publish_week(frozen)
