"""Phase 1: inspect weekly scope only; consolidation is not implemented."""

import os

import pendulum
from airflow.sdk import DAG, Param, get_current_context, task
from health_pipeline.control import weekly_scope

STUDY_TIMEZONE = os.getenv("STUDY_TIMEZONE", "America/Bogota")

# Scheduling stays disabled until daily readiness and publication freezing exist.
with DAG(
    "wearable_weekly_biostatistics",
    description="Phase 1 calendar inspection only; no biostatistics publication",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=STUDY_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["wearable", "phase-1", "scope-only"],
    params={"week_start": Param(type="string", format="date")},
    doc_md=__doc__,
) as dag:
    @task
    def resolve_scope():
        context = get_current_context()
        return weekly_scope(week_start=context["params"]["week_start"], study_timezone=STUDY_TIMEZONE)

    resolve_scope()
