# Wearable Health Data Pipeline

An incremental data engineering project for wearable activity, heart rate, sleep, and wellness data.

## Project direction

Apache Airflow orchestrates delta ingestion and dbt transformations. The local platform uses DuckDB for analytics and a separate PostgreSQL database for Airflow metadata. A Data Vault 2.0-aligned integration layer preserves source history, with Kimball dimensional marts above it. Snowflake is the documented cloud target; local environment copies do not implement Zero-Copy Clone.

The daily DAG runs [immutable raw ingestion](docs/raw_ingestion.md), [dbt staging and quality](docs/dbt_staging_quality.md), [Data Vault loading](docs/data_vault_loading.md), [quality issue reconciliation](docs/quality_issue_lifecycle.md), [affected-date propagation](docs/affected_dates.md), [dimensional marts](docs/dimensional_marts.md), and [validated daily analytics](docs/daily_analytics.md). The [weekly DAG](docs/weekly_biostatistics.md) waits for declared deliveries and daily readiness, freezes accepted inputs and the correction cutoff, and publishes immutable biostatistics revisions.

## Local Docker Setup

Follow the [Docker quick start](docs/docker_quickstart.md) to build the image, initialize Airflow, and start the local services. Dependencies are declared in [requirements.txt](requirements.txt) and [requirements-dbt.txt](requirements-dbt.txt); Airflow and dbt use separate Python environments inside the container.

## Structure

The [database foundation](docs/database_ddl.md) includes DuckDB and Snowflake DDL, SQLite controls, and Snowflake team roles with read-only published data and writable workspaces for derived tables and views. Local pipelines populate validated analytics and revisioned biostatistics. Snowflake deployment requires validation on a real account.

- `src/`: ingestion and profiling code.
- `dags/`: Airflow workflows.
- `dbt/`: transformation models, macros, and tests.
- `dbt_marts/`, `dbt_daily/`, `dbt_weekly/`: dimensional, daily, and frozen weekly transformations.
- `tests/`: Python and integration tests.
- `infra/`: container and deployment configuration.
- `docs/`: architecture and decision records.
- `scripts/`: development utilities.

Raw data, generated outputs, and credentials are excluded from Git.
See [the architecture outline](docs/architecture.md), [processing strategy](docs/processing_strategy.md), and [data quality and correction policy](docs/data_quality_policy.md). Design decisions and implementation details are indexed in [the documentation](docs/README.md).

## Run the exercise

After Docker startup, follow [baseline staging](docs/raw_ingestion.md#stage-the-exercise-baseline) to copy the supplied data and create its explicit manifest. Trigger `wearable_daily_pipeline` with the printed manifest key, target date `2026-01-08`, and a finite sensor timeout. After it succeeds, trigger `wearable_weekly_biostatistics` for each required Monday week with `expected_source_system=exercise_adapter` and `expected_delivery_id=baseline-2026-01-v1`. The first study week starts January 5 and the last starts February 2. [Weekly documentation](docs/weekly_biostatistics.md) includes a repeatable five-week helper and trigger parameters.

Both DAGs remain manual: the daily execution time, operational availability timeout, and producer inventory mapping must be configured before automatic scheduling. The tentative weekly production trigger is Monday 03:00 America/Bogota, conditional on validated delivery readiness.

The [scope and limitations guide](docs/scope_and_limitations.md) explains local concurrency, incremental processing boundaries, Snowflake portability, data interpretation, and the remaining production deployment requirements.

To run the isolated test suite and both DAGs twice:

```bash
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -v
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/verify_raw_airflow.py
```

Query `analytics.participant_day` for provisional daily results and `biostatistics.latest_participant_week` for the latest frozen weekly revision. [Chronotype comparisons](examples/queries/chronotype_comparison.sql) use frozen participant attributes and explicit coverage denominators. Missing measurements stay NULL; observed zero values remain zero.

## AI assistance

Codex assists with design, code, and documentation. Decisions and results will be reviewed and explained as the project progresses.
