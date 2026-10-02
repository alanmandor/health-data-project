# Wearable Health Data Pipeline

An incremental data engineering project for wearable activity, heart rate, sleep, and wellness data.

## Project direction

Apache Airflow will orchestrate delta ingestion and dbt transformations. The selected local platform uses DuckDB for analytics and a separate PostgreSQL database for Airflow metadata. A Data Vault 2.0-aligned integration layer will preserve source history, with Kimball dimensional marts above it. Snowflake is the documented cloud target; local environment copies do not implement Zero-Copy Clone.

The repository includes a local Docker Compose environment, pinned top-level dependencies, and [first-phase manual DAGs](docs/airflow_phase_one.md) for delivery registration and weekly calendar inspection. Raw ingestion, Data Vault models, analytical readiness, and biostatistics publication are not implemented yet.

## Local Docker Setup

Follow the [Docker quick start](docs/docker_quickstart.md) to build the image, initialize Airflow, and start the local services. Dependencies are declared in [requirements.txt](requirements.txt) and [requirements-dbt.txt](requirements-dbt.txt); Airflow and dbt use separate Python environments inside the container.

## Structure

- `src/`: ingestion and profiling code.
- `dags/`: Airflow workflows.
- `dbt/`: transformation models, macros, and tests.
- `tests/`: Python and integration tests.
- `infra/`: container and deployment configuration.
- `docs/`: architecture and decision records.
- `scripts/`: development utilities.

Raw data, generated outputs, and credentials are excluded from Git.
See [the architecture outline](docs/architecture.md), [processing strategy](docs/processing_strategy.md), and [data quality and correction policy](docs/data_quality_policy.md). Accepted design decisions are indexed in [the documentation](docs/README.md). Analytical models and pipelines remain to be implemented.

## AI assistance

Codex assists with design, code, and documentation. Decisions and results will be reviewed and explained as the project progresses.
