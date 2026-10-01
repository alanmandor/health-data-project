# Wearable Health Data Pipeline

An incremental data engineering project for wearable activity, heart rate, sleep, and wellness data.

## Project direction

Apache Airflow will orchestrate delta ingestion and dbt transformations. The selected local platform uses DuckDB for analytics and a separate PostgreSQL database for Airflow metadata. A Data Vault 2.0-aligned integration layer will preserve source history, with Kimball dimensional marts above it. Snowflake is the documented cloud target; local environment copies do not implement Zero-Copy Clone.

This repository currently contains the project skeleton only. The pipeline and deployment are not implemented.

## Structure

- `src/`: ingestion and profiling code.
- `dags/`: Airflow workflows.
- `dbt/`: transformation models, macros, and tests.
- `tests/`: Python and integration tests.
- `infra/`: container and deployment configuration.
- `docs/`: architecture and decision records.
- `scripts/`: development utilities.

Raw data, generated outputs, and credentials are excluded from Git.
See [the architecture outline](docs/architecture.md), [processing strategy](docs/processing_strategy.md), and [data quality and correction policy](docs/data_quality_policy.md). Accepted design decisions are indexed in [the documentation](docs/README.md). Runtime configuration and pipelines remain to be implemented.

## AI assistance

Codex assists with design, code, and documentation. Decisions and results will be reviewed and explained as the project progresses.
