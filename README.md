# Wearable Health Data Pipeline

An incremental data engineering project for wearable activity, heart rate, sleep, and wellness data.

## Project direction

Apache Airflow will orchestrate ingestion and dbt transformations. We will evaluate a Data Vault 2.0 integration layer with Kimball dimensional marts above it before implementing the models.

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
See [the architecture outline](docs/architecture.md) for the proposed approach.

## AI assistance

Codex assists with design, code, and documentation. Decisions and results will be reviewed and explained as the project progresses.
