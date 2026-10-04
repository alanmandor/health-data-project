# Docker Quick Start

This setup starts Airflow and initializes warehouse/control tables. The [daily pipeline](daily_analytics.md) captures explicit deliveries and publishes validated analytics; [weekly biostatistics](weekly_biostatistics.md) freezes and publishes revisioned outputs after readiness checks.

## Prerequisites

Install Docker Desktop with the Linux engine running (Windows users should use WSL 2), or Docker Engine on Linux, and Docker Compose v2.14 or newer. Allocate at least 8 GB of Docker memory for the supplied baseline and allow persistent disk space for raw history, the warehouse, temporary spill, and frozen manifests. No host Python, PostgreSQL, Airflow, or dbt installation is required.

Run all commands from the repository root. This Compose project is named `health-data-project`; it does not reuse or modify unrelated containers. PostgreSQL has no published host port. The Airflow UI binds to localhost only.

## Configure and Start

Copy the example environment file once. In PowerShell:

```powershell
Copy-Item .env.example .env
```

On Linux or macOS:

```bash
cp .env.example .env
```

Set AIRFLOW_ADMIN_PASSWORD in .env before initialization. If localhost port 8080 is occupied, change AIRFLOW_PORT. Do not commit .env. Local default database credentials and signing settings are intended for this development environment only.

```bash
docker compose config --quiet
docker compose build
docker compose up airflow-init --exit-code-from airflow-init
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
```

Wait for the API server, scheduler, DAG processor, and PostgreSQL health checks to pass. Open [Airflow](http://localhost:8080), or the port selected in .env. Log in with AIRFLOW_ADMIN_USERNAME and AIRFLOW_ADMIN_PASSWORD. Initialization creates the single-slot duckdb_access pool and preserves an existing user's password when repeated. Changing .env after initialization does not reset that password.

## Dependencies and Storage

Initialization applies DuckDB foundation and SQLite controls without dropping records. The daily DAG also checks bootstrap before capture. Stop Airflow services before rebuilding and rerunning initialization on an existing deployment to avoid overlapping database access. Initialization creates tables, not analytical data. DDL is mounted read-only at /opt/airflow/project/ddl.

requirements.txt pins Airflow and its providers. The Docker build uses the official Airflow 3.1.7 Python 3.12 constraints to resolve compatible runtime dependencies. requirements-dbt.txt pins dbt Core, dbt-duckdb, and DuckDB in a separate virtual environment to avoid dependency conflicts with Airflow. Image versions and top-level packages are pinned; transitive dbt dependencies are resolved at build time, so builds are not yet a fully hashed offline lock.

The pipeline runs dbt through /opt/airflow/dbt-venv/bin/dbt, or DBT_EXECUTABLE. Source mounts are read-only; quality, dimensional, daily, and weekly runners redirect profiles, targets, and logs to separate writable project-data directories. All four dbt projects are included in the repository.

Docker named volumes store metadata, logs, raw data, analytical files, and frozen input manifests outside the repository. Source data is not baked into the image. PROJECT_RAW_DIR and PROJECT_DATABASE_PATH define the ingestion locations. Keep all shared database access under duckdb_access; run standalone warehouse commands only while other warehouse tasks are idle.

## Inspect and Stop

```bash
docker compose logs --tail 100 airflow-init
docker compose logs --tail 100 airflow-api-server airflow-scheduler airflow-dag-processor
docker compose exec airflow-scheduler airflow pools list
docker compose exec airflow-scheduler /opt/airflow/dbt-venv/bin/dbt --version
docker compose stop
```

To remove only this project's containers and network while retaining data:

```bash
docker compose down
```

Do not add --volumes unless you intend to permanently delete this project's metadata, logs, and analytical data. Do not use system-wide prune commands as part of setup.

## Scheduling Status

Timestamp reference: UTC. Study calendar: America/Bogota. The tentative weekly trigger is Monday 03:00 local time (08:00 UTC), conditional on validated delivery. Both complete DAGs use schedule=None and require explicit manual scope. Configure the daily trigger time, operational sensor timeout, and expected producer inventory before automatic scheduling. Follow [baseline ingestion](raw_ingestion.md) and [weekly publication](weekly_biostatistics.md) to execute the exercise.

Reference: [Airflow Docker guide](https://airflow.apache.org/docs/apache-airflow/3.1.7/howto/docker-compose/index.html).

## Verified Local Startup

On October 2, 2026, the configuration was built and started on Docker Desktop's Linux engine with Compose 2.38.2. Both Python environments passed pip check. Initialization completed successfully and a subsequent startup reran initialization without recreating the administrator. PostgreSQL, the API server, scheduler, and DAG processor passed health checks; administrator authentication succeeded and duckdb_access had one slot. dbt Core 1.11.15 and dbt-duckdb 1.11.0 loaded successfully. DAG import checks returned no errors with the currently empty DAG directory. This verifies infrastructure startup, not data pipeline behavior.
