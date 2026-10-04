# ADR 001: Local Analytical Platform

Status: accepted and implemented locally with DuckDB, dbt, and Docker/Airflow. Snowflake remains the cloud deployment target.

## Decision

Use DuckDB with dbt-duckdb for the local analytical database. Use a separate PostgreSQL database for Airflow metadata. Snowflake remains the documented cloud target, not an emulated or validated runtime.

Keep immutable source versions outside the analytical database. Organize analytical models into staging, raw vault, business rules, dimensional marts, clinical analytics, and biostatistics schemas.

## Execution and Environment Isolation

File discovery, downloads, and validation can run in parallel. All independent processes opening the shared DuckDB database for writing must use one Airflow pool with one slot, release connections on completion, and avoid overlapping runs. This includes Python ingestion and dbt. Read access during writes must also respect DuckDB connection constraints; use an independent published copy for separate consumers when needed.

Development and test environments use separate database files. Prefer reconstruction from a fixed raw manifest. When testing against an existing incremental state, create a consistent physical copy after closing the database and stopping writers. Copies consume additional storage and are not Zero-Copy Clones.

Snowflake Zero-Copy Clone, virtual warehouse isolation, and native recovery capabilities require validation in Snowflake. dbt clone is not assumed to provide independent writable clones on DuckDB. Migration requires SQL, adapter configuration, and integration-test changes.

## Consequences

The local platform supports a reproducible implementation at the current dataset size, with serialized database access across processes. Database, adapter, and orchestration versions must be pinned after compatibility checks. Database files and runtime volumes should reside outside OneDrive synchronization; the source repository can remain in its current location.

References: [DuckDB concurrency](https://duckdb.org/docs/lts/connect/concurrency), [dbt-duckdb](https://github.com/duckdb/dbt-duckdb), [Snowflake clone storage](https://docs.snowflake.com/en/user-guide/tables-storage-considerations).
