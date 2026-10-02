# Documentation

We will document architecture, data quality rules, validation results, and trade-offs as we implement them.

- [Architecture outline](architecture.md)
- [Initial data profile](data_profile.md)
- [Processing and incremental loading strategy](processing_strategy.md)
- [Data quality and correction policy](data_quality_policy.md)
- [Analytics and biostatistics publication schedule](publication_schedule.md)
- [Airflow orchestration design](airflow_design.md)
- [Delivery manifest contract](delivery_manifest.md)
- [Time storage and study calendar policy](time_policy.md)
- [Logical table catalog and column contracts](table_catalog.md)
- [Database DDL and consumer access contracts](database_ddl.md)
- [Airflow phase one](airflow_phase_one.md)
- [Docker quick start](docker_quickstart.md)
- [ADR 001: Local analytical platform](decisions/001_local_platform.md)
- [ADR 002: Delta deliveries and idempotent updates](decisions/002_delta_contract.md)
- [ADR 003: Data Vault integration model](decisions/003_data_vault_model.md)

These documents record accepted design decisions, not completed implementation. Runtime version pins, physical model specifications, the daily execution time, and source-availability polling and timeout settings remain implementation prerequisites.
