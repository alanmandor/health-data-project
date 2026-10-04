# Documentation

This index covers the implemented architecture, data quality rules, verification results, and documented trade-offs.

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
- [Warehouse bootstrap and raw ingestion](raw_ingestion.md)
- [dbt staging and data quality](dbt_staging_quality.md)
- [Data Vault loading](data_vault_loading.md)
- [Quality issue lifecycle](quality_issue_lifecycle.md)
- [Affected participant dates](affected_dates.md)
- [Dimensional marts](dimensional_marts.md)
- [Daily analytics and readiness](daily_analytics.md)
- [Weekly frozen biostatistics](weekly_biostatistics.md)
- [Local verification results](verification_results.md)
- [Project scope, limits, and trade-offs](scope_and_limitations.md)
- [Airflow phase one](airflow_phase_one.md)
- [Docker quick start](docker_quickstart.md)
- [ADR 001: Local analytical platform](decisions/001_local_platform.md)
- [ADR 002: Delta deliveries and idempotent updates](decisions/002_delta_contract.md)
- [ADR 003: Data Vault integration model](decisions/003_data_vault_model.md)

The complete local pipeline is implemented: dependency/runtime setup, foundation DDL, ingestion, quality, Data Vault, issue history, affected dates, dimensional marts, daily analytics/readiness, and frozen weekly publication. The foundation now includes 60 tables and nine schema tests, with three quality reference catalogs and three additional domain/migration tests mapped in the [foundation guide](database_ddl.md). Automatic scheduling still requires a daily execution time, operational availability timeout, and producer inventory mapping. Snowflake DDL and permissions require validation in a real cloud account.
