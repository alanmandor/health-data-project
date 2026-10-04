# dbt Staging and Data Quality

Status: implemented locally with dbt-duckdb. These internal relations prepare Data Vault loading; they do not publish analytics or biostatistics data.

## Execution and ownership

The daily DAG runs `initialize_warehouse -> capture_delivery -> load_raw -> archive_delivery -> build_staging_quality -> load_data_vault`, with delivery availability also gating capture. Every database task uses the single-slot `duckdb_access` pool. Files are archived after successful raw persistence. A subsequent dbt failure retries from raw without recovering or rereading incoming files. [Data Vault loading](data_vault_loading.md) documents the additional transactional phase.

`src/health_pipeline/dbt_quality.py` invokes the isolated dbt executable, copies the committed profile template into writable runtime storage, and writes dbt logs, compiled SQL, and `run_results.json` under `/opt/airflow/project-data/dbt`. SQL source files remain read-only. No new dependency or image rebuild is needed for this phase when the existing pinned image is available.

The foundation creates provisional staging views only if absent. dbt replaces them with its managed definitions; repeated warehouse bootstrap preserves those definitions. Consumer views and the 60 foundation tables remain separate from these additional internal models. Current schema and verification counts are maintained in the [foundation guide](database_ddl.md).

## Model sequence

| Relation | Purpose |
| --- | --- |
| `staging.stg_records` | Materialized typed raw occurrences with batch lineage, original JSON, safe numeric parsing, UTC timestamps, and local dates |
| `staging.stg_participants`, `stg_device_metadata`, `stg_steps`, `stg_heart_rate`, `stg_sleep_sessions`, `stg_wellness` | Source-specific views over typed occurrences |
| `staging.stg_sleep_stages` | Ordered stage occurrences; original strings and reported durations remain available |
| `business.sleep_diagnostics` | Session-level structural checks and independent duration, timeline, and boundary alerts |
| `business.quality_candidates` | Normalized payload hashes, business keys, validity reasons, and within-batch duplicate classification |
| `business.quality_participants_current` | Latest accepted authoritative participant profile, with tombstones applied |
| `business.record_quality` | Exactly one primary decision per raw batch occurrence |
| `business.quality_current_base` | Latest accepted version per key, selecting tombstones before removing them |
| `business.quality_heart_rate_flatlines` | Exploratory flags for at least 60 consecutive valid current minutes with constant heart rate |
| `business.quality_current` | Deduplicated current candidates, including warning flags |
| `business.quality_reconciliation` | Primary counts by batch, source, and decision |

These quality relations remain occurrence-level candidate projections. The Data Vault phase populates `business.version_decisions` using persistent version identities. The [issue/resolution ledger](quality_issue_lifecycle.md) records durable history and publication-linked decisions. Historical raw occurrences and delivery sequences remain available.

## Decisions and current state

The exclusive primary categories are `accepted`, `rejected`, `pending_reference`, `conflict`, and `identical_duplicate`. A conflicting duplicate group takes precedence over row validity; otherwise structural rejection precedes missing references, followed by duplicate collapse. Firmware disagreements are conflicts. Secondary warning arrays are not additional primary records.

Identical normalized occurrences retain raw lineage and choose the lexicographically smallest record ID as their representative. Canonical hashes use typed values and UTC microseconds, not original JSON property order or ingestion time. Explicit hourly step rates normalize by 60; absent units and explicit `steps` represent minute counts. Other units are rejected. Integer fields cannot silently round fractional values. Source decimal fields reject nonzero precision beyond six decimal places rather than rounding before hashing; trailing zeros and representable scientific notation remain valid. Identifiers are trimmed and retain their original case.

Business keys include source system, source domain, participant ID, and the domain-specific device, UTC instant, or observation date. The initial deployment contains one study per database. Multiple studies or cross-producer identity integration require an explicit study/namespace contract before mixing their records in this database.

A higher delivery sequence wins only among accepted candidates. Rejected or conflicting corrections preserve the earlier accepted value. An accepted key-only delete removes current state without reviving an older upsert. Missing records in a delta do not delete anything.

Unknown participants and mismatched device assignments remain pending. All stored occurrences are reevaluated when the participant registry grows; there is no 50-participant limit. Reference resolution currently uses the latest accepted registry, rather than inventing historical enrollment or assignment effective dates. A participant tombstone makes dependent occurrences pending until an authoritative profile becomes available again.

## Sleep and warning behavior

Internal awake intervals followed by more sleep are valid. Reported sleep times, non-awake stage durations, efficiency, and the complete stage set remain available. Duration/timestamp discrepancies, stage gaps or overlaps, and out-of-session stages generate warnings. Structurally unreadable stages or invalid session boundaries are rejected. Restlessness must fall within the dictionary's 0–1 range.

Post-boundary awake stages remain present here. Their conditional exclusion belongs only to future biostatistics consolidation, after the registered cutoff. No correction deadline, stage removal, clinical correctness claim, or efficiency replacement is introduced by staging.

High normalized step counts receive review flags. Heart-rate flatline detection uses accepted current observations, collapses identical duplicates, and breaks runs at gaps or changes in value. Flatline warnings appear on `quality_current`; occurrence reconciliation retains its independent primary counts.

## Incremental boundary and retries

Raw ingestion is already incremental and idempotent. This first dbt phase rebuilds its internal tables from complete raw history. Full reevaluation deliberately resolves late master data and corrections without a premature timestamp watermark that could skip them. The next Data Vault phase can add batch checkpoints and affected-key reevaluation while retaining these acceptance rules.

dbt replaces models individually; the entire build is not one atomic publication transaction. A failed build can leave mixed internal generations. A successful dbt exit and passing invariant tests gate the task's `staging_quality_validated` result. This result is not a daily readiness manifest. Downstream loading must depend on that success, and publication must still implement its own atomic input freeze and gate.

Each candidate carries rule and code versions. The runner hashes committed dbt SQL/YAML files in deterministic path order; exact retries reproduce decisions with unchanged raw inputs and configuration. Calendar changes remain versioned contract changes. Runtime artifacts describe the last build, while raw deliveries provide durable input history; an append-only quality audit ledger remains part of the Data Vault work.

## Run and inspect

Run only while no other process holds or writes the warehouse. Airflow acquires the pool automatically; direct commands require operational coordination.

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.dbt_quality
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -p test_dbt_quality.py -v
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_staging_quality.py --baseline
```

Inspect the SQL in `dbt/models` and `dbt/tests`. The project has no external dbt packages. The profile template uses the existing `PROJECT_DATABASE_PATH`; UTC is the database session time zone, and `STUDY_TIMEZONE` controls naive source timestamp interpretation and local dates. Profiles, credentials, runtime databases, and generated dbt artifacts are excluded from Git.

The dbt tests check raw-to-quality count reconciliation, exclusive decision values, one decision per occurrence, accepted-record invariants, and unique current keys. Expected bad source records are quarantined and do not fail these tests. Synthetic integration checks cover more than 50 participants, late reference resolution, duplicate UTC representations and normalized units, off-grid timestamps, fractional integer rejection, firmware provenance/conflicts, internal and post-boundary awakenings, consecutive-minute flatlines, bootstrap preservation, exact replay, invalid later corrections, and tombstones.

Snowflake foundation DDL remains available, but these transformation macros use DuckDB functions. A Snowflake dbt profile and adapter-specific normalization/materialization macros must be implemented and tested before running this project there.

## Verified exercise results

The staging/quality milestone succeeded against the already archived baseline with 16 models and five invariant tests. The additional Vault preparation now adds two views and one structural-loadability test. Quality reconciliation retains all 4,032,814 raw occurrences without dropping quarantined or duplicate rows from the audit projection.

| Source | Accepted representatives | Conflicting occurrences | Identical duplicate occurrences | Rejected occurrences | Pending references |
| --- | ---: | ---: | ---: | ---: | ---: |
| Participants | 50 | 0 | 0 | 0 | 0 |
| Device metadata | 50 | 0 | 0 | 0 | 0 |
| Steps | 2,005,573 | 7,172 | 2,177 | 28 | 0 |
| Heart rate | 2,003,983 | 10,356 | 589 | 22 | 0 |
| Sleep | 1,400 | 0 | 0 | 0 | 0 |
| Wellness surveys | 1,400 | 0 | 0 | 0 | 14 |

Counts describe primary categories, including accepted representatives with warnings. Conflict counts count all occurrences in each group, rather than the number of business keys. Conflict precedence can include an individually invalid occurrence, so rejection counts are not the sum of every independent diagnostic.

All 14 P999 surveys remain pending reference resolution. Fifteen missing firmware versions are recovered from device labels with provenance. P001 on January 21 retains 327 reported non-awake minutes, 100% reported efficiency, and its 55-minute awake stage with `accepted_with_warning` status.

At this initial quality milestone, validation passed 10 calendar/manifest tests, 14 raw ingestion tests, six foundation tests, and the dbt correction scenario. Subsequent integration extends this verification through [daily analytics](daily_analytics.md) and [weekly biostatistics](weekly_biostatistics.md), including isolated repeated DAG runs and immutable publication revisions.
