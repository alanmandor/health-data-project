# Persistent quality issue lifecycle

The daily pipeline reconciles Business Vault quality issues after committing the matching Data Vault evaluation. dbt owns typed records, rule decisions, and accepted current projections. A transactional Python/SQL task consumes those models to maintain durable issue history; dbt does not rebuild the historical issue tables.

## Storage and identity

- `business.quality_issues`: one issue per batch, source record, and rule. Its SHA-256 identity uses a tagged JSON array. First-seen time and source lineage remain unchanged on retries. `current_status` is `open` or `resolved`.
- `business.quality_resolutions`: immutable lifecycle transitions, including `reopened` and publication-linked `excluded_at_consolidation`. A superseding domain satellite version is linked when available. Consolidation transitions identify the publication and immutable issue; regular daily transitions retain their evaluation identity.
- `business.quality_evaluations`: committed checkpoints linked to the matching Vault evaluation and a hash of the issue task's Python and SQL implementation.

Rejected records without a loadable Vault representation still produce issues through their raw batch and record IDs; their `version_id` remains null. Identical duplicates do not create issues. Conflicting source occurrences each retain their own issue and lineage.

## Opening and closing issues

### Enforced value domains

DuckDB enforces `CHECK` constraints on `current_status` (`open`, `resolved`),
`severity` (`error`, `warning`, `pending`), and the six transition codes listed below,
including `excluded_at_consolidation`. `reopened` is a transition code, not a current
status. The canonical definitions are in `scripts/generate_ddl.py`. Three reference
catalogs provide the same English code, display name, and description in both engines:

| Catalog | Equivalent ledger field | Rows |
| --- | --- | ---: |
| `business.quality_issue_statuses` | `quality_issues.current_status` | 2 |
| `business.quality_severities` | `quality_issues.severity` | 3 |
| `business.quality_resolution_codes` | `quality_resolutions.resolution_code` | 6 |

These are descriptive equivalence tables, not a configurable workflow. There are no
new foreign keys or changes to alert transitions. Adding a value requires a reviewed
catalog, constraint, and pipeline change. Existing codes are inserted only when absent;
repeated bootstrap does not duplicate or overwrite catalog rows.

Fresh warehouses receive populated catalogs through `001_foundation.sql`. Existing
warehouses can run `003_quality_catalogs.sql` for their engine without rebuilding the
ledger. In DuckDB, install and verify them while warehouse tasks are idle:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_quality_catalogs.py --apply
```

Join `current_status`, `severity`, and `resolution_code` to the corresponding catalog
`code` to obtain the English labels and definitions. Snowflake catalog DDL and seeds
are provided for review but have not been executed in a cloud account.

Fresh databases receive these constraints through the foundation DDL. Existing
databases require the transactional `002_quality_domains.sql` migration because
`CREATE TABLE IF NOT EXISTS` does not change an existing table. Run with every
warehouse task idle:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/migrate_quality_domains.py
```

The migration preserves identifiers, timestamps, source links, and all history.
Invalid existing values abort the transaction; they are never silently corrected or
deleted. Repeating the migration preserves the same records. Snowflake foundation
DDL continues to omit `CHECK` constraints; domain validation remains a pipeline
responsibility there until the cloud implementation is tested.

Semantic warnings on accepted source records create warning issues. Current projections determine whether the warning is still active; this includes contextual heart-rate flatline warnings. Internal awakenings followed by more sleep are valid and do not create a sleep-boundary issue.

Rejected, conflicting, and pending-reference records create error or pending issues. They remain open until a later accepted delta supersedes the same business key, or the original rule stops applying. A later invalid record cannot resolve an earlier warning or displace the preceding accepted state. Records without a usable business key cannot be matched automatically to later corrections.

| Transition code | Meaning |
| --- | --- |
| `reference_resolved` | The original pending record is now accepted after reference data arrives. |
| `superseded_by_accepted_delta` | A later accepted upsert supersedes the affected occurrence. A new warning on that replacement creates its own issue. |
| `source_deleted` | An accepted tombstone removes the key from current output; this does not establish clinical correctness. |
| `rule_cleared` | The original condition no longer applies, including contextual warning changes. |
| `reopened` | A previously closed issue becomes active again, such as a missing reference after participant deletion. |

Historical source occurrences can be registered and closed in the same evaluation when an accepted correction already exists. `first_seen_at` records operational detection time, not a reconstructed historical detection time. Reconciliation reevaluates retained issue conditions, so an exact retry preserves all issue and transition timestamps.

## Transaction and orchestration

`reconcile_quality_issues` runs after `load_data_vault`, in the single-slot `duckdb_access` pool. It requires the validated database, dbt code, study, unchanged raw fingerprint, and a matching committed Vault checkpoint. Issue inserts, status transitions, immutable history, and the evaluation checkpoint commit together. Failed attempts roll back and remain retryable.

The task uses the Vault memory limit, one DuckDB thread, disk-backed temporary storage, and cleanup before commit. Accepted-key lookup is restricted to keys with observed or retained issues. The local SQL implementation is DuckDB-specific; generated Snowflake DDL provides the same physical tables but does not constitute a tested Snowflake task.

## Correction cutoff and publication

P001's post-session awake stage remains in raw, Vault history, and provisional output while its boundary issue stays open. Reported sleep duration and efficiency are preserved. This task does not enforce the weekly cutoff or remove sleep stages.

The [weekly publication step](weekly_biostatistics.md) links permitted terminal awake exclusions to the frozen publication through `excluded_at_consolidation`. It closes the broad boundary issue only when retained stages clear the boundary discrepancy completely. Daily reconciliation does not reopen that resolved immutable issue merely because its unchanged provisional stage remains. Internal awakenings and unrelated discrepancies remain represented.

[Affected-date propagation](affected_dates.md), [dimensional marts](dimensional_marts.md), and [daily readiness](daily_analytics.md) run downstream as separate transactions. Weekly publication freezes accepted selections and applies publication-specific resolutions atomically.

## Verification

Run the integration fixture with the dbt Python environment. It exercises issue rollback, exact retry, reference resolution and reopening, conflicting-delta correction, rejected correction preservation, accepted sleep correction, tombstones, and flatline warnings:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -p test_dbt_quality.py
```

For a validated and committed warehouse with no concurrent writer:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.quality_issues
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_quality_issues.py --baseline
```

The October 4, 2026 baseline evaluation committed 17,578 open error issues, 14 open pending-reference issues, and 22,290 open warning issues. These are issue occurrences, not distinct participants or sessions. Independent reconciliation passed, P001's boundary warning remained open, and no publication or stage exclusion was recorded. A second invocation reused the same checkpoint without adding issues or transitions.

The updated Airflow DAG also completed twice against isolated synthetic files and a temporary warehouse. The second issue task reused its evaluation checkpoint, and the input file was archived exactly once. Foundation checks and generated DDL consistency passed at this implementation milestone. The current foundation has 60 tables and nine tests; see [verification results](verification_results.md).
