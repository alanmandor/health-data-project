# Dimensional marts

The star schema contains participant, device, study, and date dimensions; separate minute facts for steps and heart rate; sleep session and stage facts; daily wellness facts; and one Gold assignment row per authoritative participant.

`dbt_marts` is a separate dbt project using the existing dbt-duckdb environment. It reads the validated quality and Vault contracts after affected-date propagation. Keeping the analytical project separate prevents changes to a mart model from changing the ingestion project's validation hash. Its own model and loader hashes are recorded in `business.mart_evaluations`.

## Models and transaction

Eight dbt models prepare a scoped record table and candidate views in the internal `business` schema. They do not replace constrained foundation tables. Canonical lineage uses the latest accepted payload change matching current normalized values, preserving A-to-B-to-A changes while suppressing repeated identical appearances. Vault retains all source appearances.

The loader captures pending dates in `business.mart_build_scope`. A first build or changed mart implementation also includes every currently tracked participant date. dbt candidate preparation happens before the output transaction. Preparation failures do not consume queue entries or modify dimensions or facts.

The output transaction closes superseded current dimension states, inserts new dimension versions, resolves Gold assignment context, deletes facts for the captured participant dates, and inserts their accepted replacements. It replaces the complete stage array for affected sleep sessions. It validates accepted fact counts and dimension keys, records its checkpoint, and consumes corresponding queue work atomically. An error rolls the output transaction back; retries remain safe. An exact completed retry reuses its checkpoint.

The single-slot `duckdb_access` pool serializes preparation and loading. Both dbt and the loader use one execution thread and the configurable managed-memory limit. Large minute inserts use SHA-256 prefix buckets within the same transaction.

## Dimension knowledge and attribution

Participant and device surrogate keys hash a tagged array of entity identity and canonical source version. Attribute versions are retained once observed by the mart; the first mart build does not invent earlier mart knowledge intervals. `known_from_at` and `known_to_at` represent mart knowledge time. Current facts use the dimension state known at their latest rebuild, with participant reference changes invalidating all tracked dates. These dates are not producer-effective enrollment or assignment dates.

Authorization and metadata availability can change independently of attributes. If the same previously known device version becomes eligible again, its retained dimension identity is reused and its current flag is restored. Detailed authorization gaps remain in Vault and quality history; dimension intervals do not reconstruct those gaps.

Gold retains one row per authoritative participant. Shared reported devices are flagged `ambiguous_assignment`; missing accepted metadata is `pending_metadata`; inconsistent current metadata is `ambiguous_metadata`. Device keys in minute facts remain null in those cases. Accepted current metadata is used only for an unambiguous reported assignment. This does not claim historical device attribution.

Age and maximum heart rate remain enrollment context rather than global participant attributes. Firmware derived from the device label includes provenance. Study dates use the configured calendar; minute facts use Bogotá local dates and UTC event instants.

## Provisional sleep and publication

Sleep totals sum reported non-awake stage durations; deep-sleep percentage divides reported deep minutes by that sum. Reported efficiency and restlessness remain unchanged. All stages, including a discordant post-boundary awake stage, remain in the provisional star schema with a warning flag. Internal awakenings remain valid. Weekly exclusions require the separate consolidation process.

This phase does not populate `participant_day_current`, emit daily readiness, or publish biostatistics snapshots. Existing consumer publication gates remain closed. Accepted dimensions and Gold are available through their existing reader views. Generated Snowflake DDL provides matching structures, but local dbt models and loading SQL remain DuckDB-specific.

## Commands

After upstream evaluations have committed, with no concurrent warehouse writer:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.mart_loading
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_marts.py --baseline
```

The integration fixture verifies rollback and unconsumed queue work, exact retry, late-reference admission and removal, local date conversion, rejected/repeated deltas, timestamp moves, participant dimension versioning, whole-session stage replacement, shared-device ambiguity and recovery, and enrollment-only changes that preserve participant dimension identity.

## Verified baseline

On October 4, 2026, the complete baseline committed 50 participant dimensions, 50 device dimensions, one study, 28 dates, and 50 Gold assignment rows. Persisted facts contain 2,005,573 accepted steps minutes, 2,003,983 accepted heart-rate minutes, 1,400 sleep sessions, 7,000 provisional stages, and 1,400 wellness responses. Independent reconciliation passed after reconnecting, and all affected-date work was consumed through committed mart checkpoints.

P001's January 21 session retains 327 reported sleep minutes, 100% reported efficiency, and the provisional 55-minute awake stage. The 15 firmware values derived from labels retain their provenance. Unknown participant responses remain excluded from accepted facts. A completed retry reuses its checkpoint. The full DAG also succeeded twice with isolated data; the second mart task reused its result, and its source file was archived once. The six foundation tests passed with 51 tables, and both generated DDL files matched their central definition.
