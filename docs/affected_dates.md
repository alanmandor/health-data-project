# Affected participant dates

The daily DAG queues affected dates after quality issue reconciliation. The queue identifies participant-day inputs that changed; it does not update facts or publish analytics.

## State comparison

`business.participant_date_state` stores the most recently propagated signature at `(study_hk, participant_hk, local_date)`. The signature includes:

- Accepted current steps, heart-rate, sleep, and wellness values, including normalized sleep stage content.
- Current quality status and sorted warning codes, including contextual flatline warnings.
- The authoritative participant profile, accepted metadata for the reported device, and the number of authoritative participants reporting that device. Assignment ambiguity changes therefore invalidate every affected owner.
- The validated dbt code, impact implementation, and study timezone.

Minute observations use the study's local date, currently `America/Bogota`; sleep and wellness use the producer's wake or survey date. The snapshot includes every configured study day for each authoritative participant, including days without accepted observations. Accepted observations outside the configured study window are also tracked explicitly. Their presence does not extend the configured study window or establish publication eligibility.

The implementation aggregates fixed-width record hashes in deterministic business-key order before hashing each day. Batch IDs and load timestamps are excluded from semantic signatures: repeated accepted payloads do not create work merely because they arrive again. Downstream facts must preserve valid lineage without requiring a rewrite for every unchanged source appearance; Vault lineage retains all appearances.

## Delta behavior

| Input change | Queued work |
| --- | --- |
| First accepted participant state | Configured coverage days and any additional observed dates. |
| Valid measurement or sleep correction | Dates whose accepted values or warnings change. |
| Invalid or conflicting correction | No replacement of preceding accepted values; no work unless another accepted input or contextual warning changes. |
| Accepted observation tombstone | Its previous day if accepted daily inputs change. Coverage days remain represented even without observations. |
| Participant reference arrives | Newly authorized coverage days, including previously pending accepted observations. |
| Participant reference is deleted | All previously tracked dates for that participant, allowing future facts to be removed. |
| Profile, device assignment, or accepted device metadata changes | All tracked dates for that participant. Source-effective dates are unavailable, so this is conservative invalidation rather than invented historical attribution. |
| Identical delta or exact retry | No additional semantic changes; exact retries reuse the checkpoint. |
| Rule or implementation change | Reevaluate and conservatively invalidate tracked inputs. |

A timestamp/date change creates a different business key. The producer must supply a tombstone for the old key and an upsert for the new key to represent a move; the pipeline never infers deletion from absence. That pair affects both old and new days when their daily inputs change.

## Queue and transaction

`business.affected_participant_dates` is keyed by study, participant, day, and deterministic `change_id`. Reasons are `initial_state`, `current_state_changed`, or `current_state_removed`. Each change ID includes the impact evaluation and preceding/new signatures, so a legitimate A-to-B-to-A correction remains observable. Reason codes describe snapshot transitions rather than claiming a particular clinical cause.

`business.impact_evaluations` links each committed evaluation to its matching Vault and current quality issue checkpoints, implementation hash, affected-date count, and commit timestamp. Unchanged days keep their prior state and evaluation provenance. Queue insertion, snapshot changes, and the checkpoint commit atomically; a failed attempt rolls all three back.

`queue_affected_dates` uses the single-slot `duckdb_access` pool, one execution thread, the configurable managed-memory limit, and disk-backed temporary storage. It requires matching validated code/study, an unchanged raw fingerprint, committed Vault data, and the current issue implementation's checkpoint. Retained accepted history is reevaluated to detect contextual and reference changes; this first implementation does not limit input scanning to the new batch.

## Future mart consumption

The [mart loader](dimensional_marts.md) deduplicates pending work by study, participant, and day, updates accepted groups with transactional delete/insert, and sets `consumed_checkpoint_id` only with a successful mart checkpoint. Multiple outstanding changes for a day are coalesced into one rebuild while retaining queue history. New work remains pending if it arrives after the consumer's captured scope. Compact daily aggregates currently recompute from accepted facts; the date queue provides the basis for a future scoped aggregate optimization.

Propagation leaves new entries unconsumed. The [dimensional mart task](dimensional_marts.md) now captures that work and consumes it atomically with its output checkpoint. Daily aggregate rebuilding must capture dimensional mart checkpoints separately before daily readiness is enabled. Neither task emits daily readiness or alters frozen biostatistics publications. Week-level invalidation and publication revisions require their own implementation.

The generated DuckDB and Snowflake DDL includes the queue, state table, and impact checkpoint. The local propagation SQL uses DuckDB and requires a separately tested Snowflake implementation.

## Local verification

With no concurrent warehouse writer and committed upstream evaluations:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.affected_dates
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_affected_dates.py --baseline
```

The integration fixture covers rollback, exact retry, local-date conversion across UTC midnight, late references, participant deletion, rejected and unchanged deltas, date moves with tombstones, profile corrections, and pending queue consumption.

On October 4, 2026, the full baseline committed 1,400 state rows and 1,400 pending queue entries: 50 authoritative participants across 28 configured study days. P999 remained excluded. Independent verification passed, and a second invocation reused the same impact checkpoint without adding work. All six foundation tests passed with 49 tables; generated DuckDB and Snowflake DDL matched the central contract.

The updated Airflow DAG completed twice with isolated synthetic files and a temporary database. The fixture contained only a rejected measurement and no authoritative participants, so its impact count was zero. The second impact task reused the checkpoint, and the source file was archived exactly once.
