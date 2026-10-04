# Data Vault Loading

Status: DuckDB implementation. dbt prepares identities and candidate versions; a dedicated Airflow task commits persistent Vault objects and their quality decisions atomically. Consumer marts and publication remain separate work.

## Entity mapping

| Source | Hub | Links | Satellites |
| --- | --- | --- | --- |
| Participants | Participant, device | Participant-device, participant-study | Participant profile, enrollment, reported assignment |
| Device metadata | Device, observed participant | Participant-device | Device metadata, assignment context |
| Steps | Observed participant | None per minute | Steps observation, discriminated by study and UTC timestamp |
| Heart rate | Observed participant | None per minute | Heart-rate observation, discriminated by study and UTC timestamp |
| Sleep | Participant, sleep session | Participant-session | Session and its complete ordered stage set |
| Wellness | Participant, wellness response | Participant-response | Response scores |
| Project configuration | Study | Referenced by membership and event keys | Registered study configuration |

There are five hub tables, four link tables, and ten satellite tables, including the study and dependent sleep-stage satellites. `vault.version_lineage` connects persisted versions to every contributing batch/record occurrence. Existing foundation tables are populated without replacement.

The configured study is `pmdata_2026_01`, with January 8 inclusive and February 5 exclusive as its study window. `STUDY_CODE`, `STUDY_START`, `STUDY_END_EXCLUSIVE`, and `STUDY_TIMEZONE` are explicit project settings, documented in `.env.example`. Their defaults describe the supplied exercise. The loader registers the configuration before loading observations and refuses to silently change an existing study or mix studies in one database. Different studies require separate databases or a deliberate multistudy contract migration.

## SQL and orchestration

The daily dependency chain is:

```text
load_raw -> archive_delivery -> build_staging_quality -> load_data_vault
```

All database tasks acquire `duckdb_access`. The DAG remains manually scheduled, with no analytical readiness or publication marker emitted by this phase.

SQL locations:

- `dbt/models/raw_vault/dv_record_input.sql`: namespace-aware hub/link identities and structural eligibility.
- `dbt/models/raw_vault/dv_satellite_candidates.sql`: satellite payloads, discriminators, and version identities.
- `dbt/macros/vault.sql`: tagged canonical JSON-array SHA-256 encoding.
- `dbt/operations/load_vault.sql`: append hubs and links, and materialize the candidate set inside the transaction.
- `dbt/operations/vault_business_views.sql`: accepted-version, accepted-change, and current-state projections.
- `src/health_pipeline/vault_loading.py`: transaction boundaries, typed satellite inserts, stages, lineage, decision history, integrity checks, and commit checkpoints.

The loader reads foundation column definitions from DuckDB's catalog to cast the prepared JSON payload into each satellite's declared types. These are bulk SQL inserts, not Python loops over source records. Inserts append missing keys using anti-joins; no MERGE or deletion of historical Vault records is used. The future mutable mart projections can still use the agreed transactional delete/insert strategy.

## Identity and history

`dv-json-array-v1` hashes tagged, ordered arrays with explicit nulls. Hubs hash their entity name and namespace/business key; links hash their relationship name and endpoint keys. Case is preserved. Sleep and survey hubs include study, participant, and observation date under the one-event-per-participant-day contract.

Satellite version identity includes relation, parent, study, source business key, dependent discriminator, immutable batch, operation, and source payload hash. This separates domains sharing an assignment satellite, preserves participant attribution for device metadata, and allows A → B → A to create distinct versions. Source values and original timestamp strings are preserved; recovered firmware and normalized steps remain business transformations rather than replacements of satellite fields.

Raw satellites retain one distinct candidate per source payload/key/batch. A newly declared delta that repeats a source state can therefore add a source-observation version. `business.vault_accepted_changes` compares each accepted source state with its preceding accepted state and suppresses unchanged states for business change history. Exact replay of the same delivery adds no versions or lineage and does not change stored load times. Identical occurrences share a version when their canonical source payload is identical; semantic duplicates with different reported units or timestamp strings can retain different source versions while quality selects one analytical representative.

The source business key remains domain-specific. In particular, assignment observations from participants and device metadata remain distinguishable. Generic current-version projections do not resolve device reassignment or infer historical device attribution for minute events.

## Structural retention and authorization

Structurally loadable observations include business-rejected measurements, conflicting groups, and unknown references. Their decisions are preserved separately. An observed P999 identity can therefore have a participant hub and response satellite while remaining analytically ineligible. Enrollment links and satellites originate from participant-master records; events alone never enroll a participant.

Unparseable identities, measurement fields, or required timestamps remain only in immutable raw storage and the occurrence-quality projection. Integer enrollment fields must fit the foundation's SMALLINT representation; values outside that storage contract are rejected in quality rather than failing satellite insertion.

`business.version_decisions` appends decision changes. Its latest projection is `business.vault_decisions_current`. Identical duplicate occurrences map to the ledger's supported `accepted` category with `reason_code=identical_duplicate`; projections exclude that reason unless an identical source version also has the representative accepted occurrence. The original five-category reconciliation remains available in `business.record_quality`.

`business.vault_current_versions` selects the latest accepted delivery per relation/source business key, including tombstones before removing deleted current state. Rejected or conflicting later versions cannot replace accepted values. Accepted current projections additionally require the current authoritative participant registry. They are operational views, not historical enrollment intervals or frozen publication inputs.

Unknown references can move from pending to accepted after a later participant delta. The source satellite remains immutable; the prior decision remains in the append-only ledger. [Weekly publication](weekly_biostatistics.md) freezes exact accepted versions and readiness/rule fingerprints before calculating immutable outputs.

## Sleep, enrollment, and effective time

A sleep correction creates a new session version and its entire stage set. Older session and stage versions remain intact. Internal awakenings and post-boundary awake intervals stay in the Vault. Conditional post-boundary exclusion belongs only to future biostatistics consolidation; no stage is removed here.

Enrollment status records a reported source membership. Enrollment and withdrawal dates remain null because the source does not supply them. Coverage dates remain null with `coverage_basis=not_supplied`; the configured study window is not silently presented as an individual's enrollment interval. Assignment effective dates remain null as well.

Minute observations retain their actual event instant as source effective time. Calendar-only survey dates are not converted into invented midnight timestamps. Vault `loaded_at` records the first successful insertion time and is unchanged on replay.

## Validation and atomic retries

Successful dbt builds write `validated.json` under writable runtime storage. The certificate identifies the database, study/calendar, dbt code hash, and immutable raw batch/count fingerprint. The loader refuses stale certificates after raw or model changes. `vault_accepted_loadable` verifies that accepted quality representatives fit structural loading requirements.

Within one DuckDB transaction, the loader registers the study, appends entities and versions, loads stages and lineage, appends changed quality decisions, checks satellite parents, link endpoints, stage parents, source lineage, candidate persistence, and decision reconciliation, then writes `business.vault_loads`. Any failure rolls back the entire phase, including the checkpoint. A crash after commit is recovered through that checkpoint; an exact retry returns `reused=true`.

Vault SQL uses one DuckDB execution thread, disables insertion-order preservation, and defaults to `VAULT_MEMORY_LIMIT=4GB` for engine-managed working memory. Intermediate data can spill to the writable runtime's `vault-temp` directory. Large satellite, lineage, and decision inserts and reconciliation checks are partitioned into deterministic SHA-256 prefix buckets inside the same transaction, reducing their working sets. This leaves room for Airflow, PostgreSQL, and allocations outside DuckDB's managed limit in the shared Docker VM. The setting is configurable and does not constitute a hard operating-system cap. The complete baseline initially received SIGKILL with unrestricted query resources; recovery verified zero committed Vault records. Managed-limit errors during full inserts and unpartitioned final checks also rolled back, motivating bounded inserts and equally bounded integrity checks. The baseline validation uses a Docker VM with approximately 8GB of RAM.

The checkpoint stores the input, quality-code, and loader-code hashes. A changed input or implementation requires a new evaluation. Completed evaluations never rewrite prior observation history. SQL templates are included in the dbt project hash. The decision ID hashes evaluation ID, source relation, and version ID, providing a deterministic connection to its evaluation checkpoint without using task wall-clock time as identity.

The first implementation reevaluates complete retained history when inputs change. Physical inserts remain incremental by immutable key. A later optimization can narrow evaluation to affected keys and newly arrived batches, but it must still resolve late references and preserve preceding accepted-state semantics.

## Local commands

Use the Airflow DAG for coordinated execution. Direct commands require no concurrent warehouse writer:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.dbt_quality
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.vault_loading
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -p test_dbt_quality.py -v
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_data_vault.py --baseline
```

No new dependency or image build is required for the SQL and Python source changes. Recreate services when changing Compose environment settings. The Snowflake foundation includes matching physical objects and the checkpoint table, but the DuckDB loader and normalization macros require a separately tested Snowflake implementation.

Before committing, the loader drops its temporary decision view and candidate table after all integrity checks and the evaluation checkpoint have been written. A commit failure may automatically abort the DuckDB transaction; rollback handling preserves the original exception in that case. The managed memory limit is not a process-wide cap, so the Docker VM must also accommodate Airflow, PostgreSQL, and unmanaged allocations. See [DuckDB's out-of-memory guidance](https://duckdb.org/docs/current/guides/performance/oom).

## Baseline verification

On October 4, 2026, the complete baseline committed successfully with the 4GB managed-memory limit, one execution thread, bucketed operations, and temporary-relation cleanup before commit. An independent read-only reconciliation passed after reconnecting. A second loader invocation returned `reused: true` with the same evaluation ID and one committed checkpoint. The pending Airflow run subsequently completed successfully on retry, and the DAG was restored to its paused manual-execution state.

The database contains 2,012,769 steps observation versions, 2,014,361 heart-rate observation versions, 1,400 sleep session versions, 7,000 sleep stages, and 4,032,964 source-lineage records. There are 51 observed participant identities and 50 study enrollments. The 14 unknown-participant responses remain pending; P001's reported efficiency of 100% and provisional 55-minute awake stage remain intact. Transaction rollback, corrections, deletes, late references, and retry behavior also passed the integration test fixture.

[Persistent quality issue/resolution records](quality_issue_lifecycle.md) and [affected-date propagation](affected_dates.md) run in separate downstream transactions. Remaining work: marts, daily readiness, and frozen weekly publication. None of these are implicitly marked complete by a successful Vault load.
