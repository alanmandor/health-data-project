# Logical Table Catalog and Column Contracts

Status: logical contract with executable foundation DDL. See [database DDL and consumer contracts](database_ddl.md) for deployment and verification. Empty tables and initial staging/consumer views are defined; business current-state transformations, parsing diagnostics, and publication gates remain to be implemented. No analytical data has been loaded.

## Storage and Naming

Use one DuckDB analytical database with raw, staging, vault, business, marts, analytics, and biostatistics schemas. Keep business orchestration controls in the separate SQLite registry on the persistent project-data volume; PostgreSQL remains Airflow metadata only. Do not write project audit tables into Airflow's internal schema.

Names and comments use American English. hk denotes a deterministic SHA-256 hash key represented as VARCHAR; sk denotes a dimensional surrogate key; version_id identifies an immutable source observation version. Integer counters use BIGINT; calendar dates use DATE; instants use TIMESTAMPTZ and UTC session settings. Preserve source timestamp text separately. TIMESTAMPTZ represents an instant, not the original named zone; keep study_timezone and original offsets explicitly. Scores use SMALLINT, source minute counts use BIGINT, normalized step values use DECIMAL(18,6), and percentages/durations use DECIMAL(18,6). Source decimal precision must not be silently truncated before hashing.

Columns described as required are NOT NULL. Other nullable fields represent source absence, unknown effective time, unresolved attribution, or unavailable metrics; zero is not a missing-value substitute. Enforce primary/unique keys and appropriate NOT NULL checks in persisted relations. Use dbt relationship tests where physical foreign keys would obstruct transactional replacement or hub/link loading. This does not waive referential validation.

## Identity and Shared History Columns

- business_namespace is a configured identity namespace. Participant/device keys are unique only within it; the exercise does not establish globally unique identifiers across producers or studies. It must remain stable across deliveries.
- study_code is an explicit project configuration, not a fabricated source field. Its value must be registered before loading the baseline. study_hk distinguishes study observations even if a participant takes part in multiple studies.
- Hub keys hash a canonical JSON array of entity name, namespace, and business-key fields. Link keys hash link name and ordered endpoint keys. Dates are ISO dates; instants are canonical UTC strings; preserve case unless a source contract authorizes normalization. JSON array encoding distinguishes nulls and prevents separator collisions. Record hash_spec_version; do not include load time in identity hashes.
- version_id hashes the logical entity key/discriminator, stable batch_id, operation, and canonical source payload hash. Replaying a batch creates the same version_id. A -> B -> A across different batches creates separate change versions. Row position is lineage, not the version of an otherwise identical duplicate.
- Satellite common columns: version_id VARCHAR PK, parent_hk VARCHAR required, batch_id VARCHAR required, delivery_sequence BIGINT required, record_source VARCHAR required, loaded_at TIMESTAMPTZ required, source_effective_at TIMESTAMPTZ nullable, payload_hash VARCHAR required, hash_spec_version VARCHAR required, operation VARCHAR required (upsert/delete). Event satellites also carry study_hk and their dependent discriminator.
- Hub/link common columns: their hk VARCHAR PK, first_batch_id VARCHAR required, record_source VARCHAR required, loaded_at TIMESTAMPTZ required, hash_spec_version VARCHAR required. Hub business keys and link endpoints are required.

Loaded time describes knowledge arrival. Source effective time remains null when unknown. Tombstones retain their keys and operation; measurement payload columns are null for delete records. Upsert payload requirements are source-specific.

## Control Registry (SQLite)

SQLite stores UTC instants as canonical ISO strings and calendar dates as ISO date strings. The existing deliveries table already has source_system, delivery_id, sequence, manifest_hash, registered_at, and manifest_json. It is a manifest registry, not proof of copied raw data or analytical readiness. Extend it through explicit migrations; do not replace it with a differently shaped table silently.

| Table | Grain / Key | Additional Columns and Responsibility |
| --- | --- | --- |
| deliveries (existing) | PK (source_system, delivery_id); unique (source_system, sequence) | Current columns above; immutable delivery identity and precedence |
| delivery_files | One declared file per delivery; PK (source_system, delivery_id, object_key) | file_id, checksum, bytes, format, version_id nullable, source_name, immutable_raw_path nullable, verification_status, verified_at nullable |
| model_checkpoints | One model/scope/input/rule combination; PK checkpoint_id | model_name, scope_json, input_hash, rule_version, code_version, status, committed_at nullable; retry recovery |
| processing_runs | One execution attempt; PK attempt_id | airflow_run_id, task_id, batch_id nullable, checkpoint_id nullable, started_at, ended_at nullable, status, error_code nullable; attempts may change on replay |
| daily_readiness | One ready revision per study/date; PK (study_hk, target_date, revision) | input_manifest_hash, rule_version, code_version, validated_at, coverage_json, warning_counts_json; inserted only after critical checks |
| publication_registry | One weekly revision; PK publication_id; unique (study_hk, week_start, revision) | event_period_start, event_period_end, scheduled_run_at, correction_cutoff_at, consolidation_date, published_at nullable, status, input_manifest_hash, rule_version, code_version |
| publication_inputs | Exact selected immutable input per publication; PK (publication_id, input_kind, version_id) | source_relation, business_key_hash, payload_hash; observation, dimension, and quality-decision versions |

batch_id is the deterministic hash of source_system and delivery_id, never an Airflow run identifier. Source file identity hashes source_system, object_key, and checksum. Persist publication inputs atomically with the cutoff. Readers resolve only published revisions; a failed build never becomes a published revision.

## Raw and Staging

| Relation | Materialization / Grain | Columns |
| --- | --- | --- |
| raw.source_records | Append-only table; PK record_id = hash(file_id, row_number) | file_id, source_name, row_number BIGINT, payload_json JSON, original_record_text VARCHAR nullable, ingested_at TIMESTAMPTZ; all required except original text |
| raw.batch_records | Append-only lineage table; PK (batch_id, record_id) | source_system, delivery_sequence, object_key; records can occur in more than one delivery without duplicating immutable content |
| staging.stg_participants | Typed view, one original participant row per batch | Lineage + participant_id, device_id, age_at_enrollment, height_cm, gender, chronotype, max_heart_rate |
| staging.stg_device_metadata | Typed view, one original device row per batch | Lineage + participant_id, device_id, wear_site, calibration_date, firmware_version_reported, device_label |
| staging.stg_steps | Typed view, one original observation per batch | Lineage + participant_id, timestamp_original, event_at_utc, steps_reported, unit_reported |
| staging.stg_heart_rate | Typed view, one original observation per batch | Lineage + participant_id, timestamp_original, event_at_utc, heart_rate_bpm |
| staging.stg_sleep_sessions | Typed view, one original session per batch | Lineage + participant_id, wake_date, sleep_onset_original, sleep_end_original, sleep_onset_utc, sleep_end_utc, efficiency_pct_reported, restlessness |
| staging.stg_sleep_stages | Typed view, one stage of an original session | Session record_id + batch_id + stage_position INTEGER; stage_type, start_original, end_original, start_at_utc, end_at_utc, duration_min_reported |
| staging.stg_wellness | Typed view, one original response per batch | Lineage + participant_id, survey_date, fatigue_score, stress_score, readiness_score, sleep_quality_score |

Staging lineage includes record_id, batch_id, record_source, delivery_sequence, operation, parse_status, and parse_error_code nullable. Cast failures retain the raw record and become errors, rather than disappearing. Staging is not deduplicated and must not claim unique business keys. Raw blobs remain in immutable storage; raw.source_records is their queryable record index. A file parser failure can exist in delivery_files without producing records.

## Data Vault Hubs and Links

| Table | Business Key / Endpoints | Entity-Specific Columns |
| --- | --- | --- |
| vault.hub_participant | unique (business_namespace, participant_id) | participant_hk, business_namespace, participant_id |
| vault.hub_device | unique (business_namespace, device_id) | device_hk, business_namespace, device_id |
| vault.hub_study | unique study_code | study_hk, study_code |
| vault.hub_sleep_session | unique (study_hk, participant_hk, wake_date) | sleep_session_hk, study_hk, participant_hk, wake_date; one session/day contract |
| vault.hub_wellness_response | unique (study_hk, participant_hk, survey_date) | wellness_response_hk, study_hk, participant_hk, survey_date; one response/day contract |
| vault.link_participant_device | unique (participant_hk, device_hk) | participant_device_hk, participant_hk, device_hk |
| vault.link_participant_study | unique (participant_hk, study_hk) | participant_study_hk, participant_hk, study_hk |
| vault.link_participant_sleep_session | unique (participant_hk, sleep_session_hk) | participant_sleep_hk, participant_hk, sleep_session_hk |
| vault.link_participant_wellness_response | unique (participant_hk, wellness_response_hk) | participant_wellness_hk, participant_hk, wellness_response_hk |

Use the shared hub/link metadata. Composite sleep/response identities describe the current contract; multiple events per date require a versioned contract and producer event identifier. Do not mutate old hash keys. Hubs establish observed identity, not authoritative enrollment. Known participant hubs must exist before validated links are loaded; unknown events remain recoverable raw/quarantine inputs.

## Data Vault Satellites

All use the shared satellite columns unless a child grain is explicitly stated. Each parent_hk references the matching hub/link key.

| Table | Parent / Dependent Grain | Payload Columns |
| --- | --- | --- |
| vault.sat_participant_profile | hub_participant version | height_cm DECIMAL, gender VARCHAR, chronotype VARCHAR; enrollment-specific values are separated below |
| vault.sat_device_metadata | hub_device version | calibration_date DATE, firmware_version_reported VARCHAR nullable, device_label VARCHAR |
| vault.sat_participant_device | link_participant_device version | wear_site VARCHAR nullable, assignment_status VARCHAR nullable, effective_from TIMESTAMPTZ nullable, effective_to TIMESTAMPTZ nullable |
| vault.sat_study | hub_study configuration version | study_timezone VARCHAR, study_start DATE, study_end_exclusive DATE, source_registry_version VARCHAR; explicit configuration provenance |
| vault.sat_enrollment | link_participant_study version | age_at_enrollment SMALLINT, max_heart_rate SMALLINT, enrollment_status VARCHAR, enrolled_on DATE nullable, withdrawn_on DATE nullable, coverage_start DATE nullable, coverage_end_exclusive DATE nullable, coverage_basis VARCHAR |
| vault.sat_steps_observation | hub_participant + study_hk + event_at_utc + version | event_at_utc TIMESTAMPTZ, timestamp_original VARCHAR, steps_reported BIGINT, unit_reported VARCHAR nullable |
| vault.sat_heart_rate_observation | hub_participant + study_hk + event_at_utc + version | event_at_utc TIMESTAMPTZ, timestamp_original VARCHAR, heart_rate_bpm SMALLINT |
| vault.sat_sleep_session | hub_sleep_session version | sleep_onset_utc TIMESTAMPTZ, sleep_end_utc TIMESTAMPTZ, original boundary strings VARCHAR, efficiency_pct_reported DECIMAL, restlessness DECIMAL |
| vault.sat_sleep_stage | Child of a particular session version; PK (session_version_id, stage_position) | stage_type VARCHAR, start_at_utc/end_at_utc TIMESTAMPTZ, original boundary strings VARCHAR, duration_min_reported DECIMAL; session_version_id carries batch/load metadata |
| vault.sat_wellness_response | hub_wellness_response version | fatigue_score SMALLINT, stress_score SMALLINT, readiness_score SMALLINT, sleep_quality_score SMALLINT |
| vault.version_lineage | One contributing raw occurrence per satellite version; PK (source_relation, version_id, batch_id, record_id) | Maps identical duplicate occurrences to the same observation version |

Minute satellites are dependent observation histories, not one participant-profile state per load. study_hk is a required additional discriminator to avoid cross-study collisions. Preserve structurally parseable source changes and conflicting candidates; do not sequence conflicts by array order. Reuse a prior satellite version when a valid repeated payload makes no change; raw.batch_records and version_lineage still record the new appearance. Conflicting source candidates can have different version_ids within the same batch, but no candidate is authoritative without a quality decision.

Assignment dates remain unknown without producer evidence. wear_site belongs to the assignment, not exclusively to the device. Firmware recovered from device_label belongs in business rules with provenance, not a silently modified raw satellite. Initial coverage_start/end may come from the configured study window with coverage_basis=study_baseline; those dates are not invented enrollment dates.

## Business Rules and Current State

| Relation | Materialization / Key | Columns and Behavior |
| --- | --- | --- |
| business.version_decisions | Append-only table; PK decision_id | version_id, source_relation, business_key_hash, study_hk, decision (accepted/rejected/conflict/pending_reference), decided_at TIMESTAMPTZ, rule_version, batch_id, reason_code nullable, normalized_payload_hash nullable; decisions can be superseded, not erased |
| business.quality_issues | Table; PK issue_id | batch_id, record_id nullable, version_id nullable, rule_code, severity, first_seen_at, current_status; stable identity supports replay |
| business.quality_resolutions | Append-only table; PK resolution_id | issue_id, resolved_at, resolution_code, superseding_version_id nullable, publication_id nullable; preserve consolidation-specific resolutions |
| business.affected_participant_dates | Work queue; PK (study_hk, participant_hk, local_date, change_id) | reason_code, created_at, consumed_checkpoint_id nullable; old and new dates for key/date changes |
| business.current_participants | View; participant_hk | Latest authoritative accepted profile, master authorization, and lineage |
| business.current_devices | View; device_hk | Latest accepted metadata, resolved_firmware_version, firmware_provenance |
| business.current_assignments | View; participant_device_hk | Latest accepted relation state; explicit unknown effective intervals |
| business.current_enrollments | View; participant_study_hk | Accepted enrollment context and expected coverage window |
| business.current_steps / current_heart_rate | Views; unique (study_hk, participant_hk, event_at_utc) per source | Accepted observation version, normalized value, local_date, warnings and rule version |
| business.current_sleep / current_wellness | Views; sleep_session_hk / wellness_response_hk | Accepted complete source version and quality state |
| business.current_sleep_stages | View; (session_version_id, stage_position) | Stages from the current accepted session version only; no mixing old and new arrays |

Views resolve the highest accepted delivery sequence per key within its source, excluding later rejected/conflicting corrections. An accepted delete is a tombstone: it removes the current row and does not resurrect an older upsert. Reaccepting a quarantined historical record cannot overwrite a newer accepted version. Selection and rule versions are explicit; no cross-source precedence is invented. Primary reconciliation categories are exclusive, while warning flags may overlap.

## Kimball Marts

Use physical fact tables updated through transactional delete/insert of accepted keys or affected groups. Do not delete facts for rejected corrections. Store lineage version identifiers; immutable historical reproduction uses the vault and frozen publication inputs.

| Table / View | Grain / Key | Principal Columns |
| --- | --- | --- |
| marts.dim_participant | One accepted profile knowledge version; PK participant_sk | participant_hk, participant_id, business_namespace, height_cm, gender, chronotype, profile_version_id, known_from_at/known_to_at TIMESTAMPTZ, is_current BOOLEAN |
| marts.dim_device | One accepted device knowledge version; PK device_sk | device_hk, device_id, firmware_version, firmware_provenance, calibration_date, device_version_id, known_from_at/known_to_at, is_current |
| marts.dim_study | One study; PK study_sk | study_hk, study_code, study_timezone, study_start, study_end_exclusive |
| marts.dim_date | One calendar date; PK date_sk = YYYYMMDD INTEGER | calendar_date, iso_week_start DATE, iso_week_number SMALLINT, iso_week_year SMALLINT, day_of_week SMALLINT; date is interpreted with a study context |
| marts.fact_steps_minute | unique (study_hk, participant_hk, event_at_utc) | study_sk, participant_sk, device_sk nullable, date_sk, event_at_utc, steps_per_minute DECIMAL, source_version_id, quality_status, rule_version |
| marts.fact_heart_rate_minute | Same grain, separate source | Same dimensional keys, heart_rate_bpm, source_version_id, quality_status, rule_version |
| marts.fact_sleep_session | One current accepted sleep_session_hk | study_sk, participant_sk, date_sk (wake date), sleep_onset_utc, sleep_end_utc, reported_stage_sleep_min, reported_stage_deep_min, deep_sleep_pct_from_reported_stages, efficiency_pct_reported, restlessness, session_version_id, quality_status |
| marts.fact_sleep_stage | PK (session_version_id, stage_position) for current sessions | sleep_session_hk, stage_type, start_at_utc, end_at_utc, duration_min_reported, is_post_boundary_awake BOOLEAN, warning_status |
| marts.fact_wellness_daily | One current wellness_response_hk | study_sk, participant_sk, date_sk, four survey scores, response_version_id, quality_status |
| marts.gold_participant_devices | Current table; one row per authorized participant_hk | participant_id, business_namespace, device_id nullable, wear_site nullable, resolved firmware, calibration context and assignment_quality_status |

Use deterministic surrogate keys from accepted entity/version identity rather than a sequence that changes on full rebuild. known_from/to are system knowledge intervals, not invented source-effective intervals. Fact dimension assignment follows a documented knowledge-as-of-load policy until source effective dates are available; do not claim historically accurate device attribution. If device attribution is ambiguous or lacks evidence, device_sk remains null. gold_participant_devices returns one participant row, flags ambiguous assignments, and does not choose an arbitrary device.

Age and max_heart_rate are enrollment context, available by study through current_enrollments, not global participant dimension attributes. For sleep, the source supplies efficiency_pct but no session total_sleep_min field. reported_stage_sleep_min therefore explicitly sums reported non-awake stage durations; deep_sleep_pct divides reported deep-stage minutes by that total when positive. Preserve contradictory values and warnings; do not silently substitute timestamp-derived durations under the accepted correction policy.

## Daily Analytics and Versioned Biostatistics

| Relation | Grain / Key | Columns |
| --- | --- | --- |
| analytics.participant_day | PK (study_hk, participant_hk, local_date) | expected_minutes, accepted_steps_minutes, accepted_heart_rate_minutes, observed_steps_total DECIMAL, heart_rate_sum BIGINT, heart_rate_mean DECIMAL nullable, sleep/session measures, four survey scores nullable, source-presence flags, warning counts, rules/input lineage, calculated_at TIMESTAMPTZ |
| biostatistics.publications | Published metadata view; unique publication_id | Internal publication_build.publications table stores candidates; only published status is exposed |
| biostatistics.participant_snapshot | Published view; unique (publication_id, participant_hk) | Frozen chronotype, profile, enrollment, and assignment context with version references |
| biostatistics.participant_day | PK (publication_id, participant_hk, local_date) | Frozen daily metrics and coverage under that publication's consolidation rules |
| biostatistics.participant_week | PK (publication_id, participant_hk) | study_hk, week_start, expected_days, accepted days/counts by measure, observed_steps_total, heart_rate_sum/count/mean, sleep and survey sums/counts/means, warning counts, partial_week BOOLEAN |
| biostatistics.sleep_stages | PK (publication_id, session_version_id, stage_position) | Consolidated retained stages with reported duration; unresolved discordant post-boundary awake stages are absent only here |
| biostatistics.sleep_stage_exclusions | PK (publication_id, session_version_id, stage_position) | reason_code, issue_id, resolution_id; records excluded stages without deleting source history |
| biostatistics.latest_participant_week | View of highest successfully published revision per study/week | Join publication metadata to participant_week; pending/failed revisions never replace a published revision |

Daily analytics contains the expected participant/date spine even when no observation is valid. expected_minutes comes from study time-zone boundaries intersected with the configured coverage window; 1,440 applies to full Bogotá days, not every international study day. Null means no accepted measurement; an observed zero is valid. For complete declared coverage with valid zero counts, observed_steps_total may be zero; with zero accepted step observations it is null.

Weekly steps are sums of observed accepted minute counts, labeled observed rather than full-coverage estimates. Weekly heart-rate mean is sum of accepted bpm values divided by accepted minute count, not an unweighted mean of daily means. Sleep and survey means use non-null eligible session/response counts. Keep separate denominator columns; a missing survey is not zero. Enrollment/profile context is frozen alongside observations for reproducibility. First and last study weeks remain partial, and no coverage is expected outside the study window.

Reported sleep values survive provisional and consolidated outputs. Internal awakenings remain represented. At consolidation, unresolved post-boundary awake exclusions have an explicit exclusion record and accepted_with_warning resolution. Other sleep contradictions retain independent alerts. Publications must materialize frozen input selections, not read mutable current facts after the cutoff.

## Loading Order and Implementation Boundary

1. Register delivery/file identities and immutable raw occurrences.
2. Build typed staging and parse/quality diagnostics.
3. Load configured study, authoritative participants, devices, enrollment and assignment hubs/links.
4. Load source satellite versions and lineage, then quality decisions and reference recovery.
5. Resolve current views, update dimensions and facts, and consume affected-date work through checkpoints.
6. Recalculate and validate daily analytics before writing readiness.
7. Freeze weekly input versions and cutoff, build revision-specific biostatistics, and publish only after validation.

The catalog distinguishes internal base tables from public views. Daily candidates are stored in marts.participant_day_current and exposed only through the matching business.daily_publications gate. Weekly candidates and frozen outputs are stored in publication_build; biostatistics contains published-only views. Public views have logical unique grains, not physical primary keys. The generated DDL is authoritative for exact implemented column names and nullability; staging UTC fields and parse diagnostics remain transformation requirements. Before loading models, configure study_code/business_namespace and implement database/dbt checks. Cohort-wide inferential statistics and clinical cutoffs are not invented here. No sensor/DAG success currently implies that analytical relations have been loaded.

References: [Data Vault decision](decisions/003_data_vault_model.md), [quality policy](data_quality_policy.md), [time policy](time_policy.md), [publication schedule](publication_schedule.md).
