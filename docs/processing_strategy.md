# Processing and Incremental Loading Strategy

Status: implemented local processing direction through daily analytics and frozen weekly publication. DuckDB and dbt-duckdb run transformations, with PostgreSQL for Airflow metadata. The [decision records](README.md) define delta delivery and Data Vault contracts; the [quality policy](data_quality_policy.md) governs corrections and consolidation.

## Processing Order and Responsibilities

1. Inventory all objects and register a batch containing the exact set of versions to process.
2. Copy each version to immutable raw storage, verify its SHA-256 checksum, and retain its path and metadata. Source ingestion does not require a specific order across sources.
3. Parse and validate schemas, types, keys, and dates in staging while preserving original records. Isolate record-level errors when the file can be parsed; mark unreadable files as failed.
4. Integrate participants.csv as the authoritative participant master.
5. Integrate device_metadata.csv and participant-device assignments, checking references. Assignment changes require effective dates before historical events can be attributed to devices.
6. Integrate steps, heart_rate, sleep, and wellness_survey. Preparation can run in parallel after master data is available; independent processes writing the shared DuckDB database must serialize through one pool slot. Split sleep data into sessions and stages.
7. Retry records with unresolved references, apply quality rules, and build the selected Data Vault historical integration layer.
8. Build dimensions and facts; recalculate affected participant-date combinations and their weeks; run checks and publish a consistent version.

Airflow coordinates dependencies and retries; Python discovers, copies, and loads data; dbt transforms and validates it. Ingestion success and publication success are separate states. A failed publication resumes from persisted data.

## File-Based Change Capture

The data does not include a transaction log or a reliable updated_at field. The proposed approach detects file versions and compares records rather than assuming database CDC is available.

- Manifest: source, object_key, version_id when available, checksum, bytes, discovered_at, batch_id, status, attempts, and counts. LastModified helps identify candidates; ETag is not treated as a universal content hash.
- Content identity within a source and path: (source, object_key, SHA-256). A version whose content has already been ingested reuses that ingestion. Record each occurrence/version_id separately for auditing. Identical content at another path may enter raw storage, but must not duplicate final entities.
- Raw records: retain a reference to the content/version and the record position; preserve all occurrences and conflicts. Do not deduplicate raw data by business key.
- Normalized records: calculate a business key and payload_hash using a defined canonical representation, types, time zone, and null handling. Also retain the original hash and rule version to support reprocessing.
- Same key and same content: no change to business state. New key: insert. Different content: apply a correction only when an authoritative precedence rule exists; otherwise, quarantine the conflict.
- A later file version takes precedence only if the contract states that it replaces the previous version. Arrival order does not establish which value is correct.
- A missing row does not imply deletion. Apply deletions only through explicit tombstones or a complete, validated snapshot with a declared scope. Without that contract, retain records and flag differences.

The project adopts full-record deltas, including a bootstrap delta for the initial dataset. A later accepted sequenced batch supersedes prior values under an explicit exercise assumption. Missing rows do not imply deletion; deletions require explicit operations. See ADR 002 for stable sequence assignment and old-batch replay protection. Validate these assumptions with a real producer before extending the integration.

## Keys and Conflicts

| Entity | Proposed Key Under the Current Contract |
| --- | --- |
| Participant | participant_id within a source/study namespace |
| Device | device_id within the source namespace |
| Steps / heart rate | participant_id + UTC timestamp, in separate tables |
| Survey | participant_id + date |
| Sleep session | participant_id + wake-up date, following the current grain |
| Sleep stage | session key + position in its versioned array |
| Daily summary | participant_id + local study date |

Do not round timestamps before resolving anomalies. Keys require non-null values and verified uniqueness. Collapse identical duplicates in validated output while preserving lineage; isolate duplicates with different values without arbitrarily selecting MAX, AVG, or the last row. If a previously published value exists, retain it and flag the unresolved conflict; otherwise, do not publish that key.

If multiple sensors, surveys, or sessions per day become valid, extend the contract with event_id/session_id and device_id as appropriate. Do not infer a historical device from the current assignment alone.

## Unknown References and Growth

An event does not establish a valid participant. Preserve it in raw storage and quarantine with reason=unknown_participant, its key, source version, first_seen_at, and resolution status. Exclude it from validated clinical metrics. Reevaluate it after participant master updates; if the reference becomes available and all other checks pass, integrate it once and recalculate its original dates and weeks.

The current profile contains 14 surveys for P999 without a corresponding master record; the same issue has not been documented in wearable events. Apply the same policy to both. Publish pending-record counts so exclusions remain visible.

The 50 participants are an expectation for the initial batch, not a schema limit. Validate that baseline while accepting new identifiers from the authoritative master in future batches. A subsequent absence does not automatically deactivate a participant. Separate participant identity from study membership, including enrollment dates and status when supplied by the source. Calculate expected coverage from membership and the applicable period rather than a constant of 50.

In the selected Data Vault design, a hub may retain a business key observed in an event with its source lineage; this does not establish enrollment. Eligibility for analysis still depends on the master. Inferred members are not required in the initial marts.

## dbt Incremental Models and Change Propagation

Start with incremental facts because of their volume: approximately four million minute-level rows across the two sources. Small current master projections can initially be rebuilt in full; retain their vault history. Use transactional delete/insert for accepted current-state keys and aggregates, with explicit deletion handling. Verify adapter behavior before implementation. unique_key does not replace deduplication or uniqueness tests.

Select records from pending batches through a persisted change relation rather than timestamp > MAX(timestamp) in the target. Event time describes when the event occurred; ingested_at describes when it arrived. Newly received historical data must be processed.

Maintain affected_participant_dates for inserts, corrections, authorized deletions, quarantine resolutions, and dimension/rule changes. A correction that changes the date affects both the old and new dates. Mark incremental work complete after the model commit; retries must be able to apply the same set without duplication. Do not change business timestamps merely because a retry occurs.

Recalculate each affected day from all its current facts, not just the delta. Recalculate the full week from its current daily data. Also remove obsolete summaries if an authorized change leaves a group without facts. A recent lookback window can optimize checks, but must not limit late-data recovery. Week boundaries and inclusion rules still require analytical definition.

Data Vault hubs and links insert missing keys; satellites preserve source versions and identify accepted changes without treating unresolved conflicts as authoritative history. A transition A -> B -> A is a real change: compare against the current state, not every historical hash. Separate effective time, load time, and source version.

For larger scale, use deliveries partitioned by date and source, manifests, streaming reads, and fact partitioning when supported by the database. Avoid rereading full historical arrays every day. Infrastructure choices depend on volume, latency, and concurrency; participant growth alone does not require an architecture change.

## Guarantees and Acceptance Checks

- Same batch processed twice: identical current facts, aggregates, and business history; execution logs may differ.
- Failure after writing but before checkpointing: retry without duplicates, using transactional operations and concurrency control by scope/model.
- Modified file: apply only accepted changes, retain raw versions, and recalculate affected groups.
- Late historical event: update its day and week even outside a recent lookback window.
- New participant: ingest through the master without code changes or a limit of 50.
- Late reference: move from quarantine to integrated once, with traceability.
- Conflicting duplicate: remain visible without silently changing the validated value.
- Explicit deletion: stop contributing to metrics while preserving the audit trail.

Publish batch reconciliation metrics for original, accepted, identical duplicate, conflicting, invalid, and pending records; define mutually exclusive categories so counts reconcile. Verify uniqueness, references, and coverage, and confirm that a second run leaves results unchanged. A batch may complete with visible quarantined records; structural or critical failures prevent publication. Record rule and code versions to explain differences between reprocessing runs.

## Decisions Required Before Implementation

The database, adapter direction, delta assumption, correction precedence, Data Vault direction, quality policies, and [weekly publication schedule](publication_schedule.md) are selected. Before implementation, pin compatible versions, specify physical hashes/models, configure the daily execution time and availability polling, and define critical publication checks. Future enrollment/assignment effective dates still require source data. No pipeline or CDC implementation is claimed.

Technical reference: [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models).
