# Delivery Manifest Contract

Status: implemented local registration and raw ingestion contract. The producer does not publish daily manifests; stage the baseline explicitly with the adapter. See [raw ingestion](raw_ingestion.md) for bootstrap, replay, and archival, [daily readiness](daily_analytics.md), and [frozen weekly publication](weekly_biostatistics.md).

## Separate the Three Manifests

| Record | Owner | Purpose |
| --- | --- | --- |
| Delivery manifest | Producer, or explicit exercise adapter | Declare the complete files and empty source deltas for a delivery |
| Ingestion manifest | Pipeline | Freeze source identity, verified hashes, local immutable paths, and assigned sequence |
| Publication manifest | Consolidation pipeline | Freeze accepted historical versions and correction cutoff for one output revision |

A delivery declaration is not proof of successful ingestion. A successful ingestion is not proof of analytical readiness. A publication uses accepted versions, not merely all downloaded rows.

## Delivery Identity and Scope

Required fields are schema_version, delivery_id, source_system, batch_kind, delivery_mode, target_dates, timezone, completed_at, and sources. delivery_mode is delta. batch_kind is bootstrap or daily. A daily manifest declares exactly one target_date; a bootstrap may declare the full study period. target_dates describes the delivery obligation, not the event-time range of every row: corrections can concern earlier dates.

delivery_id is unique within source_system and immutable once declared complete. completed_at must include a UTC offset and is diagnostic producer metadata, not automatically the correction cutoff or precedence sequence. timezone is America/Bogota for this exercise.

The source registry contains participants, device_metadata, steps, heart_rate, sleep, and wellness_survey. Every delivery explicitly declares each registered source as files or empty_delta. Masters need not be resent daily; empty_delta means no master changes. It does not remove participants or devices. A source cannot be silently omitted. A registry version is recorded in ingestion state so future source additions do not change old delivery requirements.

For a files declaration, require a nonempty files array. Each file entry declares object_key, SHA-256 checksum, byte size, and format; version_id is optional when the source supports versioning. Formats are csv or json for these sources. File paths must be relative safe object keys without parent traversal. The same source/key must not appear twice in one delivery. An empty_delta declaration has an empty files array.

## Completion and Availability

The producer writes all immutable file versions before publishing the manifest atomically as the completion marker. Daily discovery uses the configured target-date manifest locator; bootstrap discovery is explicit/manual. Keep the locator configurable rather than assume a new layout exists in the public source.

For the exercise, an adapter may create the manifest from the downloaded baseline after verifying expected file coverage. Label its provenance exercise_adapter. Do not infer daily deliveries from the presence of the original baseline files. Synthetic correction fixtures use separately identified deliveries and provenance synthetic_fixture.

The availability sensor verifies the manifest identity, requested target date, all registered source declarations, and presence of all declared objects. It checks object sizes when available. Missing objects remain a pending delivery. Malformed manifests fail validation; they do not become empty deliveries. Download verifies each checksum before accepting ingestion, because existence alone cannot establish immutable content.

If a non-versioned object changes between discovery and download, reject the checksum mismatch rather than accepting the new bytes under the old manifest. A producer must retain declared file versions long enough for retries; the ingestion copy becomes the durable replay input once verified.

## Registration and Precedence

Under the current exercise assumption, register a stable pipeline sequence per source_system when first accepting a delivery. Registration is atomic and serialized. The exact manifest content hash is retained. Reuse the existing sequence and verified raw copies on retries. Reusing delivery_id with different manifest content is an error, even if a file's name is unchanged.

An old replay cannot replace a newer accepted version. Persist accepted sequence per business key/discriminator. Unresolved conflicts do not advance accepted state. Multiple deliveries discovered together must be registered in an explicitly deterministic order recorded by the exercise adapter; do not depend on S3 listing order or task scheduling. Producer-controlled authoritative revisions would be preferable for real integration, but are not supplied by the exercise.

## Row Operations

Unmodified exercise records are complete upserts. Future fixture/source adapters may add _op=upsert or _op=delete. Upserts require the complete business payload. Deletes require the full business key and no invented measurement values. Keys follow the declared source grain: participant_id, device_id, participant_id/date, or participant_id/normalized UTC timestamp as applicable. Contradictory operations for one key within a delivery are conflicts, not an ordered instruction stream. A full-session sleep upsert replaces the current session and its complete stage set while retaining historical versions.

Schema and format validation occurs before business-key conflict resolution. Source row positions and original operation fields remain in lineage. Technical manifest hashing includes declared content, while normalized observation hashing excludes execution-specific metadata.

## Downstream Readiness

Only after all declared source deltas pass ingestion and critical analytical checks does the daily DAG publish readiness for the requested date, with warnings and coverage counts. The weekly DAG verifies all required study dates and freezes publication versions. An explicitly empty delivery can become ready; a missing manifest cannot.

The [empty daily example](../examples/manifests/empty_daily.json) is an illustrative declaration, not a live delivery. Runtime manifests and raw inputs remain outside Git. Source credentials and local filesystem paths must not be embedded in committed examples.
