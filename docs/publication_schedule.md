# Analytics and Biostatistics Publication Schedule

Status: accepted scheduling policy; Airflow implementation pending.

## Period and Time Zone

Use America/Bogota (UTC-05:00) for scheduling and study calendar boundaries. A weekly event period starts Monday at 00:00 inclusive and ends the following Monday at 00:00 exclusive, covering all of Sunday without truncating fractional seconds. Store UTC timestamps alongside local period identifiers.

UTC is the reference for stored event instants, load timestamps, cutoffs, and completion timestamps. Calendar-only dates retain study meaning, and original timestamps/offsets remain in lineage. STUDY_TIMEZONE is configurable for future studies; it does not make Bogotá the default interpretation for all international sources. See the [time policy](time_policy.md).

Analytics runs daily and processes the preceding local calendar day plus accepted corrections affecting earlier dates. Its exact daily execution time remains to be configured. On Monday, the Sunday ingestion and analytics validation must complete before weekly consolidation.

## Monday Consolidation

The tentative weekly trigger is Monday at 03:00 local time (08:00 UTC). This is an attempt to start the workflow, not a guaranteed publication time or a fixed correction cutoff. Consolidation depends on the required source files being available in the source repository/location and successfully ingested and validated.

Interpret source repository/location as the configured data delivery location, currently the source S3 files, rather than the project's Git repository. Raw data remains excluded from Git. A different delivery location can be configured without changing the scheduling policy.

The workflow must:

1. Check availability of the required Sunday delivery using its expected batch manifest or equivalent configured delivery inventory.
2. Load and validate Sunday data and eligible corrections, then complete daily analytics.
3. Register the weekly publication, freeze its accepted input manifest, and set correction_cutoff_at to that registration time.
4. Consolidate the preceding week's biostatistics and record the publication revision and actual completion time.

Existing baseline files alone do not prove that a new Sunday delta arrived. Availability checks must identify the expected delivery and completeness of its declared files, not merely any matching file. A declared empty delta can be a valid delivery; participant-level missing measurements are a coverage issue rather than proof that a file has not arrived.

## Missing Files and Failures

If required files are unavailable at 03:00, keep the publication pending and defer or reschedule the availability check. Do not consolidate using stale files, treat absence as an empty delta, or advance the correction cutoff. The polling interval and timeout/notification threshold remain configurable implementation decisions; no producer delivery SLA is assumed.

Once files arrive, resume the same scheduled weekly scope. Ingestion or critical validation failures block consolidation. After a publication manifest and cutoff are registered, retries retain both. Later corrections create a new revision instead of changing the frozen publication.

Record scheduled_run_at, event_period_start, event_period_end, consolidation_date, correction_cutoff_at, and published_at separately. The consolidation date identifies the actual execution that registers the publication: normally Monday, but the actual local date if source delays push it to another day. The event period remains the preceding scheduled week regardless of delay or manual replay.

## Coverage and Initial Study Boundaries

Consolidation does not require complete measurements for every participant. Publish coverage, unresolved warnings, and partial-period indicators alongside metrics, subject to critical validation checks.

The initial study runs January 8 through February 4, 2026, so its first and last calendar weeks are partial. Retain Monday-based week identifiers while limiting expected study coverage to January 8-11 and February 2-4 for those weeks. Do not assume events outside the study window are missing required observations. Delivery inventory must explicitly allow no required files outside that window; schedule final-week consolidation on the following Monday under the same policy.

References: [Correction and consolidation policy](data_quality_policy.md), [Delta delivery contract](decisions/002_delta_contract.md).
