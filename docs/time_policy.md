# Time Storage and Study Calendar Policy

Status: accepted. UTC is the common timestamp reference; America/Bogota defines this study's calendar and intended schedules.

- Persist event instants, ingestion timestamps, registration timestamps, publication cutoffs, and completion timestamps in UTC. Control records serialize them as ISO 8601 strings ending in Z.
- Preserve original source timestamps and explicit offsets in raw data and lineage. Interpret naive timestamps only through the source/study contract; the current dictionary specifies UTC-05:00.
- Keep calendar-only values, such as survey dates and sleep wake-up dates, as study dates. Do not invent midnight UTC instants for them.
- Compute event-day and event-week boundaries in the configured study time zone, then convert their instants to UTC. Use half-open intervals: start inclusive, end exclusive.
- Monday 03:00 in America/Bogota corresponds to 08:00 UTC. The weekly business period still ends at Monday 00:00 local time, or 05:00 UTC, rather than at the trigger time.
- Never use task retry wall-clock time to select a different business day or week. A manual run supplies target_date or week_start explicitly; scheduled runs will use their stable scheduled trigger scope.

STUDY_TIMEZONE defaults to America/Bogota in the local configuration and is distinct from Airflow's default UTC timestamp reference. International studies require their own calendar configuration and source interpretation rules. Changing the calendar of an existing study is a versioned contract change, not a silent environment update. Studies in zones with daylight saving time may have event days of 23 or 25 hours; calculate boundaries independently rather than assume 1,440 minutes for every study day.

The current Bogotá-based baseline has 24-hour days. Its sleep/coverage conventions and enrollment window do not automatically apply to other studies. Manifest validation rejects a delivery time zone inconsistent with the configured study.
