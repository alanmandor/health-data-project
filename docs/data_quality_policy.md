# Data Quality and Correction Policy

Status: accepted rules implemented in [local staging and quality](dbt_staging_quality.md), [persistent issue history](quality_issue_lifecycle.md), and [weekly biostatistics consolidation](weekly_biostatistics.md). Assumptions are explicit and raw data remains unchanged.

## Duplicates, Time, and References

Collapse identical normalized observations in validated output while retaining original occurrences and lineage. Different values for the same key within a batch quarantine the group; do not select MAX, AVG, or the last array entry. Keep any previously accepted current value with a pending-conflict flag. A valid correction in a later sequenced batch creates a historical version and replaces current state.

Normalize explicit time zones to UTC; interpret naive timestamps using the study's fixed UTC-05:00 offset. Preserve original strings and local study dates. Minute records with nonzero seconds or microseconds are quarantined without rounding. Missing or rejected minutes remain missing, not zero. Unknown participants remain recoverable reference quarantine, with counts visible in outputs.

## Steps and Heart Rate

For this dataset, assume _unit=steps_per_hour denotes an hourly-equivalent rate for the represented minute. Divide only explicitly marked values by 60; retain original value, original unit, normalized value, and rule version. All 120,963 marked observations inspected across P005, P033, and P038 are divisible by 60, supporting this assumption without proving producer semantics.

Use the dictionary's minute-count unit when the unit is absent; unknown units are quarantined. The three unmarked P005 observations are also off-grid and are excluded under the timestamp rule without guessing their units. Reject negative steps. Values above 30 normalized steps/minute are review candidates, not automatic failures; unusually large values receive a visible anomaly flag, with no invented hard physiological threshold.

Quarantine heart-rate values outside the dictionary's 40-200 bpm range. Repeated constant values receive a suspected-flatline flag, not automatic rejection: the exploratory 60-minute threshold is not proof of failure. Distinguish acceptance checks from exploratory warnings.

## Device Metadata

Recover missing firmware_version from a parseable device_label, preserving the original blank and derived-value provenance. Populated version/label disagreements remain conflicts rather than silent replacements.

## Sleep During the Correction Period

An awake stage can occur between sleep intervals and must not be rejected solely because it is awake. Preserve reported sleep times, duration fields, and efficiency_pct. Alert on stage duration/timestamp contradictions, gaps, overlaps, and disagreement between session boundaries and stage intervals. Preserve reported efficiency without silently replacing it with a derived measure.

Records remain provisional until the relevant biostatistics publication cutoff. A later valid full-session delta replaces current session state and its full stage set, preserves vault history, and reruns checks. An unresolved alert alone does not block the agreed reported-value output; structural failures such as unreadable timestamps or invalid identity can still prevent publication.

Do not treat the awake stage after sleep_end as part of the session simply to derive an alternative efficiency denominator. Retain it provisionally with a boundary-discordance alert. For example, P001 on January 21 has 327 minutes of sleep followed by 55 awake minutes after sleep_end; retain its reported 100% efficiency during the correction period.

## Consolidation and Revisions

Each biostatistics publication records publication_id, revision, scope, correction_cutoff_at, the exact accepted input versions, and rule/code versions. The cutoff is fixed when the publication is registered and remains unchanged through retries; corrections accepted after it are handled in a later revision. Persist a publication input manifest so processing order cannot alter the selected versions.

At consolidation, sessions with unresolved semantic alerts retain reported sleep values and receive accepted_with_warning status. For an awake stage after sleep_end that remains discordant, exclude it only from the consolidated stage representation and close the alert with resolution=excluded_at_consolidation linked to the publication. Raw, vault history, and provisional source representations retain the stage.

This exclusion never applies indiscriminately to an internal awakening followed by more sleep. Other contradictions, such as deep-stage duration disagreement, are not resolved by removing an awake stage: retain their reported values and warning/resolution history separately. Do not claim that expiration of the correction period proves clinical correctness.

Late corrections create a new publication revision and recalculate affected daily and weekly results; prior publications remain available. The correction period ends at each publication cutoff rather than after an invented fixed number of days. The [publication schedule](publication_schedule.md) defines Monday-based weeks and a tentative Monday 03:00 trigger after Sunday delivery becomes available and passes ingestion and validation. The actual cutoff is registered after those prerequisites, not automatically at 03:00.

## Reconciliation

Report accepted, rejected, pending-reference, and conflicting records and identical duplicate occurrences using mutually exclusive primary reconciliation categories. Secondary warning flags can overlap and must not be added to primary counts. Publish coverage denominators and accepted_with_warning counts. Verify deterministic results on replay, fixed-cutoff retry behavior, full-session correction, internal awakenings, post-boundary awake exclusion, and post-consolidation revisions.
