# Daily analytics and readiness

Status: implemented locally in DuckDB and the daily Airflow DAG. [Weekly frozen biostatistics](weekly_biostatistics.md) consumes its validated readiness receipts.

## Calculation and coverage

`dbt_daily/models/daily_candidates.sql` prepares one row per active participant and tracked study date from the accepted dimensional facts. `business.participant_date_state` supplies the date spine, including configured study days with no observations. Minute observations use the study's local calendar; sleep uses the wake date and wellness uses the survey date. Observed dates outside configured coverage remain traceable in the internal daily table but are excluded from publication.

Expected minutes are the elapsed UTC minutes between consecutive local midnights within the configured study window. Bogota days have 1,440 minutes. A zone with daylight saving time can have 1,380 or 1,500; the calculation does not assume a fixed day length. Outside the study window the expectation is zero. Participant-specific enrollment dates are not invented.

Steps totals sum accepted minute values. Heart rate stores the accepted minute count, sum, and mean, retaining the components needed for a weighted weekly mean. A day without accepted steps has a NULL total and `has_steps=false`; a measured zero remains zero with `has_steps=true`. Missing heart rate, sleep, and wellness values remain NULL. Partial coverage is visible through expected and accepted minutes and presence flags; it does not imply zero activity.

Sleep copies the reported durations, stage-derived deep sleep percentage, reported efficiency, and restlessness from the current session fact. All provisional sleep stages remain unchanged. P001's January 21 reported 327 minutes and 100% efficiency remain available with warnings. [Frozen weekly publication](weekly_biostatistics.md) implements the correction cutoff and documented terminal awake exclusion.

`warning_count` counts warning flags on accepted current measurement records, including each flagged heart rate minute. It is not a count of distinct rules, rejected records, or unresolved historical incidents. A nonzero count produces `accepted_with_warning`. Quarantined records remain in quality issue history and do not contribute values. Coverage gaps remain explicit through counts and presence flags without inventing clinical thresholds.

## Publication gate

`src/health_pipeline/daily_loading.py` requires validated quality inputs, matching current impact and mart checkpoints, and no unconsumed dimensional work. It validates candidate grain, spine count, minute totals and counts against facts, observation presence flags, heart rate bounds, and coverage limits before changing consumer data.

A single DuckDB transaction replaces the compact `marts.participant_day_current` result, updates `business.daily_publications`, records `business.daily_evaluations`, and appends durable `business.daily_readiness_outbox` receipts. Failure rolls back the whole publication and preserves the previous analytics result. `analytics.participant_day` only exposes rows matching a successfully published input hash, rule version, and code version.

Publication requires a registered, valid six-source delta manifest explicitly declaring the target date. Declared files must have raw load receipts, delivery lineage, and `raw_loaded` or `archived` status. Explicit empty deltas are valid. Missing source declarations or unloaded files cannot establish readiness. The existing bootstrap manifest explicitly declares all 28 study dates; its historical readiness does not establish availability of a future Sunday delivery.

The daily input fingerprint identifies the validated raw state, dimensional evaluation, and admitted delivery manifests. Corrections refresh the daily result and readiness revisions. A readiness receipt includes the mart checkpoint, manifest identities, validation result, expected and accepted coverage, missing-source participant counts, warning flag counts, and an explicit empty-cohort indicator.

SQLite receipts are written only after the DuckDB transaction commits. The DuckDB outbox is durable: if SQLite is unavailable, the task fails and a retry completes the receipt without rebuilding the committed aggregate. Repeated retries do not create duplicate revisions. Receipts for superseded fingerprints are not replayed as current. There is no distributed transaction between DuckDB and SQLite.

The weekly consumer requires the latest matching receipt for every required study day, compares its dependencies with current validated inputs, and accounts for explicit out-of-study exclusions. It rejects empty cohorts and stale receipts. Its trigger must name the expected final delivery; an arbitrary historical receipt or Sunday's task success alone is insufficient.

## Execution and scale

The DAG adds `publish_daily_analytics` after `load_dimensional_marts`, under the shared `duckdb_access` pool. Scheduling remains manual until the daily execution time and operational availability timeout are agreed. Daily dbt uses a separate project and writable runtime directory, leaving quality and minute-mart implementation hashes unchanged when only aggregate logic changes.

The first implementation recomputes compact daily aggregates by scanning accepted minute facts and atomically replacing the daily result. It does not rewrite minute facts or Vault history. This favors simple, reliable reconciliation at the exercise's current scale. At larger scale, use the affected-date ledger to capture complete study/date groups since the previous daily checkpoint and apply delete/insert only to those groups, retaining date-wide publication consistency.

Run in Docker:

```bash
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m health_pipeline.daily_loading
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_daily.py --baseline
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -p test_daily_loading.py -v
```

Isolated tests cover observed zero versus missing data, heart rate weighting, declared-date publication, unloaded deliveries, missing mart checkpoints, deletion corrections, preservation of a prior publication on failure, recovery after commit, duplicate-free retries, receipt reactivation, and daylight saving day lengths. Baseline verification independently reconciles fact counts and sums, readiness fingerprints, 1,400 daily rows, and provisional P001 sleep values.
