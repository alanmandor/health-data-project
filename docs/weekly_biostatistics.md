# Weekly Frozen Biostatistics

Status: implemented locally in DuckDB, dbt, SQLite controls, and the manual weekly Airflow DAG. Snowflake has matching DDL and consumer permissions; cloud execution is not claimed.

## Readiness and Event Scope

`wearable_weekly_biostatistics` resolves a Monday-based week and waits in reschedule mode for validated daily receipts. The sensor reads SQLite without opening DuckDB. Required days are the intersection of the week and the registered study window. The first study week requires January 8–11; the final week requires February 2–4. Configured out-of-study days are explicit exclusions from delivery requirements, not missing observations.

Each trigger names `expected_source_system` and `expected_delivery_id`. The final required day's receipt must include that exact delivery and declare that date. This is Sunday for a full study week and Wednesday for the final partial week. An operator must identify the expected producer delivery; an arbitrary old bootstrap cannot satisfy an explicitly named future delta. The exercise baseline is consolidated only by explicitly naming its bootstrap delivery, which declares all 28 study dates.

Readiness permits incomplete measurement coverage, but an empty authorized cohort blocks weekly publication. The freeze checks current raw/quality, dimensional, and daily implementation checkpoints; every required date must have matching DuckDB publication and latest SQLite receipt fingerprints. A new unvalidated raw load or stale daily result blocks a new freeze. The warehouse pool serializes freeze and daily writers. Standalone maintenance commands must follow the same serialization discipline.

## Freeze and Retry Identity

`freeze_week` runs in one DuckDB transaction and records the publication metadata, cutoff, study/date scope, cohort snapshot, frozen participant days, selected immutable source versions, complete provisional stages, exclusion candidates, and readiness receipts. The correction cutoff is the actual registration instant. Consolidation date is its actual study-local date; scheduled time, event period, and publication completion time remain separate fields.

The cohort snapshot freezes chronotype, gender, height, enrollment age and maximum heart rate, profile/enrollment versions, and device/assignment context. Assignment selection matches the device and wear site used by Gold, then the latest accepted knowledge version. It does not invent effective dates. Firmware metadata versions are recorded in the input ledger.

Airflow's run ID binds a stable request to the publication. Clearing or retrying a frozen request reuses its cutoff and selections even after new data arrives. A different scope or expected delivery under the same request fails. A new request with identical validated inputs and implementation reuses the existing publication. Changed inputs or consolidation code create a new revision. Earlier published revisions remain immutable and readable.

Frozen source versions are stored in `publication_build.inputs`; this includes profile, enrollment, assignment, metadata, steps, heart rate, sleep, and wellness. The minute facts themselves are not duplicated: immutable Vault versions and their payload hashes reproduce the accepted input selection. Input key hashes use the selected relation's logical grain. Every selected domain count reconciles with the frozen daily coverage.

For audit outside DuckDB, the registry references deterministic gzip JSONL manifests in the persistent volume at `publications/<publication_id>/<input_kind>.jsonl.gz`. Each row contains version ID, source relation, logical key hash, and payload hash. SQLite `publication_inputs` stores one checksum-addressed manifest receipt per domain instead of millions of minute records. The manifests are derived from the durable frozen input ledger; retries repair missing exports and detect checksum changes.

DuckDB commits before registry/export work. A failed export or SQLite write leaves the freeze intact and the task unsuccessful; retry repairs the receipt. The SQLite request checkpoint lets a retried readiness sensor recognize an already frozen request. There is no distributed transaction. Preserve the DuckDB file, publication manifests, and control registry together when backing up the project.

## Weekly Calculation and Publication

The separate `dbt_weekly` project reads only `publication_build.participant_day` for the supplied publication ID. It does not read mutable current facts or participant dimensions. Weekly steps sum observed values. Heart rate uses the sum of accepted bpm values divided by the accepted minute count. Sleep and survey averages exclude NULL values; survey and percentage denominators are explicit. Missing data is never replaced by zero. The expected day/minute counts and partial-week flag remain visible.

The publish task validates cohort count, daily-to-weekly totals, and heart rate denominators. A single transaction inserts weekly results and retained stages, records exclusion resolutions, and marks the publication published. Public `biostatistics` views filter unpublished work; `latest_participant_week` chooses the highest successfully published revision per study/week. A failed new revision does not replace the previous published revision.

Build retries use the frozen consolidation code version. If code changes after a freeze, an unpublished request requires its original implementation or a new request/revision. Completed publications can be replayed to repair external receipts without rebuilding them.

## Sleep Correction Closure

Reported sleep duration, efficiency, and other values are preserved in both frozen daily and weekly output. A terminal awake interval wholly after the reported sleep end, with no later sleep stage, is excluded only from consolidated stages. Internal awakenings followed by more sleep remain, including sessions whose declared boundaries generate warnings.

Each excluded stage has an explicit exclusion row and an `excluded_at_consolidation` resolution linked to its publication. The broader source boundary alert closes only if the retained stages clear that boundary discrepancy entirely. Other boundary, duration, and timeline discrepancies remain independently visible. Later issue reconciliation does not reopen a publication-resolved immutable issue solely because the unchanged provisional stage remains present. This is a documented acceptance decision, not proof of clinical correctness.

Raw records, Vault history, and current analytics stages are never deleted by consolidation. P001's January 21 session retains 327 reported minutes and 100% efficiency; its discordant 55-minute terminal awake stage is recorded as an exclusion in the frozen stage output. A later valid full-session correction is used in a new publication revision without editing the previous revision.

## Run and Verify

Trigger `wearable_weekly_biostatistics` after the daily pipeline succeeds:

```json
{
  "week_start": "2026-01-19",
  "expected_source_system": "exercise_adapter",
  "expected_delivery_id": "baseline-2026-01-v1",
  "sensor_timeout_seconds": 3600
}
```

The timeout is an example, not a producer SLA. Monday 03:00 Bogota remains the tentative production trigger. Both DAGs remain manually scheduled until a daily execution time, operational timeout, and producer inventory mapping are configured. Delay preserves the requested event week; it does not move the correction cutoff before an actual freeze.

For serialized local verification of all five baseline weeks:

```bash
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/publish_baseline_weeks.py
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_weekly.py --baseline
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -v
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/verify_raw_airflow.py
```

The baseline verifier reconciles five publications, 250 frozen participant snapshots, 1,400 daily rows, 250 participant/week rows, selected/exported versions, retained/excluded stages, and linked resolutions. Isolated tests exercise missing deliveries, study boundaries, rollback, cutoff reuse, late corrections, frozen cohort attributes, weighted heart rate, internal awakenings, and terminal awake exclusions. DAG verification runs daily and weekly workflows twice in isolated storage and checks one published revision with no duplicated source files.

For descriptive chronotype comparisons, run [the example query](../examples/queries/chronotype_comparison.sql). It uses the latest published revision and its frozen cohort, with accepted measurement counts and coverage next to each mean. No clinical thresholds or inferential statistics are invented.
