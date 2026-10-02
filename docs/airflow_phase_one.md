# Airflow Phase One: Delivery Controls and Calendar Scope

Status: executable first phase. Both DAGs are manual and initially paused. No raw ingestion, Data Vault transformations, daily analytical readiness, or biostatistics publication is claimed.

## Implemented Boundaries

wearable_daily_pipeline resolves an explicit target_date, waits for a local delivery manifest and all declared files, verifies checksums, and registers the immutable manifest identity and stable sequence. Completion means delivery_registered, not ingestion_complete or analytics_ready. Verified source files are not yet copied to raw storage; keep them available through retries in this phase.

wearable_weekly_biostatistics resolves an explicit Monday week_start and returns the business scope with UTC boundaries. Completion means calendar inspection only. Readiness waiting, historical input freezing, consolidation, and publication will be added after their real processing interfaces exist. DAG descriptions and tags expose these limited behaviors.

The daily delivery adapter is local-filesystem based in this phase. Place manifests and declared files under /opt/airflow/project-data/incoming, with object_key paths relative to that root. The public S3 source does not supply live daily manifests; an S3 adapter and baseline manifest generator remain future work.

## Storage and Time

The SQLite control registry at /opt/airflow/project-data/control/deliveries.sqlite stores canonical manifests, content hashes, stable per-source sequence numbers, and UTC registered_at. It is a separate business control registry, not Airflow metadata or the analytical warehouse. BEGIN IMMEDIATE protects registration; the register task also uses the shared access pool. A replay reuses its prior sequence and registration time. A changed manifest under the same delivery_id fails.

The calendar helpers compute local boundaries using STUDY_TIMEZONE and serialize UTC instants. Manual calendar dates remain unchanged. See the [time policy](time_policy.md).

## Manual Empty-Delta Demonstration

After updating the repository, refresh mounts with:

```bash
docker compose up -d --wait airflow-api-server airflow-scheduler airflow-dag-processor
docker compose exec airflow-scheduler mkdir -p /opt/airflow/project-data/incoming
docker compose exec airflow-scheduler cp /opt/airflow/project/examples/manifests/empty_daily.json /opt/airflow/project-data/incoming/example-empty.json
```

In the Airflow trigger form, supply the following daily parameters:

```json
{
  "target_date": "2026-01-11",
  "manifest_key": "example-empty.json",
  "sensor_timeout_seconds": 60
}
```

The 60-second timeout is a manual demonstration setting, not the production delivery timeout. Repeating the same delivery must retain the same registry row and sequence. The fixture declares no changes; it does not provide measurements or establish clinical completeness.

For the weekly scope demonstration, trigger with week_start=2026-01-05. The returned UTC interval is [2026-01-05T05:00:00Z, 2026-01-12T05:00:00Z). This run does not consolidate data.

## Verification and Remaining Work

Run calendar, manifest, checksum, and replay checks inside the configured container:

```bash
docker compose exec airflow-scheduler python -m unittest discover -s /opt/airflow/project/tests -v
docker compose exec airflow-scheduler airflow dags list-import-errors --output json
```

To execute the empty-delta and weekly-scope checks through the API:

```bash
docker compose exec airflow-scheduler python /opt/airflow/project/scripts/verify_airflow_phase_one.py
```

This creates synthetic manual run records and restores each DAG's prior paused state. It creates no analytical readiness or publications. On October 2, 2026, both manual runs completed successfully, Airflow imported both DAGs without errors, and the control acceptance checks passed, including UTC boundaries, future DST behavior, changed checksums, and immutable delivery replay.

Add real immutable raw copying and processing commands before readiness emission; add persistent daily readiness and frozen historical publication inputs before weekly scheduling. Keep schedule=None in this phase. The accepted Monday 03:00 trigger will be enabled only when those dependencies exist. Daily trigger time and operational sensor timeout still require confirmation.
