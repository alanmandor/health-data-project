# Warehouse Bootstrap and Raw Ingestion

Status: implemented for local CSV and JSON-array deliveries. Docker initialization creates DuckDB tables and extends SQLite controls. The daily DAG captures, registers, loads, and archives explicit deliveries, then runs quality, Vault, marts, and [daily analytics/readiness](daily_analytics.md). It remains manual.

## Task Order

1. Resolve the explicit target date and study calendar; wait for a complete manifest.
2. Initialize warehouse and controls idempotently using duckdb_access. Initial Compose bootstrap runs before services start.
3. Bind the manifest to the Airflow run, capture immutable copies, verify sizes/checksums, and register delivery identity/sequence. Retries use the fixed manifest even if the incoming locator changes.
4. Load all declared files in one DuckDB transaction, including raw records, batch lineage, and file-load markers.
5. Confirm raw_loaded in SQLite after commit. A failure here is repaired from DuckDB markers without duplicate insertion.
6. Move and rename incoming files to archive; commit archival receipts and archived status in SQLite. Source manifests remain available and are not moved.

Database tasks and lifecycle mutations use the single-slot duckdb_access pool; max_active_runs is one. Producers must follow the publication protocol because pools cannot lock external writers.

## Paths and Identities

| Location | Purpose |
| --- | --- |
| incoming/<original key> | Producer input; names can be reused after archival |
| raw/deliveries/<batch_id>/manifest.json | Canonical immutable delivery declaration |
| raw/deliveries/<batch_id>/files/<original key> | Verified source bytes, never renamed during processing |
| raw/run_bindings/<run hash> | Fixed manifest for one Airflow run |
| raw/manifest_keys/<locator hash> | Replay pointer when the incoming manifest is unavailable |
| archive/<batch_id>/<original parent>/<stem>__<sha256><extension> | Renamed processed input with original directory context |

batch_id hashes source_system/delivery_id. file_id hashes source_system/source name/format/checksum. record_id hashes file_id and a one-based record position. Whitespace changes create different raw content identities; normalized business duplicate handling belongs downstream.

raw.source_records preserves parsed payloads without business rejection. Unknown participants and invalid measurement ranges remain raw for subsequent quality decisions. raw.batch_records links reused content to deliveries. Within one delivery, identical same-source files coalesce at the batch/record grain; delivery_files preserves all declared aliases. raw.file_loads confirms parsing, including zero-row files. SQLite archive_receipts stores original keys, final paths, and timestamps.

CSV requires unique nonempty headers and matching row widths. JSON requires an array of record objects. Structurally invalid files abort the entire raw transaction; captures and inputs remain and nothing is archived. CSV values remain strings and decoded record text is retained; JSON types are preserved. Exact source bytes remain in raw files. CSV decoded line endings may be normalized; JSON record text is nullable.

UTF-8 with optional BOM is supported. Parsing currently holds one file in memory, with vectorized inserts in 1,000-row chunks. Very large future files need a streaming parser or a chunking contract. Captures remain separate per delivery even when parsed content is reused.

Batch rows are bound as one JSON parameter and expanded inside DuckDB. This avoids expensive Python STRUCT inference while retaining typed target columns and deterministic identities. The isolated 20,000-row development comparison measured approximately 232 seconds for STRUCT binding and 0.26 seconds for JSON binding; this excludes production file I/O and indexes and is not a throughput guarantee. scripts/benchmark_raw_batch.py can reproduce the comparison; pass json to measure only the selected method.

## Producer and Archival Protocol

Write to a temporary name, close it, and publish by atomic rename. Publish the manifest last. Never edit published files in place. Retain each declared version until archival completes. Incoming/raw/archive share one local Docker volume for atomic rename and hard links. External source objects remain unchanged; stage them locally first.

Archival moves the input inode atomically to a private .claim path before verifying its checksum, avoiding a check-then-rename race. Only matching bytes become a final archive file. If input was replaced, restore it without overwriting any newer input. If restoration collides, preserve the replacement in .claim. Fail visibly while retaining committed raw history. Operators must preserve replacements as new deliveries, restore expected original input from immutable raw when needed, and retry archival. Do not delete raw history or bypass verification by editing statuses.

A crash after claiming a file or publishing the archive destination is recoverable. Retry verifies the claim/destination and repairs the receipt without moving a newer file at the reused incoming name. Completed archived entries are skipped.

| Case | Behavior |
| --- | --- |
| Same delivery and manifest | Reuse sequence, timestamp, capture, and load markers |
| Same delivery_id with changed manifest | Reject; require a new delivery_id |
| New delivery with identical source content | Add batch lineage without parsing/loading content twice |
| Same name with new content in a new delivery | Capture/load a new file identity |
| Failure after raw commit | Repair SQLite confirmation without duplicating rows |
| Archival failure | Retry archival independently; retain committed raw |
| New downstream rules | Recalculate from immutable raw, without returning files to incoming |

## Stage the Exercise Baseline

The adapter checks the three required CSVs and steps, heart_rate, and sleep files for every participant declared in participants.csv. It imposes no 50-participant cap. Scope is explicit; baseline files are not inferred to be daily deliveries. Original downloaded files are copied and retained; the bootstrap manifest is published last. Changed baseline content requires a new delivery_id.

After downloading the source into the repository data directory:

```powershell
docker compose cp data airflow-scheduler:/opt/airflow/project-data/source-baseline
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/stage_baseline.py --source-root /opt/airflow/project-data/source-baseline --delivery-id baseline-2026-01-v1 --study-start 2026-01-08 --study-end 2026-02-04
```

Use the printed manifest_key to trigger wearable_daily_pipeline manually:

```json
{
  "target_date": "2026-01-08",
  "manifest_key": "<printed manifest key>",
  "sensor_timeout_seconds": 3600
}
```

Bootstrap loads all declared 28-day files, not only the trigger date. The subsequent daily analytics task validates and publishes the explicitly declared dates. source-baseline is adapter input, not a watched directory. If the staging copy already exists, reuse it or choose another directory rather than nesting a second copy into it.

## Verification

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python -m unittest discover -s /opt/airflow/project/tests -v
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_ddl.py --ddl-root /opt/airflow/project/ddl
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/verify_raw_airflow.py
```

The last check executes the real DAG twice using dag.test and temporary synthetic files/databases. Airflow metadata is used, but no synthetic records enter the project warehouse. It asserts one content row, one lineage row, one marker, and one archive file after replay. Scheduling and production-volume behavior require separate validation.

Run the isolated DAG check only while the raw DAG is paused with no queued/running runs. The script verifies those prerequisites before creating fixtures. Mixing dag.test with an unpaused scheduler for the same DAG can cause task execution to use the normal deployment environment instead of the temporary test paths.

For an explicit scheduled-executor run without using the UI, use scripts/run_raw_delivery.py with --manifest-key and --target-date through the Airflow container's Python. It triggers through the API and restores the prior paused state when finished. If its wait expires, inspect the existing run before triggering another; a timeout does not cancel the run. Runs are manual because automatic schedules remain disabled.

scripts/retry_raw_load.py --run-id <existing run> previews exactly one raw-load task. Add --apply only for a failed or retry-pending task to clear that task without clearing capture or registration. Do not use it on an active writer or to mutate a completed delivery. Airflow's normal retries use the same captured inputs automatically.

The loader executes DuckDB only; Snowflake DDL includes the matching raw marker table. Data Vault, quality quarantine, CDC acceptance, marts, and daily/weekly publication remain next phases.

## Verified Exercise Load

On October 3, 2026 (America/Bogota), the real baseline delivery baseline-2026-01-v1 was loaded through the Airflow scheduler, archived, and successfully replayed. Source files in the repository were preserved. The original slow STRUCT-based attempt was interrupted before commit and left zero raw records and markers; its retry used the optimized JSON binding and the same verified capture/sequence.

| Source | Raw records |
| --- | ---: |
| participants | 50 |
| device_metadata | 50 |
| steps | 2,014,950 |
| heart_rate | 2,014,950 |
| sleep | 1,400 |
| wellness_survey | 1,414 |
| Total | 4,032,814 |

All 153 declared files have archive receipts. Their captured and archived SHA-256 values were reconciled after replay; raw content, batch lineage, and file markers matched. These are structural ingestion counts, not accepted clinical measurements. No daily readiness or weekly publication was generated.

After the raw DAG completes and no warehouse writer is active, reproduce the reconciliation with:

```powershell
docker compose exec -T airflow-scheduler /opt/airflow/dbt-venv/bin/python /opt/airflow/project/scripts/verify_raw_delivery.py --source-system exercise_adapter --delivery-id baseline-2026-01-v1
```
