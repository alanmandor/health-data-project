-- Extend the existing phase-one registry without dropping or changing deliveries.
-- UTC timestamps are ISO 8601 TEXT; JSON documents are serialized TEXT.
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS deliveries (
    source_system TEXT NOT NULL, delivery_id TEXT NOT NULL,
    sequence INTEGER NOT NULL, manifest_hash TEXT NOT NULL,
    registered_at TEXT NOT NULL, manifest_json TEXT NOT NULL,
    PRIMARY KEY (source_system, delivery_id), UNIQUE (source_system, sequence)
);
CREATE TABLE IF NOT EXISTS delivery_files (
    source_system TEXT NOT NULL, delivery_id TEXT NOT NULL, object_key TEXT NOT NULL,
    file_id TEXT NOT NULL, checksum TEXT NOT NULL, bytes INTEGER NOT NULL,
    format TEXT NOT NULL, version_id TEXT, source_name TEXT NOT NULL,
    immutable_raw_path TEXT, verification_status TEXT NOT NULL, verified_at TEXT,
    PRIMARY KEY (source_system, delivery_id, object_key)
);
CREATE TABLE IF NOT EXISTS model_checkpoints (
    checkpoint_id TEXT PRIMARY KEY NOT NULL, model_name TEXT NOT NULL,
    scope_json TEXT NOT NULL, input_hash TEXT NOT NULL, rule_version TEXT NOT NULL,
    code_version TEXT NOT NULL, status TEXT NOT NULL, committed_at TEXT
);
CREATE TABLE IF NOT EXISTS processing_runs (
    attempt_id TEXT PRIMARY KEY NOT NULL, airflow_run_id TEXT NOT NULL,
    task_id TEXT NOT NULL, batch_id TEXT, checkpoint_id TEXT,
    started_at TEXT NOT NULL, ended_at TEXT, status TEXT NOT NULL, error_code TEXT
);
CREATE TABLE IF NOT EXISTS daily_readiness (
    study_hk TEXT NOT NULL, target_date TEXT NOT NULL, revision INTEGER NOT NULL,
    input_manifest_hash TEXT NOT NULL, rule_version TEXT NOT NULL,
    code_version TEXT NOT NULL, validated_at TEXT NOT NULL,
    coverage_json TEXT NOT NULL, warning_counts_json TEXT NOT NULL,
    PRIMARY KEY (study_hk, target_date, revision)
);
CREATE TABLE IF NOT EXISTS publication_registry (
    publication_id TEXT PRIMARY KEY NOT NULL, study_hk TEXT NOT NULL,
    week_start TEXT NOT NULL, revision INTEGER NOT NULL,
    event_period_start TEXT NOT NULL, event_period_end TEXT NOT NULL,
    scheduled_run_at TEXT, correction_cutoff_at TEXT NOT NULL,
    consolidation_date TEXT NOT NULL, published_at TEXT, status TEXT NOT NULL,
    input_manifest_hash TEXT NOT NULL, rule_version TEXT NOT NULL, code_version TEXT NOT NULL,
    UNIQUE (study_hk, week_start, revision)
);
CREATE TABLE IF NOT EXISTS publication_inputs (
    publication_id TEXT NOT NULL, input_kind TEXT NOT NULL, version_id TEXT NOT NULL,
    source_relation TEXT NOT NULL, business_key_hash TEXT NOT NULL, payload_hash TEXT NOT NULL,
    PRIMARY KEY (publication_id, input_kind, version_id)
);
COMMIT;
