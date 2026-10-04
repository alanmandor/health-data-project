"""Append Data Vault identities, observation versions, and quality decisions atomically."""

import argparse
from datetime import date
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys

from health_pipeline.dbt_quality import input_hash, project_hash

SATELLITES = (
    "participant_profile", "enrollment", "participant_device", "device_metadata",
    "steps_observation", "heart_rate_observation", "sleep_session", "wellness_response",
)
COMMON = {"version_id", "parent_hk", "batch_id", "delivery_sequence", "record_source", "loaded_at",
          "source_effective_at", "payload_hash", "hash_spec_version", "operation"}
HASH_SPEC = "dv-json-array-v1"


def identity(*fields):
    return hashlib.sha256(json.dumps(fields, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def configuration():
    code = os.getenv("STUDY_CODE", "pmdata_2026_01")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", code):
        raise ValueError("Study code must use letters, digits, underscores, or hyphens")
    start = date.fromisoformat(os.getenv("STUDY_START", "2026-01-08"))
    end = date.fromisoformat(os.getenv("STUDY_END_EXCLUSIVE", "2026-02-05"))
    if end <= start:
        raise ValueError("Study end must follow its start")
    return code, os.getenv("STUDY_TIMEZONE", "America/Bogota"), start, end


def register_study(db, config):
    code, zone, start, end = config
    existing = db.execute("SELECT study_code FROM vault.hub_study").fetchall()
    if existing and existing != [(code,)]:
        raise ValueError("This deployment supports one registered study per database")
    old = db.execute("SELECT study_timezone, study_start, study_end_exclusive FROM vault.sat_study").fetchall()
    if old and any(row != (zone, start, end) for row in old):
        raise ValueError("Changing a registered study requires an explicit configuration migration")
    study = identity("study", code)
    payload = identity("study_configuration", code, zone, start.isoformat(), end.isoformat())
    batch = identity("configuration_batch", payload)
    version = identity("study_configuration_version", study, payload)
    db.execute("INSERT INTO vault.hub_study SELECT ?, ?, ?, 'project_configuration', now(), ? WHERE NOT EXISTS (SELECT 1 FROM vault.hub_study WHERE study_hk=?)",
               [study, code, batch, HASH_SPEC, study])
    db.execute("INSERT INTO vault.sat_study SELECT ?, ?, ?, 0, 'project_configuration', now(), NULL, ?, ?, 'upsert', ?, ?, ?, 'configured-study-v1' WHERE NOT EXISTS (SELECT 1 FROM vault.sat_study WHERE version_id=?)",
               [version, study, batch, payload, HASH_SPEC, zone, start, end, version])
    return study


def append_satellites(db):
    for name in SATELLITES:
        relation = "vault.sat_" + name
        metadata = db.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='vault' AND table_name=? ORDER BY ordinal_position",
                              ["sat_" + name]).fetchall()
        if not metadata:
            raise RuntimeError(f"Missing foundation relation: {relation}")
        selected = []
        for column, kind in metadata:
            if column == "loaded_at":
                selected.append("now()")
            elif column in COMMON:
                selected.append("v." + column)
            else:
                selected.append(f"CAST(json_extract_string(v.payload, '$.{column}') AS {kind})")
        count = db.execute("SELECT count(*) FROM dv_versions WHERE source_relation=?", [relation]).fetchone()[0]
        for bucket in bucket_filters(count):
            db.execute(f"INSERT INTO {relation} ({', '.join(column for column, _ in metadata)}) SELECT {', '.join(selected)} FROM dv_versions v WHERE source_relation=? AND {bucket} AND NOT EXISTS (SELECT 1 FROM {relation} s WHERE s.version_id=v.version_id) QUALIFY row_number() OVER (PARTITION BY version_id ORDER BY record_id)=1",
                       [relation])
        logging.info("Appended missing versions: %s", relation)


def bucket_filters(count):
    # Partition large constraint-checked inserts without splitting the transaction.
    if count <= 100000:
        return ["true"]
    return [f"left(v.version_id, 1)='{prefix}'" for prefix in "0123456789abcdef"]


def registry_view(db):
    branches = []
    endpoints = {
        "enrollment": ("vault.link_participant_study", "participant_study_hk"),
        "participant_device": ("vault.link_participant_device", "participant_device_hk"),
        "sleep_session": ("vault.hub_sleep_session", "sleep_session_hk"),
        "wellness_response": ("vault.hub_wellness_response", "wellness_response_hk"),
    }
    for name in SATELLITES:
        join = ""
        if name == "device_metadata":
            participant = "p.participant_hk"
            join = " JOIN (SELECT DISTINCT l.version_id, h.participant_hk FROM vault.version_lineage l JOIN raw.source_records r USING (record_id) JOIN raw.batch_records b USING (batch_id, record_id) JOIN vault.hub_participant h ON h.business_namespace=b.source_system AND h.participant_id=nullif(trim(json_extract_string(r.payload_json, '$.participant_id')), '') WHERE l.source_relation='vault.sat_device_metadata') p USING (version_id)"
        elif name in endpoints:
            table, key = endpoints[name]
            join = f" JOIN {table} p ON p.{key}=v.parent_hk"
            participant = "p.participant_hk"
        else:
            participant = "v.parent_hk"
        branches.append(f"SELECT 'vault.sat_{name}' AS source_relation, v.version_id, v.parent_hk, {participant} AS participant_hk, s.study_hk, v.batch_id, v.delivery_sequence, v.record_source, v.loaded_at, v.payload_hash, v.operation FROM vault.sat_{name} v{join} CROSS JOIN vault.hub_study s")
    db.execute("CREATE OR REPLACE VIEW business.vault_versions AS " + " UNION ALL ".join(branches))


def append_decisions(db, evaluation, study):
    db.execute("""CREATE OR REPLACE TEMP VIEW dv_decisions AS
        SELECT *, CASE WHEN decision='identical_duplicate' THEN 'accepted' ELSE decision END AS vault_decision
        FROM dv_versions
        QUALIFY row_number() OVER (PARTITION BY source_relation, version_id
            ORDER BY CASE WHEN decision='identical_duplicate' THEN 1 ELSE 0 END, record_id)=1""")
    count = db.execute("SELECT count(*) FROM dv_versions").fetchone()[0]
    for bucket in bucket_filters(count):
        db.execute("""INSERT INTO business.version_decisions
        SELECT sha256(to_json(list_value(?, v.source_relation, v.version_id))), v.version_id,
               v.source_relation, v.business_key_hash, ?, v.vault_decision, now(), v.rule_version,
               v.batch_id, v.reason_code, v.normalized_payload_hash
        FROM dv_decisions v LEFT JOIN business.vault_decisions_current d
            ON d.source_relation=v.source_relation AND d.version_id=v.version_id
        WHERE """ + bucket + """ AND (d.version_id IS NULL OR d.decision IS DISTINCT FROM v.vault_decision
            OR d.reason_code IS DISTINCT FROM v.reason_code OR d.rule_version IS DISTINCT FROM v.rule_version
            OR d.normalized_payload_hash IS DISTINCT FROM v.normalized_payload_hash)""", [evaluation, study])


def verify_integrity(db):
    parents = {
        "participant_profile": ("hub_participant", "participant_hk"),
        "enrollment": ("link_participant_study", "participant_study_hk"),
        "participant_device": ("link_participant_device", "participant_device_hk"),
        "device_metadata": ("hub_device", "device_hk"),
        "steps_observation": ("hub_participant", "participant_hk"),
        "heart_rate_observation": ("hub_participant", "participant_hk"),
        "sleep_session": ("hub_sleep_session", "sleep_session_hk"),
        "wellness_response": ("hub_wellness_response", "wellness_response_hk"),
    }
    for satellite, (parent, key) in parents.items():
        if db.execute(f"SELECT 1 FROM vault.sat_{satellite} s WHERE NOT EXISTS (SELECT 1 FROM vault.{parent} p WHERE p.{key}=s.parent_hk) LIMIT 1").fetchone():
            raise AssertionError(f"Orphan parent in sat_{satellite}")
    for link, target, endpoint in [
        ("participant_device", "device", "device_hk"),
        ("participant_device", "participant", "participant_hk"),
        ("participant_study", "study", "study_hk"),
        ("participant_study", "participant", "participant_hk"),
        ("participant_sleep_session", "sleep_session", "sleep_session_hk"),
        ("participant_sleep_session", "participant", "participant_hk"),
        ("participant_wellness_response", "wellness_response", "wellness_response_hk"),
        ("participant_wellness_response", "participant", "participant_hk"),
    ]:
        if db.execute(f"SELECT 1 FROM vault.link_{link} l WHERE NOT EXISTS (SELECT 1 FROM vault.hub_{target} h WHERE h.{endpoint}=l.{endpoint}) LIMIT 1").fetchone():
            raise AssertionError(f"Orphan endpoint in link_{link}")
    logging.info("Verifying stage parents")
    if db.execute("SELECT 1 FROM vault.sat_sleep_stage s WHERE NOT EXISTS (SELECT 1 FROM vault.sat_sleep_session p WHERE p.version_id=s.session_version_id) LIMIT 1").fetchone():
        raise AssertionError("Vault reconciliation failed: stage")
    logging.info("Verifying persisted source candidates")
    for satellite in SATELLITES:
        relation = "vault.sat_" + satellite
        count = db.execute("SELECT count(*) FROM dv_versions WHERE source_relation=?", [relation]).fetchone()[0]
        for bucket in bucket_filters(count):
            target_filter = bucket.replace("v.version_id", "p.version_id")
            if db.execute(f"SELECT 1 FROM dv_versions v WHERE source_relation=? AND {bucket} AND NOT EXISTS (SELECT 1 FROM {relation} p WHERE {target_filter} AND p.version_id=v.version_id) LIMIT 1", [relation]).fetchone():
                raise AssertionError(f"Vault reconciliation failed: {relation}")
    logging.info("Verifying raw lineage and current decisions")
    count = db.execute("SELECT count(*) FROM dv_versions").fetchone()[0]
    for bucket in bucket_filters(count):
        lineage_filter = bucket.replace("v.version_id", "l.version_id")
        if db.execute(f"SELECT 1 FROM vault.version_lineage l WHERE {lineage_filter} AND NOT EXISTS (SELECT 1 FROM raw.batch_records r WHERE r.batch_id=l.batch_id AND r.record_id=l.record_id) LIMIT 1").fetchone():
            raise AssertionError("Vault reconciliation failed: lineage")
        target_filter = bucket.replace("v.version_id", "d.version_id")
        sql = f"""WITH latest AS (
            SELECT source_relation, version_id, decision, reason_code, rule_version, normalized_payload_hash
            FROM business.version_decisions d WHERE {target_filter}
            QUALIFY row_number() OVER (PARTITION BY source_relation, version_id ORDER BY decided_at DESC, decision_id DESC)=1
        ) SELECT 1 FROM dv_decisions v WHERE {bucket} AND NOT EXISTS (
            SELECT 1 FROM latest d WHERE d.source_relation=v.source_relation AND d.version_id=v.version_id
                AND d.decision=v.vault_decision AND d.reason_code IS NOT DISTINCT FROM v.reason_code
                AND d.rule_version=v.rule_version AND d.normalized_payload_hash IS NOT DISTINCT FROM v.normalized_payload_hash
        ) LIMIT 1"""
        if db.execute(sql).fetchone():
            raise AssertionError("Vault reconciliation failed: decision")


def load(database=None, project=None, runtime=None, _before_commit=None):
    import duckdb
    database = Path(database or os.environ["PROJECT_DATABASE_PATH"]).resolve()
    project = Path(project or os.getenv("PROJECT_DBT_DIR", "/opt/airflow/project/dbt"))
    runtime = Path(runtime or os.getenv("PROJECT_DBT_RUNTIME", "/opt/airflow/project-data/dbt"))
    config = configuration()
    validation = json.loads((runtime / "validated.json").read_text())
    code = project_hash(project)
    if (validation["database"] != str(database) or validation["code_version"] != code
            or validation["study_code"] != config[0] or validation["study_timezone"] != config[1]):
        raise ValueError("Run dbt validation for this database, code, and study before loading the vault")
    loader_code = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    evaluation = identity("vault_evaluation", validation["input_hash"], code, loader_code, *map(str, config))
    with duckdb.connect(str(database)) as db:
        db.execute("SET TimeZone='UTC'")
        # Bound engine-managed working memory and concurrent query buffers in
        # the shared Docker VM. Large intermediate relations can spill to disk.
        db.execute("SET threads=1")
        db.execute("SET memory_limit=?", [os.getenv("VAULT_MEMORY_LIMIT", "4GB")])
        db.execute("SET preserve_insertion_order=false")
        temporary = runtime / "vault-temp"
        temporary.mkdir(parents=True, exist_ok=True)
        db.execute("SET temp_directory=?", [str(temporary)])
        if validation["input_hash"] != input_hash(db):
            raise ValueError("Raw inputs changed after dbt validation")
        db.execute("BEGIN TRANSACTION")
        try:
            study = register_study(db, config)
            db.execute("CREATE TABLE IF NOT EXISTS business.vault_loads (evaluation_id VARCHAR PRIMARY KEY, input_hash VARCHAR NOT NULL, quality_code_version VARCHAR NOT NULL, loader_code_version VARCHAR NOT NULL, study_hk VARCHAR NOT NULL, committed_at TIMESTAMPTZ NOT NULL)")
            if db.execute("SELECT 1 FROM business.vault_loads WHERE evaluation_id=?", [evaluation]).fetchone():
                db.execute("COMMIT")
                return {"status": "vault_loaded", "evaluation_id": evaluation, "reused": True, "consumer_published": False}
            db.execute((project / "operations" / "load_vault.sql").read_text())
            append_satellites(db)
            db.execute("""INSERT INTO vault.sat_sleep_stage
                SELECT v.version_id, s.stage_position, s.stage_type, s.start_at_utc, s.end_at_utc,
                       s.start_original, s.end_original, s.duration_min_reported
                FROM dv_versions v JOIN staging.stg_sleep_stages s
                    ON v.record_id=s.session_record_id AND v.batch_id=s.batch_id
                WHERE v.source_relation='vault.sat_sleep_session' AND v.operation='upsert'
                    AND NOT EXISTS (SELECT 1 FROM vault.sat_sleep_stage t
                        WHERE t.session_version_id=v.version_id AND t.stage_position=s.stage_position)
                QUALIFY row_number() OVER (PARTITION BY v.version_id, s.stage_position ORDER BY v.record_id)=1""")
            count = db.execute("SELECT count(*) FROM dv_versions").fetchone()[0]
            for bucket in bucket_filters(count):
                db.execute("""INSERT INTO vault.version_lineage
                SELECT v.source_relation, v.version_id, v.batch_id, v.record_id FROM dv_versions v
                WHERE """ + bucket + """ AND NOT EXISTS (SELECT 1 FROM vault.version_lineage l WHERE
                    l.source_relation=v.source_relation AND l.version_id=v.version_id
                    AND l.batch_id=v.batch_id AND l.record_id=v.record_id)""")
            registry_view(db)
            db.execute((project / "operations" / "vault_business_views.sql").read_text())
            append_decisions(db, evaluation, study)
            verify_integrity(db)
            db.execute("INSERT INTO business.vault_loads VALUES (?,?,?,?,?,now())",
                       [evaluation, validation["input_hash"], code, loader_code, study])
            if _before_commit is not None:
                _before_commit(db)
            # Release intermediates before committing persistent tables and indexes.
            db.execute("DROP VIEW dv_decisions")
            db.execute("DROP TABLE dv_versions")
            logging.info("Committing validated Vault load")
            db.execute("COMMIT")
        except BaseException:
            try:
                db.execute("ROLLBACK")
            except duckdb.TransactionException:
                # A failed commit can already have aborted the transaction.
                pass
            raise
    return {"status": "vault_loaded", "evaluation_id": evaluation, "reused": False, "consumer_published": False}


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(json.dumps(load()))


if __name__ == "__main__":
    main()
