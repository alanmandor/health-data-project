"""Reconcile persisted Vault versions, source lineage, and baseline expectations."""

import argparse
import json
import os
from pathlib import Path

import duckdb

from health_pipeline.vault_loading import SATELLITES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    args = parser.parse_args()
    with duckdb.connect(os.environ["PROJECT_DATABASE_PATH"], read_only=True) as db:
        db.execute("SET TimeZone='UTC'")
        db.execute("SET threads=1")
        db.execute("SET preserve_insertion_order=false")
        db.execute("SET memory_limit=?", [os.getenv("VAULT_MEMORY_LIMIT", "4GB")])
        temporary = Path(os.getenv("PROJECT_DBT_RUNTIME", "/opt/airflow/project-data/dbt")) / "verify-vault-temp"
        temporary.mkdir(parents=True, exist_ok=True)
        db.execute("SET temp_directory=?", [str(temporary)])
        counts = {}
        tables = db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='vault' ORDER BY table_name").fetchall()
        for (table,) in tables:
            counts[table] = db.execute(f"SELECT count(*) FROM vault.{table}").fetchone()[0]
        if db.execute("SELECT 1 FROM staging.dv_satellite_candidates c WHERE NOT EXISTS (SELECT 1 FROM vault.version_lineage l WHERE l.source_relation=c.source_relation AND l.version_id=c.version_id AND l.batch_id=c.batch_id AND l.record_id=c.record_id) LIMIT 1").fetchone():
            raise AssertionError("Missing candidate source lineage")
        if db.execute("SELECT 1 FROM vault.version_lineage l WHERE NOT EXISTS (SELECT 1 FROM business.vault_versions v WHERE v.source_relation=l.source_relation AND v.version_id=l.version_id) LIMIT 1").fetchone():
            raise AssertionError("Lineage references a missing persisted version")
        for relation in SATELLITES:
            table = "vault.sat_" + relation
            if db.execute(f"SELECT 1 FROM {table} v WHERE NOT EXISTS (SELECT 1 FROM business.vault_decisions_current d WHERE d.source_relation=? AND d.version_id=v.version_id) LIMIT 1", [table]).fetchone():
                raise AssertionError(f"Missing quality decision: {table}")
        if db.execute("SELECT 1 FROM business.vault_current_versions GROUP BY source_relation, business_key_hash HAVING count(*)>1 LIMIT 1").fetchone():
            raise AssertionError("Current-version keys are not unique")
        report = {"vault_counts": counts,
                  "load_checkpoints": db.execute("SELECT count(*) FROM business.vault_loads").fetchone()[0],
                  "decisions": db.execute("SELECT decision, count(*) FROM business.vault_decisions_current GROUP BY decision ORDER BY decision").fetchall()}
        if args.baseline:
            expected = {"hub_participant": 51, "hub_device": 50, "hub_study": 1, "hub_sleep_session": 1400,
                        "hub_wellness_response": 1414, "link_participant_device": 50, "link_participant_study": 50,
                        "link_participant_sleep_session": 1400, "link_participant_wellness_response": 1414,
                        "sat_participant_profile": 50, "sat_enrollment": 50, "sat_participant_device": 100,
                        "sat_device_metadata": 50, "sat_study": 1, "sat_sleep_session": 1400, "sat_wellness_response": 1414}
            for name, expected_count in expected.items():
                if counts[name] != expected_count:
                    raise AssertionError(f"Unexpected baseline count for {name}: {counts[name]}")
            if db.execute("SELECT count(*) FROM vault.sat_device_metadata WHERE firmware_version_reported IS NULL").fetchone()[0] != 15:
                raise AssertionError("Raw satellite must preserve the 15 originally blank firmware values")
            if db.execute("SELECT count(*) FROM business.vault_decisions_current WHERE source_relation='vault.sat_wellness_response' AND decision='pending_reference'").fetchone()[0] != 14:
                raise AssertionError("Unknown participant surveys must remain pending")
            if db.execute("SELECT count(*) FROM business.vault_current_versions WHERE source_relation='vault.sat_wellness_response'").fetchone()[0] != 1400:
                raise AssertionError("Unknown participant must not enter accepted current responses")
            for source, satellite in [("steps", "steps_observation"), ("heart_rate", "heart_rate_observation")]:
                current = db.execute("SELECT count(*) FROM business.vault_current_versions WHERE source_relation=?", ["vault.sat_" + satellite]).fetchone()[0]
                quality = db.execute("SELECT count(*) FROM business.record_quality WHERE source_name=? AND decision='accepted'", [source]).fetchone()[0]
                if current != quality:
                    raise AssertionError(f"Accepted minute counts differ for {source}")
            session = db.execute("SELECT v.version_id, s.efficiency_pct_reported FROM business.vault_current_versions v JOIN vault.sat_sleep_session s USING (version_id) JOIN vault.hub_sleep_session h ON h.sleep_session_hk=s.parent_hk JOIN vault.hub_participant p ON p.participant_hk=h.participant_hk WHERE v.source_relation='vault.sat_sleep_session' AND p.participant_id='P001' AND h.wake_date='2026-01-21'").fetchone()
            if session is None or session[1] != 100:
                raise AssertionError("P001 reported efficiency changed")
            awake = db.execute("SELECT duration_min_reported FROM vault.sat_sleep_stage WHERE session_version_id=? AND stage_type='awake'", [session[0]]).fetchall()
            if awake != [(55,)]:
                raise AssertionError("P001 provisional awake stage was removed")
            report["baseline_verified"] = True
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
