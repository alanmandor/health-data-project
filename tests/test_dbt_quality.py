"""Exercise real dbt SQL against isolated deliveries, corrections, and retries."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

try:
    import duckdb
except ImportError:
    duckdb = None

from health_pipeline.dbt_quality import build
from health_pipeline.raw_ingestion import initialize
from health_pipeline.vault_loading import load as load_vault
from health_pipeline.quality_issues import reconcile as reconcile_issues
from health_pipeline.affected_dates import propagate as propagate_dates
from health_pipeline.mart_loading import load as load_marts

ROOT = Path(__file__).resolve().parents[1]
DDL = Path(os.getenv("PROJECT_DDL_ROOT", str(ROOT / "infra" / "ddl")))


@unittest.skipUnless(duckdb is not None, "Run with the dbt virtual environment")
class DbtQualityTests(unittest.TestCase):
    def test_delta_quality_references_tombstones_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "fixture.duckdb"
            initialize(database, root / "registry.sqlite", DDL)
            counter = 0

            def add(source, payload, sequence=1):
                nonlocal counter
                counter += 1
                record = f"record-{counter:05}"
                with duckdb.connect(str(database)) as db:
                    db.execute("INSERT INTO raw.source_records VALUES (?, 'fixture', ?, ?, ?::JSON, NULL, now())",
                               [record, source, counter, json.dumps(payload)])
                    db.execute("INSERT INTO raw.batch_records VALUES (?, ?, 'fixture', ?, 'fixture.json')",
                               [f"batch-{sequence}", record, sequence])
                return record

            def participant(identifier):
                return {"participant_id": identifier, "device_id": "D-" + identifier, "age": 38,
                        "height_cm": 180, "gender": "male", "chronotype": "A", "max_heart_rate": 180}

            for index in range(51):
                add("participants", participant(f"P{index:03}"))
            excess_precision = add("participants", {**participant("PRECISION"), "height_cm": "180.0000001"})
            normal = {"participant_id": "P000", "timestamp": "2026-01-08T00:00:00", "steps": 1}
            first = add("steps", normal)
            identical = add("steps", {**normal, "timestamp": "2026-01-08T05:00:00Z", "steps": 60, "_unit": "steps_per_hour"})
            conflict_a = add("steps", {**normal, "timestamp": "2026-01-08T00:01:00", "steps": 2})
            conflict_b = add("steps", {**normal, "timestamp": "2026-01-08T00:01:00", "steps": 3})
            rejected = add("steps", {**normal, "timestamp": "2026-01-08T00:02:00.000001", "steps": -1})
            fractional = add("steps", {**normal, "timestamp": "2026-01-08T00:03:00", "steps": 1.5})
            stable = {**normal, "timestamp": "2026-01-08T00:10:00", "steps": 7}
            add("steps", stable)
            pending = add("wellness_survey", {"participant_id": "P999", "date": "2026-01-08",
                "fatigue_score": 1, "stress_score": 2, "readiness_score": 3, "sleep_quality_score": 4})
            metadata = add("device_metadata", {"participant_id": "P000", "device_id": "D-P000",
                "wear_site": "wrist", "calibration_date": "2026-01-01", "firmware_version": "",
                "device_label": "Tracker fw2.2.0"})
            label_conflict = add("device_metadata", {"participant_id": "P001", "device_id": "D-P001",
                "wear_site": "wrist", "calibration_date": "2026-01-01", "firmware_version": "1.0.0",
                "device_label": "Tracker fw2.2.0"})
            session = {"participant_id": "P000", "date": "2026-01-08", "sleep_onset": "2026-01-08T00:00:00",
                "sleep_end": "2026-01-08T02:00:00", "efficiency_pct": 100, "restlessness": 0.1,
                "stages": [{"stage": "light", "start_time": "2026-01-08T00:00:00", "end_time": "2026-01-08T01:00:00", "duration_min": 60},
                           {"stage": "awake", "start_time": "2026-01-08T01:00:00", "end_time": "2026-01-08T01:10:00", "duration_min": 10},
                           {"stage": "deep", "start_time": "2026-01-08T01:10:00", "end_time": "2026-01-08T02:00:00", "duration_min": 50}]}
            internal_awake = add("sleep", session)
            boundary_awake = add("sleep", {**session, "participant_id": "P001", "stages": session["stages"] + [
                {"stage": "awake", "start_time": "2026-01-08T02:00:00", "end_time": "2026-01-08T03:00:00", "duration_min": 60}]})
            start = datetime(2026, 1, 8, tzinfo=timezone.utc)
            for participant_id, minute_indexes in [("P000", range(60)), ("P001", list(range(30)) + list(range(31, 61)))]:
                for minute in minute_indexes:
                    add("heart_rate", {"participant_id": participant_id,
                        "timestamp": (start + timedelta(minutes=minute)).isoformat(), "heart_rate_bpm": 60})

            environment = {"PROJECT_DATABASE_PATH": str(database), "PROJECT_DBT_DIR": str(ROOT / "dbt"),
                "PROJECT_DBT_RUNTIME": str(root / "dbt"), "STUDY_TIMEZONE": "America/Bogota"}
            with patch.dict(os.environ, environment):
                build()
                def query(sql, parameters=None):
                    with duckdb.connect(str(database)) as db:
                        return db.execute(sql, parameters or []).fetchall()
                def decision(record):
                    return query("SELECT decision FROM business.record_quality WHERE record_id=?", [record])[0][0]

                self.assertEqual(decision(first), "accepted")
                self.assertEqual(decision(identical), "identical_duplicate")
                for record in (conflict_a, conflict_b, label_conflict):
                    self.assertEqual(decision(record), "conflict")
                for record in (rejected, fractional):
                    self.assertEqual(decision(record), "rejected")
                self.assertEqual(decision(excess_precision), "rejected")
                self.assertEqual(decision(pending), "pending_reference")
                self.assertEqual(query("SELECT count(*) FROM business.quality_participants_current")[0][0], 51)
                self.assertEqual(query("SELECT firmware_version, firmware_provenance FROM business.quality_current WHERE record_id=?", [metadata]), [("2.2.0", "device_label")])
                self.assertEqual(query("SELECT quality_status FROM business.record_quality WHERE record_id=?", [internal_awake]), [("accepted",)])
                self.assertEqual(query("SELECT quality_status, efficiency_pct_reported FROM business.record_quality WHERE record_id=?", [boundary_awake]), [("accepted_with_warning", 100)])
                self.assertEqual(query("SELECT count(*) FROM business.quality_heart_rate_flatlines")[0][0], 60)
                self.assertEqual(query("SELECT count(*) FROM staging.stg_sleep_stages WHERE session_record_id=?", [boundary_awake])[0][0], 4)
                def fail_before_commit(db):
                    raise RuntimeError("Synthetic failure before vault commit")
                with self.assertRaisesRegex(RuntimeError, "Synthetic failure"):
                    load_vault(_before_commit=fail_before_commit)
                self.assertEqual(query("SELECT count(*) FROM vault.hub_study")[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM vault.sat_steps_observation")[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM vault.version_lineage")[0][0], 0)
                self.assertFalse(load_vault()["reused"])
                self.assertTrue(load_vault()["reused"])
                with self.assertRaisesRegex(RuntimeError, "Synthetic failure"):
                    reconcile_issues(_before_commit=fail_before_commit)
                self.assertEqual(query("SELECT count(*) FROM business.quality_issues")[0][0], 0)
                self.assertFalse(reconcile_issues()["reused"])
                issue_before = query("SELECT * FROM business.quality_issues ORDER BY issue_id")
                self.assertTrue(reconcile_issues()["reused"])
                self.assertEqual(issue_before, query("SELECT * FROM business.quality_issues ORDER BY issue_id"))
                self.assertEqual(query("SELECT current_status FROM business.quality_issues WHERE record_id=? AND rule_code='unknown_participant'", [pending]), [("open",)])
                self.assertEqual(query("SELECT current_status FROM business.quality_issues WHERE record_id=? AND rule_code='sleep_boundary_discrepancy'", [boundary_awake]), [("open",)])
                self.assertEqual(query("SELECT count(*) FROM business.quality_issues WHERE record_id=?", [internal_awake])[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM business.quality_issues WHERE rule_code='suspected_heart_rate_flatline' AND current_status='open'")[0][0], 60)
                with self.assertRaisesRegex(RuntimeError, "Synthetic failure"):
                    propagate_dates(_before_commit=fail_before_commit)
                self.assertEqual(query("SELECT count(*) FROM business.participant_date_state")[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM business.affected_participant_dates")[0][0], 0)
                initial_impact = propagate_dates()
                self.assertEqual(initial_impact['affected_date_count'], 1430)
                queue_before = query("SELECT * FROM business.affected_participant_dates ORDER BY participant_hk, local_date, change_id")
                self.assertTrue(propagate_dates()['reused'])
                self.assertEqual(queue_before, query("SELECT * FROM business.affected_participant_dates ORDER BY participant_hk, local_date, change_id"))
                self.assertEqual(query("SELECT count(*) FROM business.participant_date_state s JOIN vault.hub_participant h USING (participant_hk) WHERE h.participant_id='P999'")[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM business.participant_date_state s JOIN vault.hub_participant h USING (participant_hk) WHERE h.participant_id='P000' AND local_date='2026-01-07'")[0][0], 1)
                with self.assertRaisesRegex(RuntimeError, 'Synthetic failure'):
                    load_marts(_before_commit=fail_before_commit)
                self.assertEqual(query('SELECT count(*) FROM marts.fact_steps_minute')[0][0], 0)
                self.assertEqual(query('SELECT count(*) FROM business.affected_participant_dates WHERE consumed_checkpoint_id IS NOT NULL')[0][0], 0)
                self.assertFalse(load_marts()['reused'])
                self.assertTrue(load_marts()['reused'])
                self.assertEqual(query('SELECT count(*) FROM marts.fact_steps_minute')[0][0], 2)
                self.assertEqual(query('SELECT count(*) FROM marts.fact_heart_rate_minute')[0][0], 120)
                self.assertEqual(query("SELECT count(*) FROM marts.fact_steps_minute WHERE date_sk=20260108")[0][0], 2)
                self.assertEqual(query("SELECT count(*) FROM marts.fact_heart_rate_minute WHERE date_sk=20260107")[0][0], 120)
                self.assertEqual(query('SELECT count(*) FROM marts.fact_sleep_stage')[0][0], 7)
                self.assertEqual(query('SELECT count(*) FROM marts.fact_wellness_daily')[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM vault.hub_participant WHERE participant_id='P999'")[0][0], 1)
                self.assertEqual(query("SELECT count(*) FROM vault.link_participant_study l JOIN vault.hub_participant h USING (participant_hk) WHERE h.participant_id='P999'")[0][0], 0)
                self.assertEqual(query("SELECT count(*) FROM vault.sat_device_metadata WHERE firmware_version_reported IS NULL")[0][0], 1)
                self.assertEqual(query("SELECT count(*) FROM business.vault_current_versions WHERE source_relation='vault.sat_steps_observation'")[0][0], 2)
                vault_before = query("SELECT version_id, loaded_at, payload_hash FROM vault.sat_steps_observation ORDER BY version_id")
                decision_before = query("SELECT decision_id, decided_at FROM business.version_decisions ORDER BY decision_id")
                before = query("SELECT record_id, decision, normalized_payload_hash FROM business.record_quality ORDER BY record_id")
                # Bootstrap and exact retry preserve dbt ownership and deterministic decisions.
                initialize(database, root / "registry.sqlite", DDL)
                self.assertEqual(query("SELECT count(*) FROM staging.stg_steps WHERE event_at_utc IS NOT NULL")[0][0], 7)
                build()
                self.assertTrue(load_vault()["reused"])
                self.assertEqual(vault_before, query("SELECT version_id, loaded_at, payload_hash FROM vault.sat_steps_observation ORDER BY version_id"))
                self.assertEqual(decision_before, query("SELECT decision_id, decided_at FROM business.version_decisions ORDER BY decision_id"))
                self.assertEqual(before, query("SELECT record_id, decision, normalized_payload_hash FROM business.record_quality ORDER BY record_id"))
                add("participants", participant("P999"), sequence=2)
                add("steps", {**normal, "steps": -1}, sequence=2)
                add("steps", {**stable, "steps": 8}, sequence=2)
                add("sleep", {**session, "participant_id": "P001", "sleep_end": session["sleep_onset"]}, sequence=2)
                add("steps", {**normal, "timestamp": "2026-01-08T00:01:00", "steps": 4}, sequence=2)
                add("sleep", {**session, "stages": [{"stage": "light", "start_time": session["sleep_onset"],
                    "end_time": session["sleep_end"], "duration_min": 120}]}, sequence=2)
                with self.assertRaisesRegex(ValueError, "Raw inputs changed"):
                    load_vault()
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 29)
                load_marts()
                self.assertEqual(query('SELECT count(*) FROM marts.fact_wellness_daily')[0][0], 1)
                self.assertEqual(query("SELECT count(*) FROM business.participant_date_state s JOIN vault.hub_participant h USING (participant_hk) WHERE h.participant_id='P999'")[0][0], 28)
                self.assertEqual(query("SELECT current_status FROM business.quality_issues WHERE record_id=?", [pending]), [("resolved",)])
                self.assertEqual(query("SELECT r.resolution_code FROM business.quality_resolutions r JOIN business.quality_issues i USING (issue_id) WHERE i.record_id=?", [pending]), [("reference_resolved",)])
                self.assertEqual(query("SELECT current_status FROM business.quality_issues WHERE record_id=?", [boundary_awake]), [("open",)])
                self.assertEqual(query("SELECT count(*) FROM business.quality_issues WHERE record_id IN (?,?) AND current_status='resolved'", [conflict_a, conflict_b])[0][0], 2)
                self.assertEqual(decision(pending), "accepted")
                self.assertEqual(query("SELECT decision FROM business.version_decisions WHERE source_relation='vault.sat_wellness_response' ORDER BY decided_at"), [("pending_reference",), ("accepted",)])
                self.assertEqual(query("SELECT steps_normalized FROM business.quality_current WHERE source_name='steps' AND event_at_utc='2026-01-08T05:00:00Z'")[0][0], 1)
                add("steps", {"participant_id": "P000", "timestamp": normal["timestamp"], "_op": "delete"}, sequence=3)
                add("participants", {"participant_id": "P999", "_op": "delete"}, sequence=3)
                add("sleep", {**session, "participant_id": "P001"}, sequence=3)
                add("steps", stable, sequence=3)
                add("sleep", session, sequence=3)
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 30)
                load_marts()
                self.assertEqual(query('SELECT count(*) FROM marts.fact_wellness_daily')[0][0], 0)
                self.assertEqual(query('SELECT count(*) FROM marts.fact_steps_minute')[0][0], 2)
                self.assertEqual(query("SELECT count(*) FROM business.affected_participant_dates s JOIN vault.hub_participant h USING (participant_hk) WHERE h.participant_id='P999' AND reason_code='current_state_removed'")[0][0], 28)
                self.assertEqual(query("SELECT r.resolution_code, r.superseding_version_id IS NOT NULL FROM business.quality_resolutions r JOIN business.quality_issues i USING (issue_id) WHERE i.record_id=?", [boundary_awake]), [("superseded_by_accepted_delta", True)])
                self.assertEqual(query("SELECT current_status FROM business.quality_issues WHERE record_id=?", [pending]), [("open",)])
                self.assertEqual(query("SELECT r.resolution_code FROM business.quality_resolutions r JOIN business.quality_issues i USING (issue_id) WHERE i.record_id=? ORDER BY resolved_at", [pending]), [("reference_resolved",), ("reopened",)])
                self.assertEqual(query("SELECT count(*) FROM business.quality_current WHERE source_name='steps'")[0][0], 2)
                self.assertEqual(query("SELECT count(*) FROM business.vault_current_versions WHERE source_relation='vault.sat_steps_observation'")[0][0], 2)
                self.assertEqual(query("SELECT r.resolution_code FROM business.quality_resolutions r JOIN business.quality_issues i USING (issue_id) JOIN business.record_quality q ON q.batch_id=i.batch_id AND q.record_id=i.record_id WHERE q.source_name='steps' AND q.delivery_sequence=2 AND q.event_at_utc='2026-01-08T05:00:00Z'"), [("source_deleted",)])
                self.assertEqual(query("SELECT steps_reported FROM vault.sat_steps_observation WHERE event_at_utc='2026-01-08T05:10:00Z' ORDER BY delivery_sequence"), [(7,), (8,), (7,)])
                self.assertEqual(query("SELECT count(*) FROM business.vault_accepted_changes c JOIN vault.sat_steps_observation s USING (version_id) WHERE c.source_relation='vault.sat_steps_observation' AND s.event_at_utc='2026-01-08T05:10:00Z'")[0][0], 3)
                self.assertEqual(query("SELECT count(*) FROM vault.sat_sleep_stage s JOIN vault.sat_sleep_session v ON v.version_id=s.session_version_id JOIN vault.hub_sleep_session h ON h.sleep_session_hk=v.parent_hk JOIN vault.hub_participant p USING (participant_hk) WHERE p.participant_id='P000'")[0][0], 7)
                # A rejected delta and a repeated accepted payload do not alter daily inputs.
                add('steps', {**normal, 'steps': -9}, sequence=4)
                add('steps', stable, sequence=4)
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 0)
                before_facts = query('SELECT * FROM marts.fact_steps_minute ORDER BY event_at_utc')
                load_marts()
                self.assertEqual(before_facts, query('SELECT * FROM marts.fact_steps_minute ORDER BY event_at_utc'))
                # A timestamp correction requires a tombstone for the preceding key.
                add('steps', {'participant_id': 'P000', 'timestamp': stable['timestamp'], '_op': 'delete'}, sequence=5)
                add('steps', {**stable, 'timestamp': '2026-01-09T00:10:00'}, sequence=5)
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 2)
                load_marts()
                self.assertEqual(query('SELECT count(*) FROM marts.fact_steps_minute WHERE date_sk=20260109')[0][0], 1)
                # A profile correction invalidates configured and observed coverage days.
                add('participants', {**participant('P000'), 'age': 39, 'height_cm': 181}, sequence=6)
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 29)
                load_marts()
                self.assertEqual(query("SELECT count(*) FROM marts.dim_participant WHERE participant_id='P000'")[0][0], 2)
                self.assertEqual(query("SELECT count(*) FROM marts.dim_participant WHERE participant_id='P000' AND is_current")[0][0], 1)
                add('participants', {**participant('P002'), 'device_id': 'D-P000'}, sequence=7)
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 57)
                load_marts()
                self.assertEqual(query("SELECT assignment_quality_status FROM marts.gold_participant_devices WHERE participant_id='P000'"), [('ambiguous_assignment',)])
                self.assertEqual(query("SELECT count(*) FROM marts.fact_steps_minute WHERE device_sk IS NOT NULL")[0][0], 0)
                add('participants', participant('P002'), sequence=8)
                add('participants', {**participant('P000'), 'age': 40, 'height_cm': 181}, sequence=8)
                build()
                load_vault()
                reconcile_issues()
                self.assertEqual(propagate_dates()['affected_date_count'], 57)
                load_marts()
                self.assertEqual(query("SELECT count(*) FROM marts.dim_participant WHERE participant_id='P000'")[0][0], 2)
                self.assertEqual(query("SELECT count(*) FROM marts.fact_steps_minute WHERE device_sk IS NOT NULL")[0][0], 2)


if __name__ == "__main__":
    unittest.main()
