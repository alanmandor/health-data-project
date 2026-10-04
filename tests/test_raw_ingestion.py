"""Failure and replay checks for warehouse bootstrap and raw delivery lifecycle."""

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import duckdb
except ImportError:
    duckdb = None

from health_pipeline.control import SOURCES, daily_scope
from health_pipeline.raw_ingestion import available, archive, capture, initialize, load


@unittest.skipIf(duckdb is None, "Run raw integration tests with the dbt Python environment")
class RawTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.incoming = self.root / "incoming"
        self.incoming.mkdir()
        self.raw = self.root / "raw"
        self.archived = self.root / "archive"
        self.registry = self.root / "controls.sqlite"
        self.database = self.root / "warehouse.duckdb"
        import os
        self.ddl = Path(os.environ.get("PROJECT_DDL_ROOT", Path(__file__).resolve().parents[1] / "infra/ddl"))
        initialize(self.database, self.registry, self.ddl)
        self.scope = daily_scope(target_date="2026-01-11")
        self.manifest = {"schema_version": 1, "source_system": "test", "delivery_id": "first", "batch_kind": "daily", "delivery_mode": "delta", "target_dates": ["2026-01-11"], "timezone": "America/Bogota", "completed_at": "2026-01-12T08:00:00Z", "sources": {source: {"status": "empty_delta", "files": []} for source in SOURCES}}

    def tearDown(self):
        self.temporary.cleanup()

    def publish(self, payload=b'[{"participant_id":"UNKNOWN","steps":-1}]', source="steps", format="json"):
        key = f"{source}.{format}"
        (self.incoming / key).write_bytes(payload)
        self.manifest["sources"][source] = {"status": "files", "files": [{"object_key": key, "sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload), "format": format}]}
        self.write_manifest()

    def write_manifest(self):
        (self.incoming / "manifest.json").write_text(json.dumps(self.manifest))

    def capture(self):
        return capture(self.incoming, "manifest.json", self.scope, self.raw, self.registry)

    def counts(self):
        with duckdb.connect(str(self.database)) as db:
            return tuple(db.execute(f"SELECT count(*) FROM raw.{name}").fetchone()[0] for name in ("source_records", "batch_records", "file_loads"))

    def test_replay_after_archive_and_identical_new_delivery(self):
        self.publish()
        registered = self.capture()
        load(self.database, self.registry, registered)
        archive(self.incoming, self.archived, self.registry, registered)
        self.assertFalse((self.incoming / "steps.json").exists())
        self.assertTrue(available(self.incoming, "manifest.json", "2026-01-11", "America/Bogota", self.raw))
        self.assertEqual(self.capture()["sequence"], registered["sequence"])
        load(self.database, self.registry, registered)
        archive(self.incoming, self.archived, self.registry, registered)
        self.assertEqual(self.counts(), (1, 1, 1))
        self.manifest["delivery_id"] = "second"
        self.publish()
        second = self.capture()
        with patch("health_pipeline.raw_ingestion.file_rows", side_effect=AssertionError("Must not parse duplicate content")):
            load(self.database, self.registry, second)
        self.assertEqual(self.counts(), (1, 2, 1))

    def test_same_name_changed_content_loads_new_records(self):
        self.publish()
        first = self.capture()
        load(self.database, self.registry, first)
        archive(self.incoming, self.archived, self.registry, first)
        self.manifest["delivery_id"] = "correction"
        self.publish(b'[{"participant_id":"UNKNOWN","steps":5}]')
        second = self.capture()
        load(self.database, self.registry, second)
        self.assertEqual(second["sequence"], 2)
        self.assertEqual(self.counts(), (2, 2, 2))

    def test_parse_failure_rolls_back_and_input_remains(self):
        self.publish(b'{"invalid":"not an array"}')
        registered = self.capture()
        with self.assertRaises(ValueError):
            load(self.database, self.registry, registered)
        self.assertEqual(self.counts(), (0, 0, 0))
        with self.assertRaises(ValueError):
            archive(self.incoming, self.archived, self.registry, registered)
        self.assertTrue((self.incoming / "steps.json").exists())

    def test_source_replacement_is_restored_without_wrong_archive(self):
        self.publish()
        registered = self.capture()
        load(self.database, self.registry, registered)
        replacement = b'[{"steps":22}]'
        (self.incoming / "steps.json").write_bytes(replacement)
        with self.assertRaises(ValueError):
            archive(self.incoming, self.archived, self.registry, registered)
        self.assertEqual((self.incoming / "steps.json").read_bytes(), replacement)
        self.assertFalse(list(self.archived.rglob("*.json")))

    def test_crash_after_rename_is_recoverable(self):
        self.publish()
        registered = self.capture()
        load(self.database, self.registry, registered)
        from health_pipeline.raw_ingestion import checksum as real_checksum
        def crash(path):
            if str(path).endswith(".claim"):
                raise RuntimeError("Simulated worker failure")
            return real_checksum(path)
        with patch("health_pipeline.raw_ingestion.checksum", side_effect=crash):
            with self.assertRaises(RuntimeError):
                archive(self.incoming, self.archived, self.registry, registered)
        self.assertFalse((self.incoming / "steps.json").exists())
        archive(self.incoming, self.archived, self.registry, registered)
        self.assertEqual(len(list(self.archived.rglob("*.json"))), 1)

    def test_csv_preserves_quoted_multiline_values(self):
        self.publish(b'participant_id,device_label\r\nP001,"first\r\nsecond"\r\n', "device_metadata", "csv")
        registered = self.capture()
        load(self.database, self.registry, registered)
        with duckdb.connect(str(self.database)) as db:
            row = db.execute("SELECT payload_json,original_record_text FROM raw.source_records").fetchone()
            self.assertEqual(json.loads(row[0])["device_label"], "first\nsecond")
            self.assertIn('"first', row[1])
        initialize(self.database, self.registry, self.ddl)
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_empty_delta_completes_without_inventing_rows(self):
        self.write_manifest()
        registered = self.capture()
        load(self.database, self.registry, registered)
        self.assertEqual(archive(self.incoming, self.archived, self.registry, registered)["status"], "raw_archived")
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_changed_registered_identity_and_bad_checksum_are_rejected(self):
        self.publish()
        self.capture()
        self.manifest["completed_at"] = "2026-01-12T09:00:00Z"
        self.write_manifest()
        with self.assertRaises(ValueError):
            self.capture()

    def test_control_failure_after_duckdb_commit_does_not_duplicate(self):
        self.publish()
        registered = self.capture()
        from health_pipeline.raw_ingestion import files as real_files
        cached_files = real_files(self.registry, registered)
        with patch("health_pipeline.raw_ingestion.files", return_value=cached_files), patch("health_pipeline.raw_ingestion.sqlite3.connect", side_effect=RuntimeError("Control unavailable")):
            with self.assertRaises(RuntimeError):
                load(self.database, self.registry, registered)
        self.assertEqual(self.counts(), (1, 1, 1))
        load(self.database, self.registry, registered)
        archive(self.incoming, self.archived, self.registry, registered)
        self.assertEqual(self.counts(), (1, 1, 1))

    def test_second_invalid_file_rolls_back_the_whole_delivery(self):
        self.publish(b'[{"steps":5}]')
        self.publish(b'[{"heart_rate_bpm":60}]', "heart_rate")
        self.publish(b'{"bad":"array required"}', "sleep")
        registered = self.capture()
        with self.assertRaises(ValueError):
            load(self.database, self.registry, registered)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_bad_checksum_rejects_capture(self):
        self.publish()
        (self.incoming / "steps.json").write_bytes(b"[]")
        with self.assertRaises(ValueError):
            self.capture()

    def test_baseline_adapter_declares_all_files_and_reuses_manifest(self):
        import runpy
        stage = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/stage_baseline.py"))["stage"]
        source = self.root / "baseline"
        (source / "participants").mkdir(parents=True)
        (source / "participants/participants.csv").write_text("participant_id\nP001\nP051\n")
        (source / "health_summaries").mkdir()
        for name in ("device_metadata", "wellness_survey"):
            (source / f"health_summaries/{name}.csv").write_text("participant_id\nP001\nP051\n")
        for participant in ("P001", "P051"):
            folder = source / "wearable_events" / participant
            folder.mkdir(parents=True)
            for name in ("steps", "heart_rate", "sleep"):
                (folder / f"{name}.json").write_text("[]")
        key = stage(source, self.incoming, "baseline", "2026-01-08", "2026-02-04")
        manifest = json.loads((self.incoming / key).read_text())
        self.assertEqual(len(manifest["target_dates"]), 28)
        self.assertEqual(sum(len(entry["files"]) for entry in manifest["sources"].values()), 9)
        self.assertEqual(stage(source, self.incoming, "baseline", "2026-01-08", "2026-02-04"), key)
        (source / "wearable_events/P051/steps.json").write_text('[{"steps":1}]')
        with self.assertRaises(ValueError):
            stage(source, self.incoming, "baseline", "2026-01-08", "2026-02-04")

    def test_airflow_run_binds_manifest_across_capture_retries(self):
        self.publish()
        first = capture(self.incoming, "manifest.json", self.scope, self.raw, self.registry, run_key="run-one")
        self.manifest["delivery_id"] = "later-delivery"
        self.publish(b'[{"steps":7}]')
        repeated = capture(self.incoming, "manifest.json", self.scope, self.raw, self.registry, run_key="run-one")
        self.assertEqual(repeated["delivery_id"], first["delivery_id"])
        self.assertEqual(repeated["sequence"], first["sequence"])

    def test_unclosed_csv_quote_is_not_accepted_as_a_valid_record(self):
        self.publish(b'participant_id,device_label\nP001,"unclosed\n', "device_metadata", "csv")
        registered = self.capture()
        import csv
        with self.assertRaises(csv.Error):
            load(self.database, self.registry, registered)
        self.assertEqual(self.counts(), (0, 0, 0))


if __name__ == "__main__":
    unittest.main()
