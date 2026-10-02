"""Acceptance checks for calendar boundaries and immutable registration."""

import copy
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from health_pipeline.control import SOURCES, daily_scope, weekly_scope, safe_path, validate_manifest, delivery_available, register_delivery


def empty_manifest():
    return {"schema_version": 1, "delivery_id": "test-1", "source_system": "test",
            "batch_kind": "daily", "delivery_mode": "delta", "target_dates": ["2026-01-11"],
            "timezone": "America/Bogota", "completed_at": "2026-01-12T01:00:00-05:00",
            "sources": {source: {"status": "empty_delta", "files": []} for source in SOURCES}}


class CalendarTests(unittest.TestCase):
    def test_utc_trigger_maps_to_previous_bogota_day(self):
        scope = daily_scope(datetime(2026, 1, 12, 4, tzinfo=timezone.utc))
        self.assertEqual(scope["target_date"], "2026-01-10")
        self.assertEqual(scope["event_period_end"], "2026-01-11T05:00:00Z")

    def test_monday_trigger_has_midnight_business_boundaries(self):
        scope = weekly_scope(datetime(2026, 1, 12, 8, tzinfo=timezone.utc))
        self.assertEqual(scope["week_start"], "2026-01-05")
        self.assertEqual(scope["event_period_start"], "2026-01-05T05:00:00Z")
        self.assertEqual(scope["event_period_end"], "2026-01-12T05:00:00Z")

    def test_explicit_scope_is_independent_of_retry_time(self):
        self.assertEqual(weekly_scope(week_start="2026-01-05"), weekly_scope(datetime(2026, 2, 1, tzinfo=timezone.utc), week_start="2026-01-05"))
        with self.assertRaises(ValueError):
            weekly_scope(week_start="2026-01-06")
        with self.assertRaises(ValueError):
            daily_scope(datetime(2026, 1, 12))

    def test_configured_study_zone_changes_only_boundaries(self):
        scope = daily_scope(target_date="2026-01-11", study_timezone="UTC")
        self.assertEqual(scope["target_date"], "2026-01-11")
        self.assertEqual(scope["event_period_start"], "2026-01-11T00:00:00Z")

    def test_future_dst_study_day_does_not_assume_twenty_four_hours(self):
        scope = daily_scope(target_date="2026-03-29", study_timezone="Europe/Madrid")
        start = datetime.fromisoformat(scope["event_period_start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(scope["event_period_end"].replace("Z", "+00:00"))
        self.assertEqual((end - start).total_seconds(), 23 * 3600)


class ManifestTests(unittest.TestCase):
    def test_all_sources_and_aware_completion_required(self):
        manifest = empty_manifest()
        validate_manifest(manifest, "2026-01-11")
        del manifest["sources"]["steps"]
        with self.assertRaises(ValueError):
            validate_manifest(manifest, "2026-01-11")
        manifest = empty_manifest()
        manifest["completed_at"] = "2026-01-12T01:00:00"
        with self.assertRaises(ValueError):
            validate_manifest(manifest, "2026-01-11")

    def test_missing_manifest_is_not_empty_delta(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertFalse(delivery_available(directory, "manifest.json", "2026-01-11"))
            Path(directory, "manifest.json").write_text(json.dumps(empty_manifest()))
            self.assertTrue(delivery_available(directory, "manifest.json", "2026-01-11"))

    def test_replay_retains_sequence_and_timestamp_and_detects_changed_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "manifest.json")
            manifest = empty_manifest()
            path.write_text(json.dumps(manifest))
            scope = daily_scope(target_date="2026-01-11")
            registry = Path(directory, "state.sqlite")
            first = register_delivery(directory, "manifest.json", scope, registry)
            self.assertEqual(first, register_delivery(directory, "manifest.json", scope, registry))
            newer = copy.deepcopy(manifest)
            newer["delivery_id"] = "test-2"
            path.write_text(json.dumps(newer))
            self.assertEqual(register_delivery(directory, "manifest.json", scope, registry)["sequence"], 2)
            path.write_text(json.dumps(manifest))
            self.assertEqual(register_delivery(directory, "manifest.json", scope, registry), first)
            manifest["completed_at"] = "2026-01-12T02:00:00-05:00"
            path.write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                register_delivery(directory, "manifest.json", scope, registry)

    def test_checksum_rejects_changed_file_of_same_size(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = empty_manifest()
            file = Path(directory, "steps.json")
            file.write_bytes(b"[]")
            manifest["sources"]["steps"] = {"status": "files", "files": [{"object_key": "steps.json", "format": "json", "bytes": 2, "sha256": hashlib.sha256(b"[]").hexdigest()}]}
            Path(directory, "manifest.json").write_text(json.dumps(manifest))
            self.assertTrue(delivery_available(directory, "manifest.json", "2026-01-11"))
            file.write_bytes(b"{}")
            with self.assertRaises(ValueError):
                register_delivery(directory, "manifest.json", daily_scope(target_date="2026-01-11"), Path(directory, "state.sqlite"))

    def test_path_traversal_rejected(self):
        for key in ("../secret", "/absolute", "C:/secret", "folder/../../secret", "folder\\secret"):
            with self.assertRaises(ValueError):
                safe_path("/tmp/delivery", key)


if __name__ == "__main__":
    unittest.main()
