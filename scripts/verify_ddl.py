"""Verify foundation DDL in temporary databases; never modify the warehouse."""

import argparse
import sqlite3
import unittest
from pathlib import Path

import duckdb

from generate_ddl import TABLES

DDL_ROOT = Path(__file__).resolve().parents[1] / "infra" / "ddl"


class FoundationTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect(":memory:")
        self.sql = (DDL_ROOT / "duckdb" / "001_foundation.sql").read_text()
        self.db.execute(self.sql)

    def tearDown(self):
        self.db.close()

    def insert(self, name, **overrides):
        """Build a complete synthetic row from the declared column contract."""
        columns = next(columns for relation, columns, *_ in TABLES if relation == name)
        defaults = {"s": "test", "i": 1, "h": 1, "n": 1, "d": "2026-01-12", "t": "2026-01-19T08:00:00Z", "j": "{}", "b": True}
        values = {spec.split(":")[0]: defaults[spec.split(":")[1].rstrip("!")] for spec in columns}
        values.update(overrides)
        marks = ", ".join("?" for _ in values)
        self.db.execute(f"INSERT INTO {name} ({', '.join(values)}) VALUES ({marks})", list(values.values()))

    def test_bootstrap_preserves_existing_records_and_all_tables(self):
        self.insert("raw.source_records", record_id="original")
        self.db.execute(self.sql)
        self.assertEqual(self.db.execute("SELECT count(*) FROM raw.source_records").fetchone()[0], 1)
        count = self.db.execute("SELECT count(*) FROM information_schema.tables WHERE table_type = 'BASE TABLE'").fetchone()[0]
        self.assertEqual(count, len(TABLES))

    def test_raw_invalid_measurement_is_preserved_but_fact_is_rejected(self):
        self.insert("raw.source_records", payload_json='{"heart_rate_bpm": 999}')
        with self.assertRaises(duckdb.ConstraintException):
            self.insert("marts.fact_heart_rate_minute", heart_rate_bpm=999)
        self.assertEqual(self.db.execute("SELECT count(*) FROM raw.source_records").fetchone()[0], 1)

    def test_published_revision_remains_visible_while_new_revision_builds(self):
        self.insert("publication_build.publications", publication_id="first", status="published", revision=1)
        self.insert("publication_build.publications", publication_id="next", status="building", revision=2, published_at=None)
        self.insert("publication_build.participant_week", publication_id="first")
        self.insert("publication_build.participant_week", publication_id="next")
        self.insert("publication_build.participant_snapshot", publication_id="first", chronotype="A")
        self.insert("publication_build.participant_snapshot", publication_id="next", chronotype="B")
        self.assertEqual(self.db.execute("SELECT publication_id FROM biostatistics.latest_participant_week").fetchall(), [("first",)])
        self.assertEqual(self.db.execute("SELECT chronotype FROM biostatistics.participant_snapshot").fetchall(), [("A",)])
        self.db.execute("UPDATE publication_build.publications SET status='published', published_at=now() WHERE publication_id='next'")
        self.assertEqual(self.db.execute("SELECT publication_id FROM biostatistics.latest_participant_week").fetchall(), [("next",)])
        self.assertEqual(self.db.execute("SELECT chronotype FROM biostatistics.participant_snapshot WHERE publication_id='first'").fetchall(), [("A",)])

    def test_daily_view_requires_matching_validated_publication(self):
        self.insert("marts.participant_day_current", quality_status="accepted")
        self.insert("business.daily_publications", status="building", published_at=None)
        self.assertEqual(self.db.execute("SELECT count(*) FROM analytics.participant_day").fetchone()[0], 0)
        self.db.execute("UPDATE business.daily_publications SET status='published', published_at=now()")
        self.assertEqual(self.db.execute("SELECT count(*) FROM analytics.participant_day").fetchone()[0], 1)
        self.db.execute("UPDATE marts.participant_day_current SET input_manifest_hash='new-input'")
        self.assertEqual(self.db.execute("SELECT count(*) FROM analytics.participant_day").fetchone()[0], 0)

    def test_staging_keeps_parse_failures_and_sleep_array_positions(self):
        self.insert("raw.source_records", record_id="hr", source_name="heart_rate", payload_json='{"heart_rate_bpm":"invalid","participant_id":"P001"}')
        self.insert("raw.batch_records", record_id="hr")
        self.assertEqual(self.db.execute("SELECT heart_rate_bpm FROM staging.stg_heart_rate").fetchall(), [(None,)])
        self.insert("raw.source_records", record_id="sleep", source_name="sleep", payload_json='{"participant_id":"P001","date":"2026-01-12","stages":[{"stage":"light","duration_min":20},{"stage":"awake","duration_min":5}]}')
        self.insert("raw.batch_records", record_id="sleep")
        self.assertEqual(self.db.execute("SELECT stage_position, stage_type FROM staging.stg_sleep_stages ORDER BY stage_position").fetchall(), [(0, "light"), (1, "awake")])

    def test_control_extension_preserves_delivery_identity(self):
        with sqlite3.connect(":memory:") as control:
            sql = (DDL_ROOT / "sqlite" / "001_control_registry.sql").read_text()
            control.executescript(sql)
            control.execute("INSERT INTO deliveries VALUES ('source', 'delivery', 1, 'hash', '2026-01-19T08:00:00Z', '{}')")
            control.commit()
            control.executescript(sql)
            self.assertEqual(control.execute("SELECT delivery_id FROM deliveries").fetchall(), [("delivery",)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ddl-root", type=Path, default=DDL_ROOT)
    args = parser.parse_args()
    DDL_ROOT = args.ddl_root
    unittest.main(argv=[__file__], verbosity=2)
