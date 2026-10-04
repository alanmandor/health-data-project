"""Verify foundation DDL in temporary databases; never modify the warehouse."""

import argparse
import sqlite3
import unittest
from pathlib import Path

import duckdb

from generate_ddl import TABLES
from migrate_quality_domains import migrate
from verify_quality_catalogs import verify as verify_catalogs

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
        self.assertEqual(verify_catalogs(self.db), {
            'quality_issue_statuses': 2, 'quality_severities': 3, 'quality_resolution_codes': 6})

    def test_raw_invalid_measurement_is_preserved_but_fact_is_rejected(self):
        self.insert("raw.source_records", payload_json='{"heart_rate_bpm": 999}')
        with self.assertRaises(duckdb.ConstraintException):
            self.insert("marts.fact_heart_rate_minute", heart_rate_bpm=999)
        self.assertEqual(self.db.execute("SELECT count(*) FROM raw.source_records").fetchone()[0], 1)

    def test_quality_domains_reject_invalid_inserts_and_updates(self):
        self.insert('business.quality_issues', severity='warning', current_status='open')
        for field, value in [('severity', 'fatal'), ('current_status', 'reopened')]:
            with self.subTest(field=field), self.assertRaises(duckdb.ConstraintException):
                self.db.execute(f'UPDATE business.quality_issues SET {field}=?', [value])
        with self.assertRaises(duckdb.ConstraintException):
            self.insert('business.quality_issues', issue_id='invalid', severity='error', current_status='unknown')
        codes = ('reference_resolved', 'superseded_by_accepted_delta', 'source_deleted',
                 'rule_cleared', 'excluded_at_consolidation', 'reopened')
        for code in codes:
            self.insert('business.quality_resolutions', resolution_id=code, resolution_code=code)
        with self.assertRaises(duckdb.ConstraintException):
            self.insert('business.quality_resolutions', resolution_id='invalid', resolution_code='unknown')

    def test_quality_migration_preserves_history_and_repeats(self):
        self.insert('business.quality_issues', severity='pending', current_status='open')
        self.insert('business.quality_resolutions', resolution_code='reopened')
        before = self.db.execute('SELECT * FROM business.quality_issues').fetchall()
        history = self.db.execute('SELECT * FROM business.quality_resolutions').fetchall()
        sql = (DDL_ROOT / 'duckdb/002_quality_domains.sql').read_text()
        migrate(self.db, sql)
        migrate(self.db, sql)
        self.assertEqual(before, self.db.execute('SELECT * FROM business.quality_issues').fetchall())
        self.assertEqual(history, self.db.execute('SELECT * FROM business.quality_resolutions').fetchall())

    def test_quality_migration_rolls_back_invalid_existing_history(self):
        self.insert('business.quality_issues', severity='warning', current_status='open')
        self.db.execute('DROP TABLE business.quality_resolutions')
        self.db.execute('CREATE TABLE business.quality_resolutions AS SELECT * FROM (VALUES '
                        "('r', 'test', TIMESTAMPTZ '2026-01-01', 'unknown', NULL::VARCHAR, NULL::VARCHAR)) "
                        't(resolution_id, issue_id, resolved_at, resolution_code, superseding_version_id, publication_id)')
        with self.assertRaises(duckdb.ConstraintException):
            migrate(self.db, (DDL_ROOT / 'duckdb/002_quality_domains.sql').read_text())
        self.assertEqual(self.db.execute('SELECT resolution_code FROM business.quality_resolutions').fetchone()[0], 'unknown')
        self.assertEqual(self.db.execute('SELECT count(*) FROM business.quality_issues').fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%domain_migration'").fetchone()[0], 0)

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
