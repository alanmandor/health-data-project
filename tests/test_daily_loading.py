"""Exercise daily SQL, atomic publication, and readiness recovery on isolated data."""

import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from health_pipeline.daily_loading import load, current_mart
from health_pipeline.dbt_quality import project_hash
from health_pipeline.raw_ingestion import initialize

ROOT = Path(__file__).resolve().parents[1]
DDL = Path(os.getenv('PROJECT_DDL_ROOT',str(ROOT/'infra'/'ddl')))


class DailyTests(unittest.TestCase):
    def test_observed_zero_missing_data_corrections_and_receipt_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database, registry = root/'daily.duckdb', root/'registry.sqlite'
            initialize(database,registry,DDL)
            runtime = root/'dbt'
            runtime.mkdir()
            validation = {'database':str(database.resolve()),'code_version':project_hash(ROOT/'dbt'),
                          'quality_rule_version':'fixture-quality','study_timezone':'America/Bogota','input_hash':'fixture'}
            (runtime/'validated.json').write_text(json.dumps(validation))
            manifest = {'schema_version':1,'delivery_id':'daily','source_system':'fixture',
                'timezone':'America/Bogota','delivery_mode':'delta','batch_kind':'daily',
                'target_dates':['2026-01-08'],'completed_at':'2026-01-09T08:00:00Z',
                'sources':{s:{'status':'empty_delta','files':[]} for s in
                    ('participants','device_metadata','steps','heart_rate','sleep','wellness_survey')}}
            with sqlite3.connect(registry) as db:
                db.execute("INSERT INTO deliveries VALUES ('fixture','daily',1,'manifest','2026-01-09',?)",[json.dumps(manifest)])
                unloaded = json.loads(json.dumps(manifest))
                unloaded.update(delivery_id='not-loaded',target_dates=['2026-01-09'])
                unloaded['sources']['steps'] = {'status':'files','files':[{'object_key':'later.json',
                    'sha256':'a'*64,'bytes':10,'format':'json'}]}
                db.execute("INSERT INTO deliveries VALUES ('fixture','not-loaded',2,'unloaded','2026-01-10',?)",[json.dumps(unloaded)])
            with duckdb.connect(str(database)) as db:
                db.execute("INSERT INTO marts.dim_study VALUES ('study-sk','study','fixture','America/Bogota','2026-01-08','2026-01-10')")
                for participant in ('zero','missing'):
                    for day in ('2026-01-08','2026-01-09'):
                        db.execute("INSERT INTO business.participant_date_state VALUES ('study',?,?, 'state','impact')",[participant,day])
                db.execute("CREATE TABLE business.quality_current (participant_id VARCHAR,record_source VARCHAR, source_name VARCHAR,local_date DATE,observation_date DATE,warning_codes VARCHAR[])")
                db.execute("INSERT INTO marts.fact_steps_minute VALUES ('study','zero','study-sk','participant-sk',NULL,20260108,'2026-01-08T05:00:00Z',0,'step','accepted','rule')")
                for minute,bpm in [(0,60),(1,90)]:
                    db.execute("INSERT INTO marts.fact_heart_rate_minute VALUES ('study','zero','study-sk','participant-sk',NULL,20260108,?::TIMESTAMPTZ,?,?,'accepted','rule')",
                               [f'2026-01-08T05:0{minute}:00Z',bpm,'heart'+str(minute)])
            environment = {'PROJECT_DATABASE_PATH':str(database),'PROJECT_REGISTRY_PATH':str(registry),
                           'PROJECT_DBT_RUNTIME':str(runtime),'PROJECT_DBT_DIR':str(ROOT/'dbt')}
            with patch.dict(os.environ,environment), patch('health_pipeline.daily_loading.current_mart',return_value='mart-one'):
                def fail(db):
                    raise RuntimeError('Injected before commit')
                with self.assertRaisesRegex(RuntimeError,'before commit'):
                    load(_before_commit=fail)
                with duckdb.connect(str(database)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM analytics.participant_day').fetchone()[0],0)
                def crash():
                    raise RuntimeError('Injected after commit')
                with self.assertRaisesRegex(RuntimeError,'after commit'):
                    load(_after_commit=crash)
                with sqlite3.connect(registry) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM daily_readiness').fetchone()[0],0)
                replay = load()
                self.assertTrue(replay['reused'])
                self.assertEqual(replay['ready_dates'],1)
                self.assertTrue(load()['reused'])
                with duckdb.connect(str(database)) as db:
                    rows = db.execute('''SELECT participant_hk,observed_steps_total,heart_rate_sum,heart_rate_mean,
                        has_steps,has_heart_rate,expected_minutes FROM analytics.participant_day ORDER BY participant_hk''').fetchall()
                    self.assertEqual(rows,[('missing',None,None,None,False,False,1440),('zero',0,150,75,True,True,1440)])
                    self.assertEqual(db.execute('SELECT count(*) FROM marts.participant_day_current').fetchone()[0],4)
                    self.assertEqual(db.execute("SELECT count(*) FROM analytics.participant_day WHERE local_date='2026-01-09'").fetchone()[0],0)
                with sqlite3.connect(registry) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM daily_readiness').fetchone()[0],1)
                    coverage = json.loads(db.execute('SELECT coverage_json FROM daily_readiness').fetchone()[0])
                    self.assertEqual(coverage['participants_without_steps'],1)
                with duckdb.connect(str(database)) as db:
                    db.execute('DELETE FROM marts.fact_steps_minute')
                with patch('health_pipeline.daily_loading.current_mart',return_value='mart-two'):
                    with self.assertRaisesRegex(RuntimeError,'before commit'):
                        load(_before_commit=fail)
                    with duckdb.connect(str(database)) as db:
                        self.assertEqual(db.execute("SELECT observed_steps_total FROM analytics.participant_day WHERE participant_hk='zero'").fetchone()[0],0)
                    load()
                with duckdb.connect(str(database)) as db:
                    self.assertIsNone(db.execute("SELECT observed_steps_total FROM analytics.participant_day WHERE participant_hk='zero'").fetchone()[0])
                with sqlite3.connect(registry) as db:
                    self.assertEqual(db.execute('SELECT max(revision) FROM daily_readiness').fetchone()[0],2)
                # A legitimate implementation/input rollback must reactivate its receipt,
                # rather than letting an older checkpoint suppress the replacement.
                load()
                self.assertTrue(load()['reused'])
                with sqlite3.connect(registry) as db:
                    self.assertEqual(db.execute('SELECT max(revision) FROM daily_readiness').fetchone()[0],3)

    def test_calendar_uses_real_day_length(self):
        with duckdb.connect(':memory:') as db:
            for day,expected in [('2026-03-08',1380),('2026-11-01',1500)]:
                actual = db.execute("SELECT date_diff('minute',timezone('America/New_York',?::DATE::TIMESTAMP),timezone('America/New_York',(?::DATE+1)::TIMESTAMP))",[day,day]).fetchone()[0]
                self.assertEqual(actual,expected)

    def test_missing_mart_checkpoint_blocks_publication(self):
        with duckdb.connect(':memory:') as db:
            db.execute('CREATE SCHEMA business')
            db.execute('CREATE TABLE business.mart_evaluations (evaluation_id VARCHAR)')
            with patch('health_pipeline.daily_loading.latest_impact',return_value='uncommitted-impact'):
                with self.assertRaisesRegex(ValueError,'matching dimensional marts'):
                    current_mart(db,{},ROOT/'dbt')


if __name__=='__main__':
    unittest.main()
