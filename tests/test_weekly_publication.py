"""Verify real frozen-input dbt builds, cutoff retries, revisions, and sleep policy."""

from datetime import date
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from health_pipeline.control import weekly_scope
from health_pipeline.raw_ingestion import initialize
from health_pipeline.vault_loading import identity
from health_pipeline.weekly_controls import readiness, required_days
from health_pipeline.weekly_publication import freeze, publish

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from generate_ddl import TABLES
DDL=Path(os.getenv('PROJECT_DDL_ROOT',str(ROOT/'infra'/'ddl')))


def insert(db,table,**overrides):
    specs=next(columns for name,columns,*_ in TABLES if name==table)
    defaults={'s':'fixture','i':0,'h':1,'n':0,'d':'2026-01-08','t':'2026-01-12T08:00:00Z','j':'{}','b':False}
    values={name:defaults[kind.rstrip('!')] if kind.endswith('!') else None
            for spec in specs for name,kind in [spec.split(':')]}
    values.update(overrides)
    db.execute(f"INSERT INTO {table} ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",list(values.values()))


class WeeklyTests(unittest.TestCase):
    def test_freeze_retry_sleep_policy_and_late_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            database,registry=root/'weekly.duckdb',root/'registry.sqlite'
            initialize(database,registry,DDL)
            study=identity('study','weekly_fixture')
            scope=weekly_scope(week_start='2026-01-05')
            environment={'PROJECT_DATABASE_PATH':str(database),'PROJECT_REGISTRY_PATH':str(registry),
                'PROJECT_DBT_RUNTIME':str(root/'dbt'),'PROJECT_DBT_DIR':str(ROOT/'dbt'),
                'STUDY_CODE':'weekly_fixture','STUDY_START':'2026-01-08','STUDY_END_EXCLUSIVE':'2026-01-10','STUDY_TIMEZONE':'America/Bogota'}
            with duckdb.connect(str(database)) as db:
                db.execute('CREATE TABLE business.vault_current_versions (participant_hk VARCHAR,version_id VARCHAR,source_relation VARCHAR,delivery_sequence BIGINT)')
                db.execute('CREATE TABLE business.quality_current (source_name VARCHAR,record_source VARCHAR,participant_id VARCHAR,observation_date DATE,batch_id VARCHAR,record_id VARCHAR)')
                for participant,chrono in [('P001','A'),('P002','B')]:
                    profile,enrollment,assignment=[identity(label,participant) for label in ('profile','enrollment','assignment')]
                    insert(db,'vault.hub_participant',participant_hk=participant,business_namespace='fixture',participant_id=participant)
                    insert(db,'vault.sat_participant_profile',version_id=profile,parent_hk=participant,operation='upsert',chronotype=chrono)
                    insert(db,'vault.sat_enrollment',version_id=enrollment,parent_hk=participant,operation='upsert',age_at_enrollment=40,max_heart_rate=180)
                    insert(db,'vault.hub_device',device_hk=participant,device_id='DEVICE-'+participant,business_namespace='fixture')
                    insert(db,'vault.link_participant_device',participant_device_hk=participant,participant_hk=participant,device_hk=participant)
                    insert(db,'vault.sat_participant_device',version_id=assignment,parent_hk=participant,operation='upsert')
                    db.execute('INSERT INTO business.vault_current_versions VALUES (?,?,?,1),(?,?,?,1)',
                        [participant,enrollment,'vault.sat_enrollment',participant,assignment,'vault.sat_participant_device'])
                    insert(db,'marts.dim_participant',participant_sk=participant,participant_hk=participant,participant_id=participant,
                           business_namespace='fixture',chronotype=chrono,profile_version_id=profile,is_current=True)
                    insert(db,'marts.gold_participant_devices',participant_hk=participant,participant_id=participant,business_namespace='fixture',device_id='DEVICE-'+participant,assignment_quality_status='pending_metadata')
                    for day in ('2026-01-08','2026-01-09'):
                        insert(db,'marts.participant_day_current',study_hk=study,participant_hk=participant,local_date=day,
                            expected_minutes=1440,quality_status='accepted',warning_count=0,
                            input_manifest_hash='daily-one',rule_version='daily-rule',code_version='daily-code')
                        if participant=='P001':
                            heart=[60] if day.endswith('08') else [100,100]
                            for minute,bpm in enumerate(heart):
                                version=identity('heart',day,minute)
                                event=day+f'T05:0{minute}:00Z'
                                insert(db,'vault.sat_heart_rate_observation',version_id=version,parent_hk=participant,operation='upsert',study_hk=study,event_at_utc=event,heart_rate_bpm=bpm)
                                insert(db,'marts.fact_heart_rate_minute',study_hk=study,participant_hk=participant,study_sk='study',participant_sk=participant,
                                    date_sk=int(day.replace('-','')),event_at_utc=event,heart_rate_bpm=bpm,source_version_id=version,quality_status='accepted')
                            db.execute('''UPDATE marts.participant_day_current SET accepted_heart_rate_minutes=?,heart_rate_sum=?,heart_rate_mean=?,has_heart_rate=true
                                WHERE participant_hk=? AND local_date=?''',[len(heart),sum(heart),sum(heart)/len(heart),participant,day])
                for participant,duration,stages in [('P001',100,[('deep',0,100),('awake',100,110)]),
                                                    ('P002',50,[('light',0,30),('awake',30,40),('deep',40,60)])]:
                    from datetime import datetime,timedelta,timezone
                    start=datetime(2026,1,8,5,tzinfo=timezone.utc)
                    end=start+timedelta(minutes=100 if participant=='P001' else 25)
                    version=identity('sleep',participant)
                    insert(db,'vault.sat_sleep_session',version_id=version,parent_hk=participant,operation='upsert',sleep_onset_utc=start,sleep_end_utc=end,efficiency_pct_reported=100)
                    insert(db,'marts.fact_sleep_session',sleep_session_hk=participant,study_hk=study,participant_hk=participant,study_sk='study',participant_sk=participant,
                        date_sk=20260108,wake_date='2026-01-08',sleep_onset_utc=start,sleep_end_utc=end,reported_stage_sleep_min=duration,
                        reported_stage_deep_min=duration,deep_sleep_pct_from_reported_stages=100,efficiency_pct_reported=100,session_version_id=version,quality_status='accepted_with_warning')
                    for position,(stage,left,right) in enumerate(stages):
                        insert(db,'marts.fact_sleep_stage',session_version_id=version,stage_position=position,sleep_session_hk=participant,stage_type=stage,
                            start_at_utc=start+timedelta(minutes=left),end_at_utc=start+timedelta(minutes=right),duration_min_reported=right-left,
                            is_post_boundary_awake=stage=='awake')
                    db.execute('''UPDATE marts.participant_day_current SET has_sleep=true,sleep_onset_utc=?,sleep_end_utc=?,reported_stage_sleep_min=?,
                        reported_stage_deep_min=?,deep_sleep_pct_from_reported_stages=100,efficiency_pct_reported=100,warning_count=1,quality_status='accepted_with_warning'
                        WHERE participant_hk=? AND local_date='2026-01-08' ''',[start,end,duration,duration,participant])
                    db.execute("INSERT INTO business.quality_current VALUES ('sleep','fixture',?,'2026-01-08','batch',?)",[participant,participant])
                insert(db,'business.quality_issues',issue_id='boundary',batch_id='batch',record_id='P001',version_id=identity('sleep','P001'),rule_code='sleep_boundary_discrepancy',severity='warning',current_status='open')
                insert(db,'business.quality_issues',issue_id='internal-boundary',batch_id='batch',record_id='P002',version_id=identity('sleep','P002'),rule_code='sleep_boundary_discrepancy',severity='warning',current_status='open')
                insert(db,'business.daily_publications',study_hk=study,local_date='2026-01-08',input_manifest_hash='daily-one',rule_version='daily-rule',code_version='daily-code',status='published',published_at='2026-01-12T08:00:00Z')
                insert(db,'business.daily_publications',study_hk=study,local_date='2026-01-09',input_manifest_hash='daily-one',rule_version='daily-rule',code_version='daily-code',status='published',published_at='2026-01-12T08:00:00Z')
            receipts=[(day,1,'daily-one','daily-rule','daily-code','{}','{}') for day in ('2026-01-08','2026-01-09')]
            with patch.dict(os.environ,environment),patch('health_pipeline.weekly_publication.prerequisites',return_value=(study,['2026-01-08','2026-01-09'],receipts,'daily-one')):
                def fail(db):
                    raise RuntimeError('Injected publication failure')
                with self.assertRaisesRegex(RuntimeError,'Injected'):
                    freeze(scope,'request','fixture','expected',_before_commit=fail)
                publication=freeze(scope,'request','fixture','expected')
                self.assertEqual(freeze(scope,'request','fixture','expected'),publication)
                self.assertTrue(readiness(scope,'fixture','expected',registry,request_id='request'))
                with self.assertRaisesRegex(ValueError,'cannot change'):
                    freeze(scope,'request','fixture','wrong')
                with duckdb.connect(str(database)) as db:
                    cutoff=db.execute('SELECT correction_cutoff_at FROM publication_build.publications').fetchone()[0]
                    # A correction arrives after the freeze and must not change revision one.
                    db.execute("UPDATE marts.participant_day_current SET reported_stage_sleep_min=200 WHERE participant_hk='P001' AND local_date='2026-01-08'")
                    new_profile=identity('profile','P001','corrected')
                    insert(db,'vault.sat_participant_profile',version_id=new_profile,parent_hk='P001',operation='upsert',chronotype='C')
                    db.execute("UPDATE marts.dim_participant SET chronotype='C',profile_version_id=? WHERE participant_hk='P001'",[new_profile])
                with self.assertRaisesRegex(RuntimeError,'Injected'):
                    publish(publication,_before_commit=fail)
                with duckdb.connect(str(database)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM biostatistics.participant_week').fetchone()[0],0)
                    self.assertEqual(db.execute('SELECT count(*) FROM business.quality_resolutions').fetchone()[0],0)
                def crash():
                    raise RuntimeError('After commit')
                with self.assertRaisesRegex(RuntimeError,'After commit'):
                    publish(publication,_after_commit=crash)
                self.assertTrue(publish(publication)['reused'])
                with duckdb.connect(str(database)) as db:
                    self.assertEqual(db.execute('SELECT correction_cutoff_at FROM publication_build.publications').fetchone()[0],cutoff)
                    self.assertEqual(db.execute("SELECT reported_stage_sleep_min FROM biostatistics.participant_day WHERE participant_hk='P001' AND local_date='2026-01-08'").fetchone()[0],100)
                    self.assertEqual(db.execute("SELECT chronotype FROM biostatistics.participant_snapshot WHERE participant_hk='P001'").fetchone()[0],'A')
                    self.assertEqual(db.execute("SELECT count(*) FROM biostatistics.sleep_stage_exclusions").fetchone()[0],1)
                    self.assertEqual(db.execute("SELECT count(*) FROM biostatistics.sleep_stages WHERE stage_type='awake'").fetchone()[0],1)
                    self.assertEqual(db.execute("SELECT count(*) FROM marts.fact_sleep_stage WHERE stage_type='awake'").fetchone()[0],2)
                    metrics=db.execute("SELECT expected_days,partial_week,accepted_heart_rate_minutes,heart_rate_mean FROM biostatistics.participant_week WHERE participant_hk='P001'").fetchone()
                    self.assertEqual(metrics[:3],(2,True,3))
                    self.assertAlmostEqual(float(metrics[3]),260/3,places=5)
                    self.assertEqual(db.execute("SELECT current_status FROM business.quality_issues WHERE issue_id='boundary'").fetchone()[0],'resolved')
                    self.assertEqual(db.execute("SELECT current_status FROM business.quality_issues WHERE issue_id='internal-boundary'").fetchone()[0],'open')
                with sqlite3.connect(registry) as db:
                    self.assertEqual(db.execute('SELECT status FROM publication_registry').fetchone()[0],'published')
                with duckdb.connect(str(database)) as db:
                    start=datetime(2026,1,8,5,tzinfo=timezone.utc)
                    corrected=identity('sleep','P001','corrected')
                    end=start+timedelta(minutes=200)
                    insert(db,'vault.sat_sleep_session',version_id=corrected,parent_hk='P001',operation='upsert',sleep_onset_utc=start,sleep_end_utc=end,efficiency_pct_reported=100)
                    db.execute("DELETE FROM marts.fact_sleep_stage WHERE sleep_session_hk='P001'")
                    insert(db,'marts.fact_sleep_stage',session_version_id=corrected,stage_position=0,sleep_session_hk='P001',stage_type='deep',start_at_utc=start,end_at_utc=end,duration_min_reported=200,is_post_boundary_awake=False)
                    db.execute("UPDATE marts.fact_sleep_session SET session_version_id=?,sleep_end_utc=?,reported_stage_sleep_min=200,reported_stage_deep_min=200 WHERE participant_hk='P001'",[corrected,end])
                    db.execute("UPDATE marts.participant_day_current SET sleep_end_utc=?,reported_stage_deep_min=200,warning_count=0,quality_status='accepted' WHERE participant_hk='P001' AND local_date='2026-01-08'",[end])
                    db.execute("UPDATE business.quality_current SET record_id='corrected' WHERE participant_id='P001'")
                with patch('health_pipeline.weekly_publication.prerequisites',return_value=(study,['2026-01-08','2026-01-09'],receipts,'daily-two')):
                    next_publication=freeze(scope,'next-request','fixture','expected')
                    publish(next_publication)
                with duckdb.connect(str(database)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM biostatistics.publications').fetchone()[0],2)
                    self.assertEqual(db.execute("SELECT sleep_minutes_sum FROM biostatistics.latest_participant_week WHERE participant_hk='P001'").fetchone()[0],200)
                    self.assertEqual(db.execute('SELECT count(*) FROM biostatistics.latest_participant_week').fetchone()[0],2)
                    self.assertEqual(db.execute('SELECT count(*) FROM biostatistics.sleep_stage_exclusions WHERE publication_id=?',[next_publication]).fetchone()[0],0)
                manifest=root/'publications'/publication/'profile.jsonl.gz'
                original=manifest.read_bytes()
                manifest.write_bytes(original+b'changed')
                with self.assertRaisesRegex(ValueError,'manifest cannot change'):
                    publish(publication)
                manifest.write_bytes(original)
                self.assertTrue(publish(publication)['reused'])

    def test_missing_delivery_and_partial_study_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            registry=Path(directory)/'registry.sqlite'
            with sqlite3.connect(registry) as db:
                db.executescript((DDL/'sqlite'/'001_control_registry.sql').read_text())
            scope=weekly_scope(week_start='2026-02-02')
            study,days=required_days(scope)
            self.assertEqual(days,['2026-02-02','2026-02-03','2026-02-04'])
            self.assertFalse(readiness(scope,'source','expected',registry))
            with sqlite3.connect(registry) as db:
                for day in days:
                    coverage={'validation_status':'passed','empty_cohort':False,
                        'deliveries':[{'source_system':'source','delivery_id':'expected','target_dates':days}]}
                    db.execute('INSERT INTO daily_readiness VALUES (?,?,1,?,?,?,?,?,?)',[study,day,'input','rule','code','2026-02-09T08:00:00Z',json.dumps(coverage),'{}'])
            self.assertTrue(readiness(scope,'source','expected',registry))
            self.assertFalse(readiness(scope,'source','different-delivery',registry))


if __name__=='__main__':
    unittest.main()
