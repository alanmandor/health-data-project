"""Freeze weekly input versions and atomically publish revisioned biostatistics."""

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
from zoneinfo import ZoneInfo

from health_pipeline.control import weekly_scope, utc_timestamp
from health_pipeline.daily_loading import current_mart, eligible_deliveries
from health_pipeline.dbt_quality import project_hash
from health_pipeline.mart_loading import connect
from health_pipeline.vault_loading import identity
from health_pipeline.weekly_controls import readiness, required_days

RULE_VERSION = 'weekly-consolidation-v1'
SQL = Path(__file__).parent/'sql'/'freeze_week.sql'


def implementation():
    quality = Path(os.getenv('PROJECT_DBT_DIR','/opt/airflow/project/dbt'))
    project = Path(os.getenv('PROJECT_WEEKLY_DBT_DIR',str(quality.parent/'dbt_weekly')))
    return project,identity('weekly_implementation',project_hash(project),
        hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),hashlib.sha256(SQL.read_bytes()).hexdigest(),
        hashlib.sha256(Path(__file__).with_name('weekly_controls.py').read_bytes()).hexdigest())


def prerequisites(db, scope, source, delivery, registry):
    study,days = required_days(scope)
    if not readiness(scope,source,delivery,registry):
        raise ValueError('Required daily readiness or the expected final delivery is unavailable')
    runtime = Path(os.getenv('PROJECT_DBT_RUNTIME','/opt/airflow/project-data/dbt'))
    validation = json.loads((runtime/'validated.json').read_text())
    quality = Path(os.getenv('PROJECT_DBT_DIR','/opt/airflow/project/dbt'))
    if validation['database']!=str(Path(os.environ['PROJECT_DATABASE_PATH']).resolve()) or validation['code_version']!=project_hash(quality):
        raise ValueError('Weekly freeze requires current validated quality code and database')
    mart = current_mart(db,validation,quality)
    deliveries = eligible_deliveries(db,registry,validation)
    digest = identity('daily_inputs',validation['input_hash'],mart,deliveries)
    daily_project = Path(os.getenv('PROJECT_DAILY_DBT_DIR',str(quality.parent/'dbt_daily')))
    daily_module = Path(__file__).with_name('daily_loading.py')
    daily_code = identity('daily_implementation',project_hash(daily_project),hashlib.sha256(daily_module.read_bytes()).hexdigest())
    latest = db.execute('''SELECT mart_evaluation_id,implementation_hash,input_manifest_hash
        FROM business.daily_evaluations ORDER BY committed_at DESC LIMIT 1''').fetchone()
    if latest!=(mart,daily_code,digest):
        raise ValueError('Daily analytics are stale; run the daily pipeline before freezing')
    receipts = []
    with sqlite3.connect(registry) as control:
        for day in days:
            p = db.execute('''SELECT input_manifest_hash,rule_version,code_version FROM business.daily_publications
                WHERE study_hk=? AND local_date=? AND status='published' AND published_at IS NOT NULL''',[study,day]).fetchone()
            r = control.execute('''SELECT revision,input_manifest_hash,rule_version,code_version,coverage_json,warning_counts_json
                FROM daily_readiness WHERE study_hk=? AND target_date=? ORDER BY revision DESC LIMIT 1''',[study,day]).fetchone()
            if p is None or p!=(digest,validation['quality_rule_version']+'/daily-aggregation-v1',daily_code) or r is None or r[1:4]!=p:
                raise ValueError('A required day has no matching current readiness fingerprint')
            receipts.append((day,*r))
    cohort = db.execute('SELECT count(*) FROM marts.gold_participant_devices').fetchone()[0]
    count = db.execute('SELECT count(*) FROM analytics.participant_day WHERE study_hk=? AND local_date BETWEEN ? AND ?',
                       [study,days[0],days[-1]]).fetchone()[0]
    if not cohort or count!=cohort*len(days):
        raise ValueError('Weekly cohort/day spine is incomplete or empty')
    return study,days,receipts,digest


def sync_registry(db, publication, registry):
    """Replay metadata and compact manifest pointers after the warehouse commit."""
    row = db.execute('SELECT * FROM publication_build.publications WHERE publication_id=?',[publication]).fetchone()
    if row is None:
        raise ValueError('Unknown publication')
    values = [utc_timestamp(v) if isinstance(v,datetime) else v.isoformat() if hasattr(v,'isoformat') else v for v in row]
    root = Path(os.getenv('PROJECT_PUBLICATION_DIR',str(Path(os.environ['PROJECT_DATABASE_PATH']).parent/'publications')))/publication
    root.mkdir(parents=True,exist_ok=True)
    manifests = []
    kinds = db.execute('SELECT DISTINCT input_kind FROM publication_build.inputs WHERE publication_id=? ORDER BY input_kind',[publication]).fetchall()
    for (kind,) in kinds:
        destination = root/(kind+'.jsonl.gz')
        if not destination.exists():
            temporary = destination.with_suffix('.tmp')
            cursor = db.execute('''SELECT version_id,source_relation,business_key_hash,payload_hash
                FROM publication_build.inputs WHERE publication_id=? AND input_kind=? ORDER BY version_id''',[publication,kind])
            try:
                with temporary.open('wb') as output, gzip.GzipFile(fileobj=output,mode='wb',mtime=0,compresslevel=1) as compressed:
                    while chunk := cursor.fetchmany(1000):
                        for record in chunk:
                            compressed.write((json.dumps(record,separators=(',',':'))+'\n').encode())
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
        with destination.open('rb') as stream:
            digest = hashlib.file_digest(stream,'sha256').hexdigest()
        manifests.append((kind,digest,'publication_build.inputs',identity('publication_input_manifest',publication,kind),digest))
    with sqlite3.connect(registry,timeout=30) as control:
        control.execute('BEGIN IMMEDIATE')
        old = control.execute('SELECT input_manifest_hash,correction_cutoff_at FROM publication_registry WHERE publication_id=?',[publication]).fetchone()
        if old and old!=(values[11],values[7]):
            raise ValueError('An existing publication registry identity cannot change')
        control.execute('''INSERT INTO publication_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(publication_id) DO UPDATE SET published_at=excluded.published_at,status=excluded.status''',values)
        for manifest in manifests:
            existing=control.execute('SELECT version_id FROM publication_inputs WHERE publication_id=? AND input_kind=?',
                                     [publication,manifest[0]]).fetchone()
            if existing and existing[0]!=manifest[1]:
                raise ValueError('A frozen compressed input manifest cannot change')
            control.execute('INSERT OR IGNORE INTO publication_inputs VALUES (?,?,?,?,?,?)',[publication,*manifest])
        for request,binding in db.execute('SELECT request_id,scope_json::VARCHAR FROM publication_build.requests WHERE publication_id=?',[publication]).fetchall():
            scope=json.loads(binding)
            scope.update(study_hk=values[1],publication_id=publication)
            control.execute('''INSERT INTO model_checkpoints VALUES (?, 'weekly_freeze',?,?,?,?,?,?)
                ON CONFLICT(checkpoint_id) DO UPDATE SET status=excluded.status''',
                [identity('weekly_request',request),json.dumps(scope,sort_keys=True),values[11],values[12],values[13],values[10],values[7]])


def freeze(scope, request_id, expected_source_system, expected_delivery_id, scheduled_run_at=None, _before_commit=None):
    if not request_id or not expected_source_system or not expected_delivery_id:
        raise ValueError('A stable request identity and expected delivery identity are required')
    if scheduled_run_at is not None:
        scheduled_run_at=utc_timestamp(datetime.fromisoformat(scheduled_run_at.replace('Z','+00:00')))
    _,code = implementation()
    registry = Path(os.environ['PROJECT_REGISTRY_PATH'])
    binding = json.dumps({'scope':scope,'expected_source_system':expected_source_system,
                         'expected_delivery_id':expected_delivery_id,'scheduled_run_at':scheduled_run_at},sort_keys=True)
    with connect(os.environ['PROJECT_DATABASE_PATH']) as db:
        previous = db.execute('SELECT publication_id,scope_json::VARCHAR FROM publication_build.requests WHERE request_id=?',[request_id]).fetchone()
        if previous:
            if json.loads(previous[1])!=json.loads(binding):
                raise ValueError('A frozen request cannot change its scope or delivery identity')
            sync_registry(db,previous[0],registry)
            return previous[0]
        db.execute('BEGIN')
        try:
            study,days,receipts,daily_digest = prerequisites(db,scope,expected_source_system,expected_delivery_id,registry)
            digest = identity('weekly_inputs',study,scope['week_start'],daily_digest,receipts)
            old = db.execute('''SELECT publication_id FROM publication_build.publications
                WHERE study_hk=? AND week_start=? AND input_manifest_hash=? AND code_version=?
                ORDER BY revision DESC LIMIT 1''',[study,scope['week_start'],digest,code]).fetchone()
            if old:
                publication=old[0]
            else:
                revision = db.execute('SELECT coalesce(max(revision),0)+1 FROM publication_build.publications WHERE study_hk=? AND week_start=?',[study,scope['week_start']]).fetchone()[0]
                publication = identity('weekly_publication',study,scope['week_start'],revision,digest)
                cutoff = datetime.now(timezone.utc)
                db.execute("INSERT INTO publication_build.publications VALUES (?,?,?,?,?,?,?,?,?,?, 'building',?,?,?)",
                    [publication,study,scope['week_start'],revision,scope['event_period_start'],scope['event_period_end'],scheduled_run_at,
                     cutoff,cutoff.astimezone(ZoneInfo(scope['study_timezone'])).date(),None,digest,RULE_VERSION,code])
                db.execute('CREATE TEMP TABLE freeze_context(publication_id VARCHAR,study_hk VARCHAR,first_day DATE,last_day DATE)')
                db.execute('INSERT INTO freeze_context VALUES (?,?,?,?)',[publication,study,days[0],days[-1]])
                db.execute(SQL.read_text())
                for day,*receipt in receipts:
                    db.execute('INSERT INTO publication_build.readiness VALUES (?,?,?,?,?,?,?::JSON,?::JSON)',[publication,day,*receipt])
                for kind,table,satellite,date_field,version_field in [
                    ('steps','fact_steps_minute','sat_steps_observation',None,'source_version_id'),
                    ('heart_rate','fact_heart_rate_minute','sat_heart_rate_observation',None,'source_version_id'),
                    ('sleep','fact_sleep_session','sat_sleep_session','wake_date','session_version_id'),
                    ('wellness','fact_wellness_daily','sat_wellness_response','survey_date','response_version_id')]:
                    day_expr='f.'+date_field if date_field else "timezone(?,f.event_at_utc)::DATE"
                    key_expr='f.'+date_field if date_field else 'f.event_at_utc'
                    date_params=[] if date_field else [scope['study_timezone']]
                    for prefix in ('0123456789abcdef' if kind in ('steps','heart_rate') else ['']):
                        db.execute(f'''INSERT INTO publication_build.inputs SELECT ?,?,s.version_id,?,
                            sha256(to_json(list_value(?,f.study_hk,f.participant_hk,cast({key_expr} AS VARCHAR)))),s.payload_hash
                            FROM marts.{table} f JOIN vault.{satellite} s ON s.version_id=f.{version_field}
                            WHERE f.study_hk=? AND {day_expr} BETWEEN ? AND ? AND starts_with(s.version_id,?)''',
                            [publication,kind,'vault.'+satellite,kind,study,*date_params,days[0],days[-1],prefix])
                snapshots=db.execute('SELECT count(*) FROM publication_build.participant_snapshot WHERE publication_id=?',[publication]).fetchone()[0]
                if snapshots*len(days)!=db.execute('SELECT count(*) FROM publication_build.participant_day WHERE publication_id=?',[publication]).fetchone()[0]:
                    raise AssertionError('Frozen cohort context does not reconcile with daily coverage')
                if db.execute('''SELECT 1 FROM publication_build.participant_snapshot WHERE publication_id=?
                    AND (profile_version_id IS NULL OR enrollment_version_id IS NULL) LIMIT 1''',[publication]).fetchone():
                    raise AssertionError('Frozen cohort lacks authoritative profile or enrollment versions')
                for kind,column in [('steps','accepted_steps_minutes'),('heart_rate','accepted_heart_rate_minutes'),
                                    ('sleep','has_sleep'),('wellness','has_wellness')]:
                    expected=db.execute(f'SELECT coalesce(sum({column}::BIGINT),0) FROM publication_build.participant_day WHERE publication_id=?',[publication]).fetchone()[0]
                    selected=db.execute('SELECT count(*) FROM publication_build.inputs WHERE publication_id=? AND input_kind=?',[publication,kind]).fetchone()[0]
                    if expected!=selected:
                        raise AssertionError('Frozen accepted versions do not reconcile: '+kind)
            db.execute('INSERT INTO publication_build.requests VALUES (?,?,?::JSON)',[request_id,publication,binding])
            if _before_commit:
                _before_commit(db)
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
        sync_registry(db,publication,registry)
    return publication


def publish(publication, _before_commit=None, _after_commit=None):
    if not re.fullmatch('[a-f0-9]{64}',publication):
        raise ValueError('Expected a canonical publication identifier')
    project,code=implementation()
    registry=Path(os.environ['PROJECT_REGISTRY_PATH'])
    with connect(os.environ['PROJECT_DATABASE_PATH']) as db:
        row=db.execute('SELECT status,code_version FROM publication_build.publications WHERE publication_id=?',[publication]).fetchone()
        if row is None:
            raise ValueError('Freeze the publication before building it')
        if row[0]=='published':
            sync_registry(db,publication,registry)
            return {'publication_id':publication,'status':'published','reused':True}
        if row[1]!=code:
            raise ValueError('Frozen publication code changed; retry with its original code or create a new request')
    runtime=Path(os.getenv('PROJECT_DBT_RUNTIME','/opt/airflow/project-data/dbt'))
    runtime=runtime.parent/(runtime.name+'-weekly')
    profiles=runtime/'profiles'
    profiles.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(project/'profiles.example.yml',profiles/'profiles.yml')
    subprocess.run([os.getenv('DBT_EXECUTABLE','/opt/airflow/dbt-venv/bin/dbt'),'run','--project-dir',str(project),
        '--profiles-dir',str(profiles),'--vars',json.dumps({'publication_id':publication}),
        '--target-path',str(runtime/'target'),'--log-path',str(runtime/'logs')],check=True)
    with connect(os.environ['PROJECT_DATABASE_PATH']) as db:
        db.execute('BEGIN')
        try:
            expected=db.execute('SELECT count(*) FROM publication_build.participant_snapshot WHERE publication_id=?',[publication]).fetchone()[0]
            if expected==0 or db.execute('SELECT count(*) FROM business.weekly_candidates WHERE publication_id=?',[publication]).fetchone()[0]!=expected:
                raise AssertionError('Weekly candidates do not reconcile with the frozen cohort')
            for count,total in [('accepted_steps_minutes','observed_steps_total'),('accepted_heart_rate_minutes','heart_rate_sum')]:
                if db.execute(f'SELECT sum({count}),sum({total}) FROM business.weekly_candidates').fetchone()!=db.execute(f'SELECT sum({count}),sum({total}) FROM publication_build.participant_day WHERE publication_id=?',[publication]).fetchone():
                    raise AssertionError('Weekly metric reconciliation failed')
            if db.execute('''SELECT 1 FROM business.weekly_candidates WHERE accepted_heart_rate_minutes=0
                AND heart_rate_mean IS NOT NULL OR accepted_heart_rate_minutes>0
                AND abs(heart_rate_mean-heart_rate_sum::DOUBLE/accepted_heart_rate_minutes)>0.000001 LIMIT 1''').fetchone():
                raise AssertionError('Weekly heart rate denominators are invalid')
            db.execute('INSERT INTO publication_build.participant_week SELECT * FROM business.weekly_candidates')
            db.execute('''INSERT INTO publication_build.sleep_stages SELECT t.* FROM publication_build.sleep_stage_candidates t
                WHERE t.publication_id=? AND NOT EXISTS(SELECT 1 FROM publication_build.sleep_stage_exclusions e
                WHERE e.publication_id=t.publication_id AND e.session_version_id=t.session_version_id AND e.stage_position=t.stage_position)''',[publication])
            db.execute('''INSERT INTO business.quality_resolutions SELECT DISTINCT e.resolution_id,e.issue_id,now(),
                'excluded_at_consolidation',NULL,e.publication_id FROM publication_build.sleep_stage_exclusions e
                WHERE e.publication_id=? AND NOT EXISTS(SELECT 1 FROM business.quality_resolutions r WHERE r.resolution_id=e.resolution_id)''',[publication])
            # Close the broad boundary alert only if the retained stages clear it completely.
            db.execute('''UPDATE business.quality_issues i SET current_status='resolved' WHERE EXISTS(
                SELECT 1 FROM publication_build.sleep_stage_exclusions e JOIN vault.sat_sleep_session s ON s.version_id=e.session_version_id
                WHERE e.publication_id=? AND e.issue_id=i.issue_id AND EXISTS(
                    SELECT 1 FROM publication_build.sleep_stages t WHERE t.publication_id=e.publication_id
                    AND t.session_version_id=e.session_version_id GROUP BY t.session_version_id
                    HAVING min(t.start_at_utc)=s.sleep_onset_utc
                    AND max(t.end_at_utc) FILTER(WHERE t.stage_type<>'awake')=s.sleep_end_utc
                    AND NOT bool_or(t.start_at_utc<s.sleep_onset_utc OR t.end_at_utc>s.sleep_end_utc)))''',[publication])
            db.execute("UPDATE publication_build.publications SET status='published',published_at=now() WHERE publication_id=?",[publication])
            if _before_commit:
                _before_commit(db)
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
        if _after_commit:
            _after_commit()
        sync_registry(db,publication,registry)
    return {'publication_id':publication,'status':'published','reused':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['freeze','publish','run'])
    parser.add_argument('--week-start')
    parser.add_argument('--request-id')
    parser.add_argument('--expected-source-system')
    parser.add_argument('--expected-delivery-id')
    parser.add_argument('--scheduled-run-at')
    parser.add_argument('--publication-id')
    args=parser.parse_args()
    if args.action=='publish':
        result=publish(args.publication_id or '')
    else:
        scope=weekly_scope(week_start=args.week_start,study_timezone=os.getenv('STUDY_TIMEZONE','America/Bogota'))
        publication=freeze(scope,args.request_id,args.expected_source_system,args.expected_delivery_id,args.scheduled_run_at)
        result={'publication_id':publication,'status':'frozen'} if args.action=='freeze' else publish(publication)
    print(json.dumps(result))


if __name__=='__main__':
    main()
