"""Validate daily aggregates, publish analytics, and replay durable readiness receipts."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

from health_pipeline.control import validate_manifest
from health_pipeline.dbt_quality import project_hash
from health_pipeline.mart_loading import connect, latest_impact
from health_pipeline.raw_ingestion import identity as batch_identity
from health_pipeline.vault_loading import identity

RULE_VERSION = 'daily-aggregation-v1'


def current_mart(db, validation, quality_project):
    impact = latest_impact(db, validation)
    project = Path(os.getenv('PROJECT_MART_DBT_DIR', str(quality_project.parent / 'dbt_marts')))
    module = Path(__file__).with_name('mart_loading.py')
    implementation = identity('mart_implementation', project_hash(project), hashlib.sha256(module.read_bytes()).hexdigest())
    evaluation = identity('mart_evaluation', impact, implementation)
    if not db.execute('SELECT 1 FROM business.mart_evaluations WHERE evaluation_id=?', [evaluation]).fetchone():
        raise ValueError('Commit matching dimensional marts before daily aggregation')
    if db.execute('SELECT 1 FROM business.affected_participant_dates WHERE consumed_checkpoint_id IS NULL LIMIT 1').fetchone():
        raise ValueError('Unconsumed dimensional work prevents publication')
    return evaluation


def eligible_deliveries(db, registry, validation):
    """Admit complete declarations whose files have committed raw load receipts."""
    accepted = []
    with sqlite3.connect(registry) as control:
        deliveries = control.execute('SELECT source_system,delivery_id,manifest_hash,manifest_json FROM deliveries ORDER BY source_system,sequence').fetchall()
        for source, delivery, digest, serialized in deliveries:
            manifest = json.loads(serialized)
            validate_manifest(manifest, manifest['target_dates'][0], validation['study_timezone'])
            files = control.execute('SELECT file_id,verification_status FROM delivery_files WHERE source_system=? AND delivery_id=?', [source,delivery]).fetchall()
            declared = sum(len(s['files']) for s in manifest['sources'].values())
            if len(files) != declared or any(status not in ('raw_loaded','archived') for _, status in files):
                continue
            if any(not db.execute('SELECT 1 FROM raw.file_loads WHERE file_id=?', [file]).fetchone() for file, _ in files):
                continue
            # A reused raw file must still have this delivery's lineage and precedence.
            batch = batch_identity(source, delivery)
            loaded = db.execute('SELECT count(*) FROM raw.batch_records WHERE batch_id=?', [batch]).fetchone()[0]
            expected = sum(db.execute('SELECT record_count FROM raw.file_loads WHERE file_id=?', [file]).fetchone()[0] for file in {f for f, _ in files})
            if loaded != expected:
                continue
            accepted.append({'source_system':source, 'delivery_id':delivery, 'manifest_hash':digest,
                             'target_dates':manifest['target_dates']})
    return accepted


def flush_readiness(db, registry):
    """DuckDB commits first; SQLite retries never advertise an uncommitted result."""
    receipts = db.execute('''SELECT o.study_hk, o.local_date, o.input_manifest_hash, o.rule_version,
        o.code_version, o.validated_at, o.coverage_json, o.warning_counts_json
        FROM business.daily_readiness_outbox o JOIN business.daily_publications p
        ON p.study_hk=o.study_hk AND p.local_date=o.local_date
        AND p.input_manifest_hash=o.input_manifest_hash AND p.rule_version=o.rule_version AND p.code_version=o.code_version
        WHERE p.status='published' AND p.published_at IS NOT NULL
        QUALIFY row_number() OVER(PARTITION BY o.study_hk,o.local_date ORDER BY o.validated_at DESC)=1''').fetchall()
    with sqlite3.connect(registry, timeout=30) as control:
        control.execute('BEGIN IMMEDIATE')
        for study, day, digest, rule, code, timestamp, coverage, warnings in receipts:
            day = day.isoformat()
            latest = control.execute('''SELECT input_manifest_hash,rule_version,code_version FROM daily_readiness
                WHERE study_hk=? AND target_date=? ORDER BY revision DESC LIMIT 1''', [study,day]).fetchone()
            if latest == (digest,rule,code):
                continue
            revision = control.execute('SELECT coalesce(max(revision),0)+1 FROM daily_readiness WHERE study_hk=? AND target_date=?', [study,day]).fetchone()[0]
            control.execute('INSERT INTO daily_readiness VALUES (?,?,?,?,?,?,?,?,?)',
                [study,day,revision,digest,rule,code,timestamp.isoformat(),coverage,warnings])
    return len(receipts)


def validate_candidates(db):
    if db.execute('''SELECT 1 FROM business.daily_candidates GROUP BY study_hk,participant_hk,local_date
        HAVING count(*)<>1 LIMIT 1''').fetchone():
        raise AssertionError('Duplicate daily grain')
    expected = db.execute('SELECT count(*) FROM business.participant_date_state').fetchone()[0]
    if db.execute('SELECT count(*) FROM business.daily_candidates').fetchone()[0] != expected:
        raise AssertionError('Daily spine reconciliation failed')
    if db.execute('''SELECT study_hk,participant_hk,local_date FROM business.daily_candidates
        EXCEPT SELECT study_hk,participant_hk,local_date FROM business.participant_date_state''').fetchone():
        raise AssertionError('Daily spine keys do not match current participants and dates')
    if db.execute('''SELECT 1 FROM business.daily_candidates WHERE
        (accepted_steps_minutes=0 AND observed_steps_total IS NOT NULL)
        OR (accepted_heart_rate_minutes=0 AND (heart_rate_sum IS NOT NULL OR heart_rate_mean IS NOT NULL))
        OR has_steps<>(accepted_steps_minutes>0) OR has_heart_rate<>(accepted_heart_rate_minutes>0)
        OR (expected_minutes>0 AND (accepted_steps_minutes>expected_minutes OR accepted_heart_rate_minutes>expected_minutes))
        OR (has_heart_rate AND heart_rate_mean NOT BETWEEN 40 AND 200) LIMIT 1''').fetchone():
        raise AssertionError('Daily coverage or measurement invariants failed')
    for fact, count, total, value in [('fact_steps_minute','accepted_steps_minutes','observed_steps_total','steps_per_minute'),
                                      ('fact_heart_rate_minute','accepted_heart_rate_minutes','heart_rate_sum','heart_rate_bpm')]:
        actual = db.execute(f'SELECT sum({count}),sum({total}) FROM business.daily_candidates').fetchone()
        expected = db.execute(f'SELECT count(*),sum({value}) FROM marts.{fact}').fetchone()
        if (actual[0] or 0, actual[1]) != expected:
            raise AssertionError('Daily minute reconciliation failed: '+fact)
        if db.execute(f'''WITH observations AS (
            SELECT study_hk,participant_hk,date_sk,count(*) AS n,sum({value}) AS total
            FROM marts.{fact} GROUP BY ALL)
            SELECT 1 FROM observations f LEFT JOIN business.daily_candidates d
            ON d.study_hk=f.study_hk AND d.participant_hk=f.participant_hk
            AND strftime(d.local_date,'%Y%m%d')::INTEGER=f.date_sk
            WHERE d.{count} IS DISTINCT FROM f.n OR d.{total} IS DISTINCT FROM f.total LIMIT 1''').fetchone():
            raise AssertionError('Daily participant/date reconciliation failed: '+fact)
    for fact,flag in [('fact_sleep_session','has_sleep'),('fact_wellness_daily','has_wellness')]:
        actual = db.execute(f'SELECT count(*) FROM business.daily_candidates WHERE {flag}').fetchone()[0]
        expected = db.execute(f'SELECT count(*) FROM marts.{fact}').fetchone()[0]
        if actual != expected:
            raise AssertionError('Daily response reconciliation failed: '+fact)


def load(_before_commit=None, _after_commit=None):
    database = Path(os.environ['PROJECT_DATABASE_PATH']).resolve()
    registry = Path(os.environ['PROJECT_REGISTRY_PATH'])
    quality_runtime = Path(os.getenv('PROJECT_DBT_RUNTIME','/opt/airflow/project-data/dbt'))
    validation = json.loads((quality_runtime / 'validated.json').read_text())
    quality_project = Path(os.getenv('PROJECT_DBT_DIR','/opt/airflow/project/dbt'))
    if validation['database'] != str(database) or validation['code_version'] != project_hash(quality_project):
        raise ValueError('Validated quality code or database does not match')
    project = Path(os.getenv('PROJECT_DAILY_DBT_DIR',str(quality_project.parent / 'dbt_daily')))
    implementation = identity('daily_implementation', project_hash(project), hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    rule = validation['quality_rule_version'] + '/' + RULE_VERSION
    with connect(database) as db:
        mart = current_mart(db,validation,quality_project)
        deliveries = eligible_deliveries(db,registry,validation)
        digest = identity('daily_inputs',validation['input_hash'],mart,deliveries)
        prior = db.execute('''SELECT evaluation_id,mart_evaluation_id,implementation_hash,input_manifest_hash
            FROM business.daily_evaluations ORDER BY committed_at DESC LIMIT 1''').fetchone()
        if prior and prior[1:] == (mart,implementation,digest):
            return {'status':'daily_published','reused':True,'ready_dates':flush_readiness(db,registry)}
        evaluation = identity('daily_evaluation',mart,implementation,digest,prior[0] if prior else None)
    runtime = quality_runtime.parent / (quality_runtime.name+'-daily')
    profiles = runtime / 'profiles'
    profiles.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(project / 'profiles.example.yml',profiles / 'profiles.yml')
    subprocess.run([os.getenv('DBT_EXECUTABLE','/opt/airflow/dbt-venv/bin/dbt'),'run','--project-dir',str(project),
        '--profiles-dir',str(profiles),'--target-path',str(runtime/'target'),'--log-path',str(runtime/'logs')],check=True)
    with connect(database) as db:
        if current_mart(db,validation,quality_project)!=mart or eligible_deliveries(db,registry,validation)!=deliveries:
            raise ValueError('Daily inputs changed during preparation')
        db.execute('BEGIN')
        try:
            validate_candidates(db)
            db.execute('DELETE FROM marts.participant_day_current')
            db.execute('''INSERT INTO marts.participant_day_current SELECT *,?,?,?,now()
                FROM business.daily_candidates''',[digest,rule,implementation])
            db.execute('DELETE FROM business.daily_publications')
            days = sorted({day for d in deliveries for day in d['target_dates']})
            for study, start, end in db.execute('SELECT study_hk,study_start,study_end_exclusive FROM marts.dim_study').fetchall():
                for day in days:
                    if not start.isoformat()<=day<end.isoformat():
                        continue
                    db.execute("INSERT INTO business.daily_publications VALUES (?,?,?,?,?,'published',now())",[study,day,digest,rule,implementation])
                    coverage = db.execute('''SELECT count(*),coalesce(sum(expected_minutes),0),
                        coalesce(sum(accepted_steps_minutes),0),coalesce(sum(accepted_heart_rate_minutes),0),
                        count(*) FILTER(WHERE NOT has_steps),count(*) FILTER(WHERE NOT has_heart_rate),
                        count(*) FILTER(WHERE NOT has_sleep),count(*) FILTER(WHERE NOT has_wellness)
                        FROM business.daily_candidates WHERE study_hk=? AND local_date=?''',[study,day]).fetchone()
                    names = ['participants','expected_minutes','accepted_steps_minutes','accepted_heart_rate_minutes',
                             'participants_without_steps','participants_without_heart_rate','participants_without_sleep','participants_without_wellness']
                    details = dict(zip(names,coverage))
                    details.update({'mart_evaluation_id':mart,'validation_status':'passed','deliveries':deliveries,
                                    'empty_cohort':coverage[0]==0})
                    warnings = db.execute('SELECT coalesce(sum(warning_count),0) FROM business.daily_candidates WHERE study_hk=? AND local_date=?',[study,day]).fetchone()[0]
                    db.execute('INSERT INTO business.daily_readiness_outbox VALUES (?,?,?,?,?,?,now(),?::JSON,?::JSON)',
                        [evaluation,study,day,digest,rule,implementation,json.dumps(details),json.dumps({'accepted_warning_flags':warnings})])
            db.execute('INSERT INTO business.daily_evaluations VALUES (?,?,?,?,now())',[evaluation,mart,implementation,digest])
            if _before_commit:
                _before_commit(db)
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
        if _after_commit:
            _after_commit()
        ready = flush_readiness(db,registry)
    return {'status':'daily_published','reused':False,'ready_dates':ready}


if __name__=='__main__':
    print(json.dumps(load()))
