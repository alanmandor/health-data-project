"""Build dbt candidates and atomically apply dimensional marts for captured dates."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

from health_pipeline.dbt_quality import input_hash, project_hash
from health_pipeline.vault_loading import identity


def latest_impact(db, validation):
    row = db.execute('''SELECT i.evaluation_id FROM business.impact_evaluations i
        JOIN business.vault_loads v ON v.evaluation_id=i.vault_evaluation_id
        WHERE v.input_hash=? AND v.quality_code_version=? ORDER BY i.committed_at DESC LIMIT 1''',
        [validation['input_hash'], validation['code_version']]).fetchone()
    if row is None or input_hash(db) != validation['input_hash']:
        raise ValueError('Commit matching validated impact inputs before building marts')
    return row[0]


def connect(database):
    import duckdb
    db = duckdb.connect(str(database))
    db.execute("SET TimeZone='UTC'")
    db.execute('SET threads=1')
    db.execute('SET memory_limit=?', [os.getenv('VAULT_MEMORY_LIMIT', '4GB')])
    db.execute('SET preserve_insertion_order=false')
    return db


def load(_before_commit=None):
    import duckdb
    database = Path(os.environ['PROJECT_DATABASE_PATH']).resolve()
    quality_runtime = Path(os.getenv('PROJECT_DBT_RUNTIME', '/opt/airflow/project-data/dbt'))
    validation = json.loads((quality_runtime / 'validated.json').read_text())
    quality_project = Path(os.getenv('PROJECT_DBT_DIR', '/opt/airflow/project/dbt'))
    if validation['database'] != str(database) or validation['code_version'] != project_hash(quality_project):
        raise ValueError('Validated quality code or database does not match')
    project = Path(os.getenv('PROJECT_MART_DBT_DIR', str(quality_project.parent / 'dbt_marts')))
    runtime = quality_runtime.parent / (quality_runtime.name + '-marts')
    profiles = runtime / 'profiles'
    profiles.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(project / 'profiles.example.yml', profiles / 'profiles.yml')
    implementation = identity('mart_implementation', project_hash(project), hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    with connect(database) as db:
        impact = latest_impact(db, validation)
        evaluation = identity('mart_evaluation', impact, implementation)
        old = db.execute('SELECT affected_date_count FROM business.mart_evaluations WHERE evaluation_id=?', [evaluation]).fetchone()
        if old:
            return {'status': 'marts_loaded', 'reused': True, 'affected_date_count': old[0], 'consumer_published': False}
        prior = db.execute('SELECT implementation_hash FROM business.mart_evaluations ORDER BY committed_at DESC LIMIT 1').fetchone()
        db.execute('BEGIN')
        db.execute('DELETE FROM business.mart_build_scope')
        db.execute('''INSERT INTO business.mart_build_scope SELECT DISTINCT study_hk, participant_hk, local_date
            FROM business.affected_participant_dates WHERE consumed_checkpoint_id IS NULL''')
        if prior is None or prior[0] != implementation:
            db.execute('''INSERT INTO business.mart_build_scope SELECT study_hk, participant_hk, local_date
                FROM business.participant_date_state s WHERE NOT EXISTS (SELECT 1 FROM business.mart_build_scope w
                WHERE w.study_hk=s.study_hk AND w.participant_hk=s.participant_hk AND w.local_date=s.local_date)''')
        db.execute('COMMIT')
    subprocess.run([os.getenv('DBT_EXECUTABLE', '/opt/airflow/dbt-venv/bin/dbt'), 'run',
        '--project-dir', str(project), '--profiles-dir', str(profiles),
        '--target-path', str(runtime / 'target'), '--log-path', str(runtime / 'logs')], check=True)
    with connect(database) as db:
        if latest_impact(db, validation) != impact:
            raise ValueError('Impact inputs changed during mart preparation')
        db.execute('BEGIN')
        try:
            count = db.execute('SELECT count(*) FROM business.mart_build_scope').fetchone()[0]
            for dimension, model, key in [('participant','mart_participants','participant_sk'), ('device','mart_devices','device_sk')]:
                target = 'marts.dim_' + dimension
                candidate = 'business.' + model
                db.execute(f'''UPDATE {target} SET is_current=false, known_to_at=now()
                    WHERE is_current AND {key} NOT IN (SELECT {key} FROM {candidate})''')
                db.execute(f'''UPDATE {target} SET is_current=true, known_to_at=NULL
                    WHERE {key} IN (SELECT {key} FROM {candidate})''')
                db.execute(f'''INSERT INTO {target} SELECT c.*, now(), NULL, true FROM {candidate} c
                    WHERE NOT EXISTS (SELECT 1 FROM {target} d WHERE d.{key}=c.{key})''')
            db.execute('''INSERT INTO marts.dim_study SELECT sha256(to_json(list_value('study_dimension',h.study_hk))),
                h.study_hk, h.study_code, s.study_timezone, s.study_start, s.study_end_exclusive
                FROM vault.hub_study h JOIN vault.sat_study s ON s.parent_hk=h.study_hk
                WHERE NOT EXISTS (SELECT 1 FROM marts.dim_study d WHERE d.study_hk=h.study_hk)''')
            db.execute('''INSERT INTO marts.dim_date SELECT strftime(local_date,'%Y%m%d')::INTEGER,
                local_date, date_trunc('week',local_date)::DATE, week(local_date), isoyear(local_date), isodow(local_date)
                FROM (SELECT DISTINCT local_date FROM business.mart_build_scope) s
                WHERE NOT EXISTS (SELECT 1 FROM marts.dim_date d WHERE d.calendar_date=s.local_date)''')
            db.execute('DELETE FROM marts.gold_participant_devices')
            db.execute('INSERT INTO marts.gold_participant_devices SELECT * FROM business.mart_gold')
            db.execute('''DELETE FROM marts.fact_sleep_stage WHERE session_version_id IN (
                SELECT f.session_version_id FROM marts.fact_sleep_session f JOIN business.mart_build_scope w
                ON w.study_hk=f.study_hk AND w.participant_hk=f.participant_hk AND w.local_date=f.wake_date)''')
            for table, date in [('fact_steps_minute',None), ('fact_heart_rate_minute',None),
                                ('fact_sleep_session','wake_date'), ('fact_wellness_daily','survey_date')]:
                expression = f'f.{date}' if date else "timezone(?,f.event_at_utc)::DATE"
                parameters = [] if date else [validation['study_timezone']]
                db.execute(f'''DELETE FROM marts.{table} f WHERE EXISTS (SELECT 1 FROM business.mart_build_scope w
                    WHERE w.study_hk=f.study_hk AND w.participant_hk=f.participant_hk AND w.local_date={expression})''', parameters)
            for source, table, value in [('steps','fact_steps_minute','steps_per_minute'),
                                         ('heart_rate','fact_heart_rate_minute','heart_rate_bpm')]:
                for prefix in '0123456789abcdef':
                    db.execute(f'''INSERT INTO marts.{table} SELECT study_hk, participant_hk, study_sk,
                        participant_sk, device_sk, date_sk, event_at_utc, {value}, source_version_id,
                        quality_status, rule_version FROM business.mart_minutes
                        WHERE source_name=? AND left(source_version_id,1)=?''', [source,prefix])
            for table, model in [('fact_sleep_session','mart_sleep'), ('fact_sleep_stage','mart_stages'), ('fact_wellness_daily','mart_wellness')]:
                db.execute(f'INSERT INTO marts.{table} SELECT * FROM business.{model}')
            for source, table in [('steps','fact_steps_minute'), ('heart_rate','fact_heart_rate_minute'),
                                  ('sleep','fact_sleep_session'), ('wellness_survey','fact_wellness_daily')]:
                expected = db.execute('SELECT count(*) FROM business.mart_records WHERE source_name=?', [source]).fetchone()[0]
                actual = db.execute(f'''SELECT count(*) FROM marts.{table} f JOIN business.mart_build_scope w
                    ON w.study_hk=f.study_hk AND w.participant_hk=f.participant_hk
                    AND f.date_sk=strftime(w.local_date,'%Y%m%d')::INTEGER''').fetchone()[0]
                if expected != actual:
                    raise AssertionError('Accepted fact reconciliation failed: ' + source)
            for table in ('fact_steps_minute','fact_heart_rate_minute'):
                if db.execute(f'''SELECT 1 FROM marts.{table} f WHERE device_sk IS NOT NULL AND NOT EXISTS
                    (SELECT 1 FROM marts.dim_device d WHERE d.device_sk=f.device_sk) LIMIT 1''').fetchone():
                    raise AssertionError('Orphan device key: ' + table)
            for table in ('fact_steps_minute','fact_heart_rate_minute','fact_sleep_session','fact_wellness_daily'):
                if db.execute(f'''SELECT 1 FROM marts.{table} f WHERE NOT EXISTS
                    (SELECT 1 FROM marts.dim_participant p WHERE p.participant_sk=f.participant_sk)
                    OR NOT EXISTS (SELECT 1 FROM marts.dim_study s WHERE s.study_sk=f.study_sk)
                    OR NOT EXISTS (SELECT 1 FROM marts.dim_date d WHERE d.date_sk=f.date_sk) LIMIT 1''').fetchone():
                    raise AssertionError('Orphan dimensional key: ' + table)
            db.execute('''UPDATE business.affected_participant_dates q SET consumed_checkpoint_id=?
                WHERE consumed_checkpoint_id IS NULL AND EXISTS (SELECT 1 FROM business.mart_build_scope w
                    WHERE w.study_hk=q.study_hk AND w.participant_hk=q.participant_hk AND w.local_date=q.local_date)''', [evaluation])
            db.execute('INSERT INTO business.mart_evaluations VALUES (?,?,?,?,now())', [evaluation,impact,implementation,count])
            if _before_commit is not None:
                _before_commit(db)
            db.execute('COMMIT')
        except BaseException:
            try:
                db.execute('ROLLBACK')
            except duckdb.TransactionException:
                pass
            raise
    return {'status': 'marts_loaded', 'reused': False, 'affected_date_count': count, 'consumer_published': False}


if __name__ == '__main__':
    print(json.dumps(load()))
