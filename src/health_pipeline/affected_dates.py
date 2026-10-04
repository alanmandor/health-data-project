"""Queue participant dates whose accepted analytical inputs have changed."""

import hashlib
import json
import os
from pathlib import Path

from health_pipeline.dbt_quality import input_hash, project_hash
from health_pipeline.vault_loading import configuration, identity
from health_pipeline import quality_issues


def propagate(database=None, project=None, runtime=None, _before_commit=None):
    import duckdb
    database = Path(database or os.environ['PROJECT_DATABASE_PATH']).resolve()
    project = Path(project or os.getenv('PROJECT_DBT_DIR', '/opt/airflow/project/dbt'))
    runtime = Path(runtime or os.getenv('PROJECT_DBT_RUNTIME', '/opt/airflow/project-data/dbt'))
    validation = json.loads((runtime / 'validated.json').read_text())
    config = configuration()
    code = project_hash(project)
    if (validation['database'] != str(database) or validation['code_version'] != code
            or validation['study_code'] != config[0] or validation['study_timezone'] != config[1]):
        raise ValueError('Run dbt validation for this database, code, and study first')
    sql_path = Path(__file__).parent / 'sql' / 'affected_dates.sql'
    implementation = hashlib.sha256(Path(__file__).read_bytes() + sql_path.read_bytes()).hexdigest()
    issue_module = Path(quality_issues.__file__)
    issue_implementation = hashlib.sha256(issue_module.read_bytes() +
        (issue_module.parent / 'sql' / 'quality_issues.sql').read_bytes()).hexdigest()
    study = identity('study', config[0])
    with duckdb.connect(str(database)) as db:
        db.execute("SET TimeZone='UTC'")
        db.execute('SET threads=1')
        db.execute('SET memory_limit=?', [os.getenv('VAULT_MEMORY_LIMIT', '4GB')])
        db.execute('SET preserve_insertion_order=false')
        temporary = runtime / 'affected-dates-temp'
        temporary.mkdir(parents=True, exist_ok=True)
        db.execute('SET temp_directory=?', [str(temporary)])
        if validation['input_hash'] != input_hash(db):
            raise ValueError('Raw inputs changed after dbt validation')
        if db.execute('SELECT study_timezone, study_start, study_end_exclusive FROM vault.sat_study').fetchall() != [config[1:]]:
            raise ValueError('Study configuration differs from the committed Vault')
        checkpoint = db.execute('''SELECT v.evaluation_id, q.evaluation_id FROM business.vault_loads v
            JOIN business.quality_evaluations q ON q.vault_evaluation_id=v.evaluation_id
            WHERE v.input_hash=? AND v.quality_code_version=? AND v.study_hk=? AND q.implementation_hash=?
            ORDER BY v.committed_at DESC, q.committed_at DESC, q.evaluation_id DESC LIMIT 1''',
            [validation['input_hash'], code, study, issue_implementation]).fetchone()
        if checkpoint is None:
            raise ValueError('Commit the matching Vault and issue evaluations before propagating dates')
        evaluation = identity('impact_evaluation', *checkpoint, implementation)
        db.execute('BEGIN TRANSACTION')
        try:
            previous = db.execute('SELECT affected_date_count FROM business.impact_evaluations WHERE evaluation_id=?', [evaluation]).fetchone()
            if previous:
                db.execute('COMMIT')
                return {'status': 'affected_dates_queued', 'evaluation_id': evaluation,
                        'reused': True, 'affected_date_count': previous[0], 'consumer_published': False}
            db.execute('''CREATE TEMP TABLE impact_configuration AS SELECT ?::VARCHAR AS study_hk,
                ?::VARCHAR AS quality_code, ?::VARCHAR AS implementation_hash, ?::VARCHAR AS study_timezone''',
                [study, code, implementation, config[1]])
            db.execute(sql_path.read_text())
            count = db.execute('SELECT count(*) FROM impact_changes').fetchone()[0]
            db.execute('''INSERT INTO business.affected_participant_dates
                SELECT study_hk, participant_hk, local_date,
                       sha256(to_json(list_value('affected_day', ?, study_hk, participant_hk,
                           local_date::VARCHAR, previous_hash, next_hash))), reason_code, now(), NULL
                FROM impact_changes''', [evaluation])
            db.execute('''DELETE FROM business.participant_date_state p WHERE EXISTS (
                SELECT 1 FROM impact_changes c WHERE c.study_hk=p.study_hk
                    AND c.participant_hk=p.participant_hk AND c.local_date=p.local_date)''')
            db.execute('''INSERT INTO business.participant_date_state
                SELECT n.study_hk, n.participant_hk, n.local_date, n.state_hash, ?
                FROM impact_new_state n JOIN impact_changes c USING (study_hk, participant_hk, local_date)''', [evaluation])
            db.execute('INSERT INTO business.impact_evaluations VALUES (?,?,?,?,?,now())',
                       [evaluation, *checkpoint, implementation, count])
            if _before_commit is not None:
                _before_commit(db)
            for table in ('impact_changes', 'impact_new_state', 'impact_references',
                          'impact_observation_days', 'impact_configuration'):
                db.execute('DROP TABLE ' + table)
            db.execute('COMMIT')
        except BaseException:
            try:
                db.execute('ROLLBACK')
            except duckdb.TransactionException:
                pass
            raise
    return {'status': 'affected_dates_queued', 'evaluation_id': evaluation,
            'reused': False, 'affected_date_count': count, 'consumer_published': False}


if __name__ == '__main__':
    print(json.dumps(propagate()))
