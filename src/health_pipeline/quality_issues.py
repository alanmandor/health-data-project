"""Persist quality issue lifecycles after a validated Data Vault evaluation."""

import hashlib
import json
import os
from pathlib import Path

from health_pipeline.dbt_quality import input_hash, project_hash
from health_pipeline.vault_loading import configuration, identity


def reconcile(database=None, project=None, runtime=None, _before_commit=None):
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
    sql_path = Path(__file__).parent / 'sql' / 'quality_issues.sql'
    implementation = hashlib.sha256(Path(__file__).read_bytes() + sql_path.read_bytes()).hexdigest()
    with duckdb.connect(str(database)) as db:
        db.execute("SET TimeZone='UTC'")
        db.execute('SET threads=1')
        db.execute('SET memory_limit=?', [os.getenv('VAULT_MEMORY_LIMIT', '4GB')])
        db.execute('SET preserve_insertion_order=false')
        temporary = runtime / 'quality-issues-temp'
        temporary.mkdir(parents=True, exist_ok=True)
        db.execute('SET temp_directory=?', [str(temporary)])
        if validation['input_hash'] != input_hash(db):
            raise ValueError('Raw inputs changed after dbt validation')
        if db.execute('SELECT study_timezone, study_start, study_end_exclusive FROM vault.sat_study').fetchall() != [config[1:]]:
            raise ValueError('Study configuration differs from the committed Vault')
        vault = db.execute('''SELECT evaluation_id FROM business.vault_loads
            WHERE input_hash=? AND quality_code_version=? AND study_hk=?
            ORDER BY committed_at DESC, evaluation_id DESC LIMIT 1''',
            [validation['input_hash'], code, identity('study', config[0])]).fetchone()
        if vault is None:
            raise ValueError('Commit the matching Data Vault evaluation before reconciling issues')
        evaluation = identity('quality_evaluation', vault[0], implementation)
        db.execute('BEGIN TRANSACTION')
        try:
            if db.execute('SELECT 1 FROM business.quality_evaluations WHERE evaluation_id=?', [evaluation]).fetchone():
                db.execute('COMMIT')
                return {'status': 'quality_issues_reconciled', 'evaluation_id': evaluation, 'reused': True}
            db.execute(sql_path.read_text())
            db.execute('''INSERT INTO business.quality_resolutions
                SELECT sha256(to_json(list_value(?, issue_id, resolution_code))), issue_id,
                       now(), resolution_code, superseding_version_id, NULL
                FROM issue_transitions''', [evaluation])
            db.execute('''UPDATE business.quality_issues AS i SET current_status=
                CASE WHEN t.is_active THEN 'open' ELSE 'resolved' END
                FROM issue_transitions t WHERE t.issue_id=i.issue_id''')
            if db.execute('''SELECT 1 FROM business.quality_resolutions r WHERE NOT EXISTS
                (SELECT 1 FROM business.quality_issues i WHERE i.issue_id=r.issue_id) LIMIT 1''').fetchone():
                raise AssertionError('Orphan quality resolution')
            db.execute('INSERT INTO business.quality_evaluations VALUES (?,?,?,now())',
                       [evaluation, vault[0], implementation])
            if _before_commit is not None:
                _before_commit(db)
            for table in ('issue_transitions', 'issue_domain_versions', 'issue_state',
                          'issue_latest_accepted', 'issue_observed', 'issue_current_warnings'):
                db.execute('DROP TABLE ' + table)
            db.execute('COMMIT')
        except BaseException:
            try:
                db.execute('ROLLBACK')
            except duckdb.TransactionException:
                pass
            raise
    return {'status': 'quality_issues_reconciled', 'evaluation_id': evaluation, 'reused': False}


if __name__ == '__main__':
    print(json.dumps(reconcile()))
