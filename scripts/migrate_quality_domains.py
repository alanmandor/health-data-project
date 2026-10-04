"""Apply quality ledger domain constraints while all warehouse tasks are idle."""

import argparse
import json
import os
from pathlib import Path

import duckdb


def migrate(db, sql):
    """Preserve every ledger row or roll back the entire migration."""
    relations = ('business.quality_issues', 'business.quality_resolutions')
    before = {name: db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
              for name in relations}
    try:
        db.execute(sql)
    except Exception:
        db.execute('ROLLBACK')
        raise
    return before


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default=os.environ.get(
        'PROJECT_DATABASE_PATH', '/opt/airflow/project-data/warehouse.duckdb'))
    parser.add_argument('--ddl-root', type=Path, default=Path(os.environ.get(
        'PROJECT_DDL_ROOT', str(Path(__file__).resolve().parents[1] / 'infra/ddl'))))
    args = parser.parse_args()
    if not Path(args.database).is_file():
        parser.error('The existing warehouse database must exist.')
    with duckdb.connect(args.database) as db:
        counts = migrate(db, (args.ddl_root / 'duckdb/002_quality_domains.sql').read_text())
    print(json.dumps({'status': 'quality_domains_enforced', 'preserved_rows': counts}))
