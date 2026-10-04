"""Verify quality equivalence catalogs; optionally install them in an idle warehouse."""

import argparse
import json
import os
from pathlib import Path

import duckdb

from generate_ddl import QUALITY_CATALOGS, TABLES


def verify(db):
    counts = {}
    for catalog, expected in QUALITY_CATALOGS.items():
        rows = db.execute(f'SELECT code, display_name, description FROM business.{catalog} ORDER BY code').fetchall()
        if rows != sorted(expected):
            raise ValueError(f'Catalog does not match its canonical equivalences: {catalog}')
        counts[catalog] = len(rows)
    for name, _, _, _, checks in TABLES:
        if name in ('business.quality_issues', 'business.quality_resolutions'):
            for check in checks:
                if db.execute(f'SELECT count(*) FROM {name} WHERE NOT ({check})').fetchone()[0]:
                    raise ValueError(f'Ledger contains an unknown domain value: {name}')
    return counts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Create and seed catalogs before verification')
    parser.add_argument('--database', default=os.environ.get('PROJECT_DATABASE_PATH', '/opt/airflow/project-data/warehouse.duckdb'))
    parser.add_argument('--ddl-root', type=Path, default=Path(os.environ.get('PROJECT_DDL_ROOT', str(Path(__file__).resolve().parents[1] / 'infra/ddl'))))
    args = parser.parse_args()
    if not Path(args.database).is_file():
        parser.error('The warehouse must already exist.')
    with duckdb.connect(args.database, read_only=not args.apply) as db:
        if args.apply:
            db.execute((args.ddl_root / 'duckdb/003_quality_catalogs.sql').read_text())
        print(json.dumps({'catalog_counts': verify(db), 'verified': True}))
