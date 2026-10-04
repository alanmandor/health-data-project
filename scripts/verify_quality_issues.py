"""Verify durable issues before and after publication-linked sleep resolutions."""

import argparse
import json
import os

import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', action='store_true')
    args = parser.parse_args()
    with duckdb.connect(os.environ['PROJECT_DATABASE_PATH'], read_only=True) as db:
        db.execute('SET threads=1')
        db.execute('SET memory_limit=?', [os.getenv('VAULT_MEMORY_LIMIT', '4GB')])
        if db.execute("SELECT 1 FROM business.quality_issues WHERE current_status NOT IN ('open','resolved') LIMIT 1").fetchone():
            raise AssertionError('Invalid issue status')
        if db.execute('''SELECT 1 FROM business.quality_issues i WHERE NOT EXISTS
            (SELECT 1 FROM raw.batch_records r WHERE r.batch_id=i.batch_id AND r.record_id=i.record_id) LIMIT 1''').fetchone():
            raise AssertionError('Missing raw issue lineage')
        if db.execute('''SELECT 1 FROM business.quality_resolutions r WHERE NOT EXISTS
            (SELECT 1 FROM business.quality_issues i WHERE i.issue_id=r.issue_id) LIMIT 1''').fetchone():
            raise AssertionError('Orphan resolution')
        if db.execute('''SELECT 1 FROM business.quality_issues i WHERE current_status='resolved'
            AND NOT EXISTS (SELECT 1 FROM business.quality_resolutions r WHERE r.issue_id=i.issue_id
                AND r.resolution_code<>'reopened') LIMIT 1''').fetchone():
            raise AssertionError('Resolved issue without history')
        report = {'issues': db.execute('''SELECT severity, current_status, count(*) FROM business.quality_issues
            GROUP BY severity, current_status ORDER BY severity, current_status''').fetchall(),
            'transitions': db.execute('SELECT count(*) FROM business.quality_resolutions').fetchone()[0],
            'evaluations': db.execute('SELECT count(*) FROM business.quality_evaluations').fetchone()[0]}
        if args.baseline:
            pending = db.execute("SELECT count(*) FROM business.quality_issues WHERE rule_code='unknown_participant' AND current_status='open'").fetchone()[0]
            if pending != 14:
                raise AssertionError('Expected 14 unresolved participant-reference issues')
            boundary = db.execute('''SELECT i.issue_id,i.current_status FROM business.quality_issues i
                JOIN business.record_quality q ON q.batch_id=i.batch_id AND q.record_id=i.record_id
                WHERE q.source_name='sleep' AND q.participant_id='P001' AND q.observation_date='2026-01-21'
                AND i.rule_code='sleep_boundary_discrepancy' ''').fetchall()
            if len(boundary)!=1:
                raise AssertionError('P001 boundary issue lineage changed')
            consolidated=db.execute('''SELECT 1 FROM business.quality_resolutions r
                JOIN biostatistics.publications p USING(publication_id)
                WHERE r.issue_id=? AND r.resolution_code='excluded_at_consolidation' ''',[boundary[0][0]]).fetchone()
            if boundary[0][1]!=('resolved' if consolidated else 'open'):
                raise AssertionError('P001 boundary status disagrees with its publication resolution')
            report['baseline_verified'] = True
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
