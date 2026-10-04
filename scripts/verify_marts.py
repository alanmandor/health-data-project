"""Reconcile persisted star-schema facts and provisional baseline sleep values."""

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
        db.execute('SET memory_limit=?', [os.getenv('VAULT_MEMORY_LIMIT','4GB')])
        counts = {t: db.execute('SELECT count(*) FROM marts.'+t).fetchone()[0] for t in
            ('dim_participant','dim_device','dim_study','dim_date','gold_participant_devices',
             'fact_steps_minute','fact_heart_rate_minute','fact_sleep_session','fact_sleep_stage','fact_wellness_daily')}
        for source, table in [('steps','fact_steps_minute'), ('heart_rate','fact_heart_rate_minute'),
                              ('sleep','fact_sleep_session'), ('wellness_survey','fact_wellness_daily')]:
            expected = db.execute('SELECT count(*) FROM business.quality_current WHERE source_name=?', [source]).fetchone()[0]
            if counts[table] != expected:
                raise AssertionError('Accepted count mismatch: '+source)
            if db.execute(f'''SELECT 1 FROM marts.{table} f WHERE NOT EXISTS
                (SELECT 1 FROM marts.dim_participant p WHERE p.participant_sk=f.participant_sk)
                OR NOT EXISTS (SELECT 1 FROM marts.dim_study s WHERE s.study_sk=f.study_sk)
                OR NOT EXISTS (SELECT 1 FROM marts.dim_date d WHERE d.date_sk=f.date_sk) LIMIT 1''').fetchone():
                raise AssertionError('Orphan dimensional key: '+table)
        if db.execute('''SELECT 1 FROM marts.fact_sleep_stage t WHERE NOT EXISTS
            (SELECT 1 FROM marts.fact_sleep_session s WHERE s.session_version_id=t.session_version_id) LIMIT 1''').fetchone():
            raise AssertionError('Stale sleep stage version')
        for table in ('dim_participant','dim_device'):
            key = 'participant_hk' if table=='dim_participant' else 'device_hk'
            if db.execute(f'SELECT 1 FROM marts.{table} WHERE is_current GROUP BY {key} HAVING count(*)>1 LIMIT 1').fetchone():
                raise AssertionError('Ambiguous current dimension: '+table)
        pending = db.execute('SELECT count(*) FROM business.affected_participant_dates WHERE consumed_checkpoint_id IS NULL').fetchone()[0]
        if pending:
            raise AssertionError('Mart evaluation left unconsumed date work')
        if db.execute('''SELECT 1 FROM business.affected_participant_dates q WHERE NOT EXISTS
            (SELECT 1 FROM business.mart_evaluations m WHERE m.evaluation_id=q.consumed_checkpoint_id) LIMIT 1''').fetchone():
            raise AssertionError('Queue references a missing mart checkpoint')
        if args.baseline:
            for table, expected in [('dim_participant',50),('dim_device',50),('dim_study',1),('dim_date',28),
                                    ('gold_participant_devices',50),('fact_sleep_session',1400),
                                    ('fact_sleep_stage',7000),('fact_wellness_daily',1400)]:
                if counts[table]!=expected:
                    raise AssertionError('Baseline count mismatch: '+table)
            result = db.execute('''SELECT f.reported_stage_sleep_min, f.efficiency_pct_reported,
                sum(t.duration_min_reported) FILTER(WHERE t.stage_type='awake'),
                bool_or(t.is_post_boundary_awake)
                FROM marts.fact_sleep_session f JOIN marts.dim_participant p USING(participant_sk)
                JOIN marts.fact_sleep_stage t USING(session_version_id)
                WHERE p.participant_id='P001' AND f.wake_date='2026-01-21'
                GROUP BY f.reported_stage_sleep_min, f.efficiency_pct_reported''').fetchone()
            if result != (327,100,55,True):
                raise AssertionError('P001 provisional sleep values changed')
            if db.execute("SELECT count(*) FROM marts.gold_participant_devices WHERE firmware_provenance='device_label'").fetchone()[0]!=15:
                raise AssertionError('Derived firmware provenance was lost')
        print(json.dumps({'counts':counts,'pending_work':pending,'baseline_verified':args.baseline},indent=2))


if __name__=='__main__':
    main()
