"""Independently reconcile daily analytics and committed readiness receipts."""

import argparse
import json
import os
import sqlite3

import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',action='store_true')
    args = parser.parse_args()
    with duckdb.connect(os.environ['PROJECT_DATABASE_PATH'],read_only=True) as db:
        db.execute('SET threads=1')
        rows = db.execute('SELECT count(*) FROM marts.participant_day_current').fetchone()[0]
        published = db.execute('SELECT count(*) FROM analytics.participant_day').fetchone()[0]
        for fact,count,total,value in [('fact_steps_minute','accepted_steps_minutes','observed_steps_total','steps_per_minute'),
                                        ('fact_heart_rate_minute','accepted_heart_rate_minutes','heart_rate_sum','heart_rate_bpm')]:
            daily = db.execute(f'SELECT sum({count}),sum({total}) FROM marts.participant_day_current').fetchone()
            minute = db.execute(f'SELECT count(*),sum({value}) FROM marts.{fact}').fetchone()
            if (daily[0] or 0,daily[1])!=minute:
                raise AssertionError('Minute reconciliation failed: '+fact)
        receipts = db.execute('''SELECT study_hk,local_date,input_manifest_hash,rule_version,code_version
            FROM business.daily_publications WHERE status='published' ''').fetchall()
        with sqlite3.connect(os.environ['PROJECT_REGISTRY_PATH']) as control:
            for study,day,digest,rule,code in receipts:
                if not control.execute('''SELECT 1 FROM daily_readiness WHERE study_hk=? AND target_date=?
                    AND input_manifest_hash=? AND rule_version=? AND code_version=?''',[study,day.isoformat(),digest,rule,code]).fetchone():
                    raise AssertionError('Missing matching readiness receipt')
        if args.baseline:
            if rows!=1400 or published!=1400 or len(receipts)!=28:
                raise AssertionError('Unexpected baseline daily counts')
            sleep = db.execute('''SELECT d.reported_stage_sleep_min,d.efficiency_pct_reported,d.warning_count
                FROM analytics.participant_day d JOIN vault.hub_participant h USING(participant_hk)
                WHERE h.participant_id='P001' AND d.local_date='2026-01-21' ''').fetchone()
            if sleep[:2]!=(327,100) or sleep[2]==0:
                raise AssertionError('Provisional P001 sleep or warning was lost')
            if db.execute('SELECT 1 FROM analytics.participant_day WHERE expected_minutes<>1440 LIMIT 1').fetchone():
                raise AssertionError('Unexpected Bogota day length')
        print(json.dumps({'daily_rows':rows,'analytics_rows':published,'ready_dates':len(receipts),'baseline_verified':args.baseline}))


if __name__=='__main__':
    main()
