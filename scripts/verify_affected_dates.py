"""Verify impact queue integrity and the supplied study's initial date scope."""

import argparse
import json
import os

import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', action='store_true')
    args = parser.parse_args()
    with duckdb.connect(os.environ['PROJECT_DATABASE_PATH'], read_only=True) as db:
        if db.execute('''SELECT 1 FROM business.affected_participant_dates q WHERE NOT EXISTS
            (SELECT 1 FROM vault.hub_participant p WHERE p.participant_hk=q.participant_hk)
            OR NOT EXISTS (SELECT 1 FROM vault.hub_study s WHERE s.study_hk=q.study_hk) LIMIT 1''').fetchone():
            raise AssertionError('Orphan impact identity')
        if db.execute('''SELECT 1 FROM business.participant_date_state s WHERE NOT EXISTS
            (SELECT 1 FROM business.impact_evaluations e WHERE e.evaluation_id=s.evaluation_id) LIMIT 1''').fetchone():
            raise AssertionError('State references a missing checkpoint')
        if db.execute('''SELECT 1 FROM business.impact_evaluations e WHERE NOT EXISTS
            (SELECT 1 FROM business.quality_evaluations q WHERE q.evaluation_id=e.quality_evaluation_id
                AND q.vault_evaluation_id=e.vault_evaluation_id) LIMIT 1''').fetchone():
            raise AssertionError('Impact references a mismatched upstream checkpoint')
        report = {'queue_counts': db.execute('''SELECT reason_code, count(*) FROM business.affected_participant_dates
            GROUP BY reason_code ORDER BY reason_code''').fetchall(),
            'state_dates': db.execute('SELECT count(*) FROM business.participant_date_state').fetchone()[0],
            'evaluations': db.execute('SELECT count(*) FROM business.impact_evaluations').fetchone()[0],
            'pending_dates': db.execute('''SELECT count(*) FROM (SELECT DISTINCT study_hk, participant_hk, local_date
                FROM business.affected_participant_dates WHERE consumed_checkpoint_id IS NULL)''').fetchone()[0]}
        if args.baseline:
            if report['state_dates'] != 1400 or report['pending_dates'] not in (0,1400):
                raise AssertionError('Expected 50 participants across 28 study days')
            if db.execute('''SELECT count(*) FROM business.participant_date_state s JOIN vault.hub_participant h
                USING (participant_hk) WHERE h.participant_id='P999' ''').fetchone()[0]:
                raise AssertionError('Pending participant must not enter accepted date scope')
            if db.execute('''SELECT 1 FROM business.affected_participant_dates q
                WHERE consumed_checkpoint_id IS NOT NULL AND NOT EXISTS
                (SELECT 1 FROM business.mart_evaluations m WHERE m.evaluation_id=q.consumed_checkpoint_id) LIMIT 1''').fetchone():
                raise AssertionError('Consumption references a missing mart checkpoint')
            report['baseline_verified'] = True
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
