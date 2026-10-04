"""Independently reconcile frozen biostatistics, input receipts, and sleep exclusions."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import duckdb


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',action='store_true')
    args=parser.parse_args()
    with duckdb.connect(os.environ['PROJECT_DATABASE_PATH'],read_only=True) as db:
        db.execute('SET threads=1')
        db.execute('SET memory_limit=?',[os.getenv('VAULT_MEMORY_LIMIT','4GB')])
        publications=db.execute('SELECT publication_id,week_start,revision,correction_cutoff_at,status FROM biostatistics.publications ORDER BY week_start,revision').fetchall()
        if not publications:
            raise AssertionError('No weekly publications found')
        with sqlite3.connect(os.environ['PROJECT_REGISTRY_PATH']) as control:
            for publication,week,revision,cutoff,status in publications:
                row=control.execute('SELECT status,correction_cutoff_at FROM publication_registry WHERE publication_id=?',[publication]).fetchone()
                if row is None or row[0]!='published':
                    raise AssertionError('Published warehouse output lacks a registry receipt')
                for count,total in [('accepted_steps_minutes','observed_steps_total'),('accepted_heart_rate_minutes','heart_rate_sum')]:
                    daily=db.execute(f'SELECT sum({count}),sum({total}) FROM biostatistics.participant_day WHERE publication_id=?',[publication]).fetchone()
                    weekly=db.execute(f'SELECT sum({count}),sum({total}) FROM biostatistics.participant_week WHERE publication_id=?',[publication]).fetchone()
                    if daily!=weekly:
                        raise AssertionError('Weekly metric reconciliation failed')
                if db.execute('''SELECT 1 FROM biostatistics.participant_week w WHERE w.publication_id=? AND
                    (w.expected_days<>(SELECT count(*) FROM publication_build.readiness r WHERE r.publication_id=w.publication_id)
                    OR w.partial_week<>(w.expected_days<7)) LIMIT 1''',[publication]).fetchone():
                    raise AssertionError('Weekly coverage or partial-week flag mismatch')
                full=db.execute('SELECT count(*) FROM publication_build.sleep_stage_candidates WHERE publication_id=?',[publication]).fetchone()[0]
                retained=db.execute('SELECT count(*) FROM biostatistics.sleep_stages WHERE publication_id=?',[publication]).fetchone()[0]
                excluded=db.execute('SELECT count(*) FROM biostatistics.sleep_stage_exclusions WHERE publication_id=?',[publication]).fetchone()[0]
                if full!=retained+excluded:
                    raise AssertionError('Frozen stages do not reconcile with exclusions')
                if db.execute('''SELECT 1 FROM biostatistics.sleep_stage_exclusions e WHERE e.publication_id=?
                    AND NOT EXISTS(SELECT 1 FROM business.quality_resolutions r WHERE r.resolution_id=e.resolution_id
                    AND r.issue_id=e.issue_id AND r.publication_id=e.publication_id AND r.resolution_code='excluded_at_consolidation') LIMIT 1''',[publication]).fetchone():
                    raise AssertionError('A consolidated exclusion has no publication-linked resolution')
                root=Path(os.getenv('PROJECT_PUBLICATION_DIR',str(Path(os.environ['PROJECT_DATABASE_PATH']).parent/'publications')))/publication
                for kind,digest in control.execute('SELECT input_kind,version_id FROM publication_inputs WHERE publication_id=?',[publication]):
                    path=root/(kind+'.jsonl.gz')
                    with path.open('rb') as stream:
                        if hashlib.file_digest(stream,'sha256').hexdigest()!=digest:
                            raise AssertionError('Compressed frozen input manifest checksum changed')
                    with gzip.open(path,'rt') as stream:
                        exported=sum(1 for _ in stream)
                    selected=db.execute('SELECT count(*) FROM publication_build.inputs WHERE publication_id=? AND input_kind=?',[publication,kind]).fetchone()[0]
                    if exported!=selected:
                        raise AssertionError('Exported input versions do not reconcile with the frozen ledger')
        counts={table:db.execute('SELECT count(*) FROM biostatistics.'+table).fetchone()[0] for table in
                ('publications','participant_snapshot','participant_day','participant_week','sleep_stages','sleep_stage_exclusions')}
        comparison=Path(__file__).resolve().parents[1]/'examples'/'queries'/'chronotype_comparison.sql'
        groups=db.execute(comparison.read_text()).fetchall()
        latest=db.execute('SELECT count(*) FROM biostatistics.latest_participant_week').fetchone()[0]
        if sum(row[2] for row in groups)!=latest:
            raise AssertionError('Chronotype comparison does not reconcile with the latest frozen cohort')
        if args.baseline:
            if counts['publications']!=5 or counts['participant_week']!=250 or counts['participant_snapshot']!=250 or counts['participant_day']!=1400:
                raise AssertionError('Unexpected baseline weekly counts')
            result=db.execute('''SELECT d.reported_stage_sleep_min,d.efficiency_pct_reported,d.quality_status
                FROM biostatistics.participant_day d JOIN biostatistics.participant_snapshot p USING(publication_id,participant_hk)
                WHERE p.participant_id='P001' AND d.local_date='2026-01-21' ''').fetchone()
            if result!=(327,100,'accepted_with_warning'):
                raise AssertionError('P001 reported consolidated values changed')
            if not db.execute('''SELECT 1 FROM biostatistics.sleep_stage_exclusions e
                JOIN publication_build.inputs i ON i.publication_id=e.publication_id AND i.version_id=e.session_version_id
                JOIN vault.sat_sleep_session s ON s.version_id=e.session_version_id
                JOIN vault.hub_sleep_session h ON h.sleep_session_hk=s.parent_hk
                JOIN vault.hub_participant p USING(participant_hk)
                WHERE p.participant_id='P001' AND h.wake_date='2026-01-21' ''').fetchone():
                raise AssertionError('P001 post-boundary awake was not excluded from the frozen stages')
            if db.execute('SELECT count(*) FROM marts.fact_sleep_stage').fetchone()[0]!=7000:
                raise AssertionError('Consolidation modified provisional stages')
        print(json.dumps({'counts':counts,'chronotype_groups':len(groups),'baseline_verified':args.baseline}))


if __name__=='__main__':
    main()
