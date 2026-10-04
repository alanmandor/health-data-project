"""Read weekly prerequisites from SQLite without holding a DuckDB connection."""

from datetime import date
import json
import os
from pathlib import Path
import sqlite3

from health_pipeline.vault_loading import configuration, identity


def required_days(scope):
    code,zone,start,end = configuration()
    if scope['study_timezone'] != zone:
        raise ValueError('Weekly scope does not match the configured study time zone')
    days = [day for day in scope['target_dates'] if start<=date.fromisoformat(day)<end]
    if not days:
        raise ValueError('The requested week has no configured study days')
    return identity('study',code),days


def readiness(scope, expected_source_system, expected_delivery_id, registry=None, request_id=None):
    study,days = required_days(scope)
    registry = Path(registry or os.environ['PROJECT_REGISTRY_PATH'])
    if not registry.exists():
        return False
    with sqlite3.connect(registry) as db:
        if request_id:
            frozen=db.execute('SELECT scope_json FROM model_checkpoints WHERE checkpoint_id=? AND model_name=?',
                              [identity('weekly_request',request_id),'weekly_freeze']).fetchone()
            if frozen:
                binding=json.loads(frozen[0])
                if binding['study_hk']==study and binding['scope']==scope and binding['expected_source_system']==expected_source_system and binding['expected_delivery_id']==expected_delivery_id:
                    return True
        for day in days:
            row = db.execute('''SELECT coverage_json FROM daily_readiness
                WHERE study_hk=? AND target_date=? ORDER BY revision DESC LIMIT 1''',[study,day]).fetchone()
            if row is None:
                return False
            coverage = json.loads(row[0])
            if coverage.get('validation_status')!='passed' or coverage.get('empty_cohort',True):
                return False
            if day==days[-1] and not any(d['source_system']==expected_source_system and
                    d['delivery_id']==expected_delivery_id and day in d['target_dates']
                    for d in coverage.get('deliveries',[])):
                return False
    return True
