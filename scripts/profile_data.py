"""Read-only profiling of the study exports using the Python standard library."""
import csv
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'
START = datetime(2026, 1, 8)
END = datetime(2026, 2, 5)
LOCAL = timezone(timedelta(hours=-5))
EXPECTED = {START + timedelta(minutes=i) for i in range(40320)}
report = {}


def csv_rows(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return result.astimezone(LOCAL).replace(tzinfo=None) if result.tzinfo else result


def duplicates(rows, keys):
    counts = Counter(tuple(row.get(k) for k in keys) for row in rows)
    return sum(n - 1 for n in counts.values()), sum(n > 1 for n in counts.values())


participants = csv_rows(DATA / 'participants/participants.csv')
ids = {r['participant_id'] for r in participants}
devices = {r['participant_id']: r['device_id'] for r in participants}
report['participants'] = {
    'rows': len(participants), 'duplicate_ids': duplicates(participants, ['participant_id']),
    'duplicate_devices': duplicates(participants, ['device_id']),
    'missing_expected_ids': sorted({f'P{i:03d}' for i in range(1, 51)} - ids),
    'nulls': dict(Counter(k for r in participants for k,v in r.items() if not v)),
    'categories': {k: dict(Counter(r[k] for r in participants)) for k in ['gender','chronotype']},
    'numeric_ranges': {k: [min(int(r[k]) for r in participants), max(int(r[k]) for r in participants)] for k in ['age','height_cm','max_heart_rate']},
}
metadata = csv_rows(DATA / 'health_summaries/device_metadata.csv')
report['device_metadata'] = {
    'rows': len(metadata), 'duplicate_ids': duplicates(metadata, ['participant_id']),
    'missing_participants': sorted(ids - {r['participant_id'] for r in metadata}),
    'unknown_participants': sum(r['participant_id'] not in ids for r in metadata),
    'device_mismatch': sum(devices.get(r['participant_id']) != r['device_id'] for r in metadata),
    'wear_sites': dict(Counter(r['wear_site'] for r in metadata)),
    'missing_firmware': [r['participant_id'] for r in metadata if not r['firmware_version']],
    'firmware_label_mismatch': [], 'unparseable_labels': [],
    'calibrated_after_study_start': sum(timestamp(r['calibration_date']) >= START for r in metadata),
}
for r in metadata:
    match = re.search(r'\bfw(\d+\.\d+\.\d+)\b', r['device_label'])
    if not match:
        report['device_metadata']['unparseable_labels'].append(r['participant_id'])
    elif r['firmware_version'] and match[1] != r['firmware_version']:
        report['device_metadata']['firmware_label_mismatch'].append(r['participant_id'])
surveys = csv_rows(DATA / 'health_summaries/wellness_survey.csv')
survey_keys = {(r['participant_id'], r['date']) for r in surveys}
expected_days = {(pid, (START + timedelta(days=d)).date().isoformat()) for pid in ids for d in range(28)}
report['wellness_survey'] = {
    'rows': len(surveys), 'duplicate_keys': duplicates(surveys, ['participant_id','date']),
    'missing_participant_days': len(expected_days - survey_keys),
    'unexpected_participant_days': len(survey_keys - expected_days),
    'unknown_participant_rows': sum(r['participant_id'] not in ids for r in surveys),
    'unknown_participants': sorted({r['participant_id'] for r in surveys} - ids),
    'nulls': dict(Counter(k for r in surveys for k,v in r.items() if not v)),
    'invalid_scores': {},
}
for col, low, high in [('fatigue_score',1,5),('stress_score',1,5),('readiness_score',0,10),('sleep_quality_score',1,5)]:
    invalid = []
    for r in surveys:
        try:
            valid = low <= int(r[col]) <= high
        except (ValueError, TypeError):
            valid = False
        if not valid:
            invalid.append({k:r[k] for k in ['participant_id','date',col]})
    report['wellness_survey']['invalid_scores'][col] = {'count':len(invalid),'examples':invalid[:5]}

report['minute_sources'] = {}
report['sleep'] = {'rows':0, 'issues':{}, 'examples':{}, 'duplicate_keys':0, 'missing_participant_days':0}
sleep_counts = Counter()
sleep_examples = {}
sleep_keys = set()
minute_keys = {}


def sleep_issue(rule, pid, day):
    sleep_counts[rule] += 1
    sleep_examples.setdefault(rule, [])
    if len(sleep_examples[rule]) < 5:
        sleep_examples[rule].append({'participant_id':pid,'date':day})


for source, field in [('steps','steps'),('heart_rate','heart_rate_bpm')]:
    result = {'rows':0, 'issues':{}, 'per_participant':[], 'value_range':None, 'schema_variants':{}, 'flat_runs_60min':[]}
    issues = Counter()
    schemas = Counter()
    units = Counter()
    values = []
    for pid in sorted(ids):
        rows = json.loads((DATA / f'wearable_events/{pid}/{source}.json').read_text(encoding='utf-8'))
        keys = Counter()
        series = {}
        local_issues = Counter()
        for row in rows:
            schemas[','.join(sorted(row))] += 1
            if '_unit' in row:
                units[(pid, str(row['_unit']))] += 1
            if row.get('participant_id') != pid:
                local_issues['participant_path_mismatch'] += 1
            try:
                raw_ts = row['timestamp']
                t = timestamp(raw_ts)
                if datetime.fromisoformat(raw_ts.replace('Z','+00:00')).tzinfo:
                    local_issues['explicit_timezone'] += 1
            except (KeyError, ValueError, TypeError):
                local_issues['invalid_timestamp'] += 1
                continue
            keys[(row.get('participant_id'),t)] += 1
            if t.second or t.microsecond:
                local_issues['off_minute_boundary'] += 1
            if not START <= t < END:
                local_issues['outside_study_window'] += 1
            value = row.get(field)
            if type(value) is not int:
                local_issues['non_integer_or_null_value'] += 1
            else:
                values.append(value)
                if source == 'heart_rate' and not 40 <= value <= 200:
                    local_issues['outside_dictionary_range'] += 1
                if source == 'steps' and value < 0:
                    local_issues['negative_steps'] += 1
                if source == 'steps' and value > 30:
                    local_issues['above_30_steps_candidate_only'] += 1
            series.setdefault(t, []).append(value)
        valid_grid = {t for p,t in keys if p == pid}
        local_issues['missing_expected_minutes'] = len(EXPECTED - valid_grid)
        local_issues['duplicate_key_excess_rows'] = sum(n-1 for n in keys.values())
        local_issues['conflicting_timestamp_groups'] = sum(len(set(v)) > 1 for v in series.values())
        result['rows'] += len(rows)
        issues.update(local_issues)
        result['per_participant'].append({'participant_id':pid,'rows':len(rows),'issues':dict(local_issues)})
        minute_keys[(source,pid)] = valid_grid
        if source == 'heart_rate':
            run = []
            previous = None
            previous_value = None
            def finish():
                if len(run) >= 60:
                    result['flat_runs_60min'].append({'participant_id':pid,'start':run[0].isoformat(),'end':run[-1].isoformat(),'minutes':len(run),'bpm':previous_value})
            for t, vv in sorted(series.items()):
                value = vv[0] if len(set(vv)) == 1 and type(vv[0]) is int else None
                if previous is not None and (t-previous != timedelta(minutes=1) or value != previous_value or value is None):
                    finish()
                    run = []
                run.append(t)
                previous, previous_value = t, value
            finish()
    result['issues'] = dict(issues)
    result['schema_variants'] = dict(schemas)
    result['explicit_units'] = [{'participant_id':p,'unit':u,'rows':n} for (p,u),n in units.items()]
    result['value_range'] = [min(values),max(values)] if values else None
    report['minute_sources'][source] = result
    print(f'Profiled {source}: {result["rows"]} rows', flush=True)

for pid in sorted(ids):
    rows = json.loads((DATA / f'wearable_events/{pid}/sleep.json').read_text(encoding='utf-8'))
    report['sleep']['rows'] += len(rows)
    for r in rows:
        day = r.get('date')
        key = (r.get('participant_id'),day)
        if key in sleep_keys:
            report['sleep']['duplicate_keys'] += 1
        sleep_keys.add(key)
        if r.get('participant_id') != pid:
            sleep_issue('participant_path_mismatch',pid,day)
        try:
            onset, end = timestamp(r['sleep_onset']), timestamp(r['sleep_end'])
            if end <= onset:
                sleep_issue('nonpositive_session_duration',pid,day)
            if end.date().isoformat() != day:
                sleep_issue('wake_date_mismatch',pid,day)
            stages = r['stages']
            if stages and len(stages) >= 2 and stages[-1]['stage'] == 'awake' and timestamp(stages[-2]['end_time']) == end:
                sleep_issue('diagnostic_last_nonawake_ends_at_sleep_end',pid,day)
            elif stages:
                sleep_issue('sleep_end_not_last_nonawake_end',pid,day)
            if [s.get('stage') for s in stages] != ['light','deep','rem','light','awake']:
                sleep_issue('unexpected_stage_order',pid,day)
            previous_end = onset
            duration_sum = 0
            nonawake = 0
            for s in stages:
                a,b = timestamp(s['start_time']),timestamp(s['end_time'])
                duration = (b-a).total_seconds()/60
                if duration <= 0:
                    sleep_issue('nonpositive_stage_duration',pid,day)
                if a != previous_end:
                    sleep_issue('stage_gap_or_overlap',pid,day)
                if abs(duration-s['duration_min']) > 1e-9:
                    sleep_issue('stage_duration_mismatch',pid,day)
                if a < onset or b > end:
                    sleep_issue('stage_outside_session',pid,day)
                previous_end = b
                duration_sum += s['duration_min']
                if s['stage'] != 'awake':
                    nonawake += s['duration_min']
            if previous_end != end:
                sleep_issue('last_stage_end_mismatch',pid,day)
            session_duration = (end-onset).total_seconds()/60
            if abs(duration_sum-session_duration) > 1e-9:
                sleep_issue('stage_sum_session_mismatch',pid,day)
            if session_duration > 0 and abs(r['efficiency_pct'] - 100*nonawake/session_duration) > 1:
                sleep_issue('reported_vs_derived_efficiency_over_1pp',pid,day)
            if duration_sum > 0 and abs(r['efficiency_pct'] - 100*nonawake/duration_sum) > 1:
                sleep_issue('diagnostic_efficiency_vs_all_stages_over_1pp',pid,day)
            if not 0 <= r['efficiency_pct'] <= 100:
                sleep_issue('efficiency_out_of_range',pid,day)
            if not 0 <= r['restlessness'] <= 1:
                sleep_issue('restlessness_out_of_range',pid,day)
        except (ValueError,TypeError,KeyError):
            sleep_issue('parse_or_schema_error',pid,day)
report['sleep']['issues'] = dict(sleep_counts)
report['sleep']['examples'] = sleep_examples
report['sleep']['missing_participant_days'] = len(expected_days-sleep_keys)
report['sleep']['unexpected_participant_days'] = len(sleep_keys-expected_days)
report['cross_source_grid'] = {
    'steps_minutes_without_heart_rate':sum(len(minute_keys[('steps',p)]-minute_keys[('heart_rate',p)]) for p in ids),
    'heart_rate_minutes_without_steps':sum(len(minute_keys[('heart_rate',p)]-minute_keys[('steps',p)]) for p in ids),
}
output = ROOT / 'output/profiling'
output.mkdir(parents=True, exist_ok=True)
(output / 'profile.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in report.items() if k != 'minute_sources'},indent=2))
for source, result in report['minute_sources'].items():
    print(source,json.dumps({k:v for k,v in result.items() if k != 'per_participant'},indent=2))
