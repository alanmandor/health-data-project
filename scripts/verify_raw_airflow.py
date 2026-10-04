"""Execute daily and weekly DAGs twice against isolated synthetic deliveries."""

import hashlib
import json
import os
import runpy
import subprocess
import tempfile
from pathlib import Path

import pendulum
import requests


def main():
    session = requests.Session()
    base = "http://airflow-api-server:8080"
    response = session.post(base + "/auth/token", json={"username": os.environ["AIRFLOW_ADMIN_USERNAME"], "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, timeout=20)
    response.raise_for_status()
    session.headers["Authorization"] = "Bearer " + response.json()["access_token"]
    url = base + "/api/v2/dags/wearable_daily_pipeline"
    response = session.get(url, timeout=20)
    response.raise_for_status()
    if not response.json()["is_paused"]:
        raise RuntimeError("Pause the raw DAG before isolated testing to prevent scheduler interference")
    offset = 0
    while True:
        response = session.get(url + "/dagRuns", params={"limit": 100, "offset": offset}, timeout=20)
        response.raise_for_status()
        runs = response.json()["dag_runs"]
        if any(run["state"] in {"queued", "running"} for run in runs):
            raise RuntimeError("Finish existing raw DAG runs before isolated testing")
        if len(runs) < 100:
            break
        offset += 100
    with tempfile.TemporaryDirectory(prefix="raw-dag-check-", dir="/opt/airflow/project-data") as directory:
        root = Path(directory)
        incoming = root / "incoming"
        incoming.mkdir()
        for variable, name in [("PROJECT_INCOMING_DIR", "incoming"), ("PROJECT_RAW_DIR", "raw"), ("PROJECT_ARCHIVE_DIR", "archive"), ("PROJECT_REGISTRY_PATH", "control.sqlite"), ("PROJECT_DATABASE_PATH", "warehouse.duckdb"), ("PROJECT_DBT_RUNTIME", "dbt")]:
            os.environ[variable] = str(root / name)
        payloads={
            'participants':[{'participant_id':'CHECK','device_id':'DEVICE','age':40,'height_cm':180,'gender':'male','chronotype':'A','max_heart_rate':180}],
            'steps':[{'participant_id':'CHECK','timestamp':'2026-01-11T00:00:00','steps':0}],
            'heart_rate':[{'participant_id':'CHECK','timestamp':'2026-01-11T00:00:00','heart_rate_bpm':60}],
            'sleep':[{'participant_id':'CHECK','date':'2026-01-11','sleep_onset':'2026-01-10T22:00:00','sleep_end':'2026-01-11T06:00:00',
                      'efficiency_pct':100,'restlessness':0,'stages':[
                          {'stage':'deep','start_time':'2026-01-10T22:00:00','end_time':'2026-01-11T06:00:00','duration_min':480},
                          {'stage':'awake','start_time':'2026-01-11T06:00:00','end_time':'2026-01-11T06:10:00','duration_min':10}]}]}
        sources = {name: {"status": "empty_delta", "files": []} for name in ("participants", "device_metadata", "steps", "heart_rate", "sleep", "wellness_survey")}
        for source,records in payloads.items():
            payload=json.dumps(records).encode()
            key=source+'.json'
            (incoming/key).write_bytes(payload)
            sources[source]={'status':'files','files':[{'object_key':key,'sha256':hashlib.sha256(payload).hexdigest(),'bytes':len(payload),'format':'json'}]}
        manifest = {"schema_version": 1, "delivery_id": "isolated-smoke", "source_system": "synthetic_fixture", "batch_kind": "bootstrap", "delivery_mode": "delta", "target_dates": ["2026-01-08","2026-01-09","2026-01-10","2026-01-11"], "timezone": "America/Bogota", "completed_at": "2026-01-12T08:00:00Z", "sources": sources}
        (incoming / "manifest.json").write_text(json.dumps(manifest))
        dag = runpy.run_path("/opt/airflow/dags/wearable_daily_pipeline.py")["dag"]
        conf = {"target_date": "2026-01-11", "manifest_key": "manifest.json", "sensor_timeout_seconds": 60}
        for _ in range(2):
            result = dag.test(logical_date=pendulum.now("UTC"), run_conf=conf)
            if str(result.state) != "success":
                raise RuntimeError("Raw DAG smoke run failed")
        if (incoming / "steps.json").exists() or len(list((root / "archive").rglob("*.json"))) != 4:
            raise AssertionError("Raw DAG did not archive exactly once")
        weekly_url=base+'/api/v2/dags/wearable_weekly_biostatistics'
        response=session.get(weekly_url,timeout=20)
        response.raise_for_status()
        if not response.json()['is_paused']:
            raise RuntimeError('Pause the weekly DAG before isolated testing')
        response=session.get(weekly_url+'/dagRuns',params={'state':['running','queued']},timeout=20)
        response.raise_for_status()
        if response.json()['dag_runs']:
            raise RuntimeError('Finish existing weekly DAG runs before isolated testing')
        weekly=runpy.run_path('/opt/airflow/dags/wearable_weekly_biostatistics.py')['dag']
        weekly_conf={'week_start':'2026-01-05','expected_source_system':'synthetic_fixture','expected_delivery_id':'isolated-smoke','sensor_timeout_seconds':60}
        for _ in range(2):
            result=weekly.test(logical_date=pendulum.now('UTC'),run_conf=weekly_conf)
            if str(result.state)!='success':
                raise RuntimeError('Weekly DAG smoke run failed')
        check='''import duckdb,os,sqlite3
db=duckdb.connect(os.environ['PROJECT_DATABASE_PATH'])
assert db.execute('SELECT count(*) FROM raw.source_records').fetchone()[0]==4
assert db.execute('SELECT count(*) FROM raw.batch_records').fetchone()[0]==4
assert db.execute('SELECT count(*) FROM raw.file_loads').fetchone()[0]==4
assert db.execute('SELECT count(*) FROM biostatistics.publications').fetchone()[0]==1
assert db.execute('SELECT count(*) FROM biostatistics.participant_day').fetchone()[0]==4
assert db.execute('SELECT count(*) FROM biostatistics.sleep_stage_exclusions').fetchone()[0]==1
assert db.execute('SELECT count(*) FROM marts.fact_sleep_stage').fetchone()[0]==2
assert db.execute('SELECT count(*) FROM biostatistics.sleep_stages').fetchone()[0]==1
control=sqlite3.connect(os.environ['PROJECT_REGISTRY_PATH'])
assert control.execute('SELECT count(*) FROM publication_registry').fetchone()[0]==1
assert control.execute('SELECT count(*) FROM daily_readiness').fetchone()[0]==4
'''
        subprocess.run(['/opt/airflow/dbt-venv/bin/python','-c',check],check=True)
        print('Daily and weekly DAGs succeeded twice with isolated data, four archived files, and one frozen publication.')


if __name__ == "__main__":
    main()
