"""Trigger an explicit raw delivery through Airflow's API and restore DAG pause state."""

import argparse
import json
import os
import time
import uuid

import requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--sensor-timeout-seconds", type=int, default=3600)
    parser.add_argument("--wait-seconds", type=int, default=1800)
    args = parser.parse_args()
    base = "http://airflow-api-server:8080"
    session = requests.Session()
    response = session.post(base + "/auth/token", json={"username": os.environ["AIRFLOW_ADMIN_USERNAME"], "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, timeout=20)
    response.raise_for_status()
    session.headers["Authorization"] = "Bearer " + response.json()["access_token"]
    url = base + "/api/v2/dags/wearable_daily_pipeline"
    response = session.get(url, timeout=20)
    response.raise_for_status()
    paused = response.json()["is_paused"]
    response = session.patch(url, json={"is_paused": False}, timeout=20)
    response.raise_for_status()
    try:
        run_id = "manual__raw_" + uuid.uuid4().hex
        response = session.post(url + "/dagRuns", json={"dag_run_id": run_id, "logical_date": None, "conf": {"manifest_key": args.manifest_key, "target_date": args.target_date, "sensor_timeout_seconds": args.sensor_timeout_seconds}}, timeout=20)
        response.raise_for_status()
        print(json.dumps({"run_id": run_id, "status": "triggered"}), flush=True)
        deadline = time.monotonic() + args.wait_seconds
        previous = None
        while time.monotonic() < deadline:
            response = session.get(url + "/dagRuns/" + run_id, timeout=20)
            response.raise_for_status()
            state = response.json()["state"]
            if state != previous:
                print(json.dumps({"run_id": run_id, "state": state}), flush=True)
                previous = state
            if state == "success":
                return
            if state == "failed":
                raise RuntimeError(f"Raw delivery failed: {run_id}")
            time.sleep(5)
        raise TimeoutError(f"Wait expired; inspect the existing run before retrying: {run_id}")
    finally:
        response = session.patch(url, json={"is_paused": paused}, timeout=20)
        response.raise_for_status()


if __name__ == "__main__":
    main()
