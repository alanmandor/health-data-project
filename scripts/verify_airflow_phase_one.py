"""Run manual control-phase checks through the local Airflow API, without publishing analytics."""

import json
import os
import time
import uuid
from pathlib import Path

import requests


def main():
    base = "http://airflow-api-server:8080"
    session = requests.Session()
    response = session.post(base + "/auth/token", json={
        "username": os.environ["AIRFLOW_ADMIN_USERNAME"],
        "password": os.environ["AIRFLOW_ADMIN_PASSWORD"],
    }, timeout=20)
    response.raise_for_status()
    session.headers["Authorization"] = "Bearer " + response.json()["access_token"]

    incoming = Path(os.environ["PROJECT_INCOMING_DIR"])
    incoming.mkdir(parents=True, exist_ok=True)
    fixture = Path("/opt/airflow/project/examples/manifests/empty_daily.json")
    (incoming / "phase-one-empty.json").write_text(fixture.read_text(), encoding="utf-8")
    checks = [
        ("wearable_daily_pipeline", {"target_date": "2026-01-11", "manifest_key": "phase-one-empty.json", "sensor_timeout_seconds": 60}),
        ("wearable_weekly_biostatistics", {"week_start": "2026-01-05"}),
    ]
    for dag_id, conf in checks:
        url = base + "/api/v2/dags/" + dag_id
        response = session.get(url, timeout=20)
        response.raise_for_status()
        was_paused = response.json()["is_paused"]
        response = session.patch(url, json={"is_paused": False}, timeout=20)
        response.raise_for_status()
        try:
            run_id = "manual__phase_one_" + uuid.uuid4().hex
            response = session.post(url + "/dagRuns", json={"dag_run_id": run_id, "logical_date": None, "conf": conf}, timeout=20)
            response.raise_for_status()
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                response = session.get(url + "/dagRuns/" + run_id, timeout=20)
                response.raise_for_status()
                state = response.json()["state"]
                if state in {"success", "failed"}:
                    break
                time.sleep(2)
            else:
                raise TimeoutError(f"Run did not finish: {dag_id}/{run_id}")
            if state != "success":
                raise RuntimeError(f"Control-phase run failed: {dag_id}/{run_id}")
            print(json.dumps({"dag_id": dag_id, "run_id": run_id, "state": state, "scope": "control_phase_only"}), flush=True)
        finally:
            response = session.patch(url, json={"is_paused": was_paused}, timeout=20)
            response.raise_for_status()


if __name__ == "__main__":
    main()
