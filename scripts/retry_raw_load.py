"""Preview and clear only the raw-load task of an explicitly named Airflow run."""

import argparse
import json
import os

import requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    base = "http://airflow-api-server:8080"
    session = requests.Session()
    response = session.post(base + "/auth/token", json={"username": os.environ["AIRFLOW_ADMIN_USERNAME"], "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, timeout=20)
    response.raise_for_status()
    session.headers["Authorization"] = "Bearer " + response.json()["access_token"]
    body = {"dag_run_id": args.run_id, "task_ids": ["load_raw"], "dry_run": True, "reset_dag_runs": True, "only_failed": False, "only_running": False}
    url = base + "/api/v2/dags/wearable_daily_pipeline/clearTaskInstances"
    response = session.post(url, json=body, timeout=20)
    response.raise_for_status()
    preview = response.json()
    tasks = preview.get("task_instances", [])
    if len(tasks) != 1 or tasks[0]["task_id"] != "load_raw" or tasks[0]["dag_run_id"] != args.run_id:
        raise RuntimeError("Clear preview did not match exactly one requested raw-load task")
    print(json.dumps({"run_id": args.run_id, "task_id": "load_raw", "preview_count": len(tasks)}))
    if args.apply:
        if tasks[0].get("state") not in {"failed", "up_for_retry"}:
            raise RuntimeError("Only failed or retry-pending raw-load tasks may be cleared")
        body["dry_run"] = False
        response = session.post(url, json=body, timeout=20)
        response.raise_for_status()
        print("Raw-load task cleared for retry; captured inputs remain unchanged.")


if __name__ == "__main__":
    main()
