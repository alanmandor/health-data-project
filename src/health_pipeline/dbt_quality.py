"""Build internal staging and quality models without publishing consumer data."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def project_hash(project):
    digest = hashlib.sha256()
    for path in sorted(project.rglob("*")):
        if path.is_file() and path.suffix in (".sql", ".yml") and not any(
            part in {"target", "logs", "dbt_packages"} for part in path.relative_to(project).parts
        ):
            digest.update(path.relative_to(project).as_posix().encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return digest.hexdigest()


def input_hash(db):
    batches = db.execute("SELECT batch_id, count(*) FROM raw.batch_records GROUP BY batch_id ORDER BY batch_id").fetchall()
    return hashlib.sha256(json.dumps(batches, separators=(",", ":")).encode()).hexdigest()


def build(registration=None):
    project = Path(os.getenv("PROJECT_DBT_DIR", "/opt/airflow/project/dbt"))
    runtime = Path(os.getenv("PROJECT_DBT_RUNTIME", "/opt/airflow/project-data/dbt"))
    runtime.mkdir(parents=True, exist_ok=True)
    profiles = runtime / "profiles"
    profiles.mkdir(exist_ok=True)
    shutil.copyfile(project / "profiles.example.yml", profiles / "profiles.yml")
    variables = {
        "study_timezone": os.getenv("STUDY_TIMEZONE", "America/Bogota"),
        "quality_rule_version": "staging-quality-v1",
        "code_version": project_hash(project),
    }
    command = [os.getenv("DBT_EXECUTABLE", "/opt/airflow/dbt-venv/bin/dbt"), "build",
               "--project-dir", str(project), "--profiles-dir", str(profiles),
               "--target-path", str(runtime / "target"), "--log-path", str(runtime / "logs"),
               "--vars", json.dumps(variables)]
    # Merge output streams and drain continuously; dbt emits substantial stdout.
    subprocess.run(command, check=True)
    results = json.loads((runtime / "target" / "run_results.json").read_text())
    failures = [item["unique_id"] for item in results["results"]
                if item["status"] not in {"success", "pass", "warn"}]
    if failures:
        raise RuntimeError(f"dbt did not complete successfully: {failures}")
    import duckdb
    database = Path(os.environ["PROJECT_DATABASE_PATH"]).resolve()
    with duckdb.connect(str(database), read_only=True) as db:
        fingerprint = input_hash(db)
    validation = {**variables, "database": str(database), "input_hash": fingerprint,
                  "study_code": os.getenv("STUDY_CODE", "pmdata_2026_01")}
    temporary = runtime / "validated.json.tmp"
    temporary.write_text(json.dumps(validation), encoding="utf-8")
    temporary.replace(runtime / "validated.json")
    return {"status": "staging_quality_validated", "batch_id": (registration or {}).get("batch_id"),
            "code_version": variables["code_version"], "rule_version": variables["quality_rule_version"],
            "result_count": len(results["results"]), "consumer_published": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registration", default="{}")
    args = parser.parse_args()
    print(json.dumps(build(json.loads(args.registration))))


if __name__ == "__main__":
    main()
