# Airflow Phase One: Historical Milestone

The original milestone implemented immutable delivery registration and study calendar inspection. Those limited behaviors have been superseded by the complete [daily pipeline](daily_analytics.md) and [weekly frozen biostatistics](weekly_biostatistics.md). Both DAGs remain manual while production scheduling settings and a producer inventory are agreed.

The daily DAG now captures raw files, archives processed inputs, evaluates quality, appends Vault history, updates dimensions/facts, and publishes validated daily analytics and readiness. The weekly DAG waits for identified delivery readiness, freezes input selections and the cutoff, and publishes revisioned biostatistics. A successful weekly trigger therefore publishes data; it is no longer a calendar-only demonstration.

The compatibility script retains its original filename but now performs read-only DAG import checks:

```bash
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/verify_airflow_phase_one.py
```

To exercise both workflows twice without changing the real warehouse, use isolated test storage:

```bash
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/verify_raw_airflow.py
```

Pause both DAGs and finish any active runs before this verification. The script restores no source files in the real incoming directory and removes its isolated temporary storage after successful verification. Follow [Docker setup](docker_quickstart.md), [baseline ingestion](raw_ingestion.md), and [weekly execution](weekly_biostatistics.md) for the full exercise.
