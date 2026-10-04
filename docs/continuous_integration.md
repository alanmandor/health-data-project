# Continuous Integration

The [Python application workflow](../.github/workflows/python-app.yml) runs on
pushes to `main`, pull requests, and manual requests from GitHub's Actions tab.
It builds the same Airflow/dbt Docker runtime used locally, rather than installing
both dependency sets into one Python environment.

## Checks

The shared [check script](../scripts/verify_ci.sh) fails on the first unsuccessful
command and verifies:

- Generated DuckDB and Snowflake SQL matches the canonical generator.
- All nine foundation tests, including catalog equivalences, domain constraints,
  and transactional migration behavior.
- Ten calendar and delivery-control tests.
- Fourteen raw-ingestion tests using synthetic deliveries, including retry,
  rollback, and archival recovery.
- Both complete DAGs import successfully and include their final publication tasks.

The workflow uses temporary test databases and does not download the supplied study
data, publish analytical outputs, start Airflow services, or access a cloud account.
It needs no repository secrets. Repository checkout has read-only permissions,
the source mount is read-only, and newer runs cancel older checks for the same ref.

## Evaluator Use and Limits

Open the repository's **Actions** tab and select **Python application** to inspect
individual check output and the tested commit. A successful run establishes these
checks only; it does not execute the full dbt correction suite, run DAG tasks, test
Snowflake execution, or demonstrate a complete startup from fresh Docker volumes.
Those checks remain documented separately in [verification results](verification_results.md).

Builds require network access to Docker and Python package registries. Dependencies
retain the build-reproducibility limits described in
[scope and limitations](scope_and_limitations.md). The workflow does not configure
branch protection or require a passing check before merging.

The shared check script passed locally in a new isolated container: nine foundation,
ten control, and fourteen raw-ingestion tests passed, generated DDL matched, and both
complete DAGs imported without errors. This checks the runtime commands; the GitHub
hosted run and image build status must be read from the Actions tab after publication.
