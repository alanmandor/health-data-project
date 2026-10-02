FROM apache/airflow:3.1.7-python3.12

COPY requirements.txt requirements-dbt.txt /opt/project-dependencies/
RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt \
    -r /opt/project-dependencies/requirements.txt \
    && python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir \
    -r /opt/project-dependencies/requirements-dbt.txt \
    && pip check \
    && /opt/airflow/dbt-venv/bin/pip check

ENV DBT_EXECUTABLE=/opt/airflow/dbt-venv/bin/dbt
COPY --chown=airflow:root infra/initialize.sh /opt/airflow/initialize.sh
# Normalize scripts checked out with Windows line endings.
RUN sed -i 's/\r$//' /opt/airflow/initialize.sh
