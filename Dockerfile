# Airflow image extended with our task dependencies (pinned).
FROM apache/airflow:3.3.2-python3.12
COPY requirements-airflow.txt requirements-dbt.txt /tmp/
# Re-pinning apache-airflow prevents pip from silently up/downgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /tmp/requirements-airflow.txt
# dbt in its own venv: its pins never touch Airflow's (call /opt/airflow/dbt-venv/bin/dbt).
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
