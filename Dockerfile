# Airflow image extended with our task dependencies (pinned).
FROM apache/airflow:3.3.2-python3.12
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
# Re-pinning apache-airflow prevents pip from silently up/downgrading Airflow itself.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /tmp/requirements-airflow.txt
