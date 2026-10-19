"""dbt build of the warehouse, triggered by the raw loads (Airflow assets).

Runs when daily_load or load_velib_grid_2026_07 has updated raw. Three tasks, all in the
duckdb_writer pool: DuckDB allows one writer, and refuses even a reader while a load writes.
- dbt_source_freshness: data age of raw.gbfs_station_status (_snapshot_at; warn 2 h, error 26 h).
  A warning exits 0; an error fails the task.
- dbt_build: models and tests. Runs even if freshness failed (all_done): the marts stay useful on
  the data already loaded.
- dbt_docs_generate: static docs with the lineage (dbt/target/static_index.html), only after a
  successful build.
- run_failed_watcher: Airflow sets a run's state from its leaf tasks, so a failed freshness check
  followed by a successful build and docs would leave the run green. The watcher runs only if a
  task failed (one_failed) and then fails itself, so the run shows as failed; otherwise it is
  skipped and the run stays successful.
"""
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from warehouse_assets import RAW_GBFS, RAW_GRID

DBT = "/opt/airflow/dbt-venv/bin/dbt"
PROJECT = "--project-dir /opt/airflow/dbt"


@dag(
    schedule=(RAW_GBFS | RAW_GRID),
    start_date=pendulum.datetime(2026, 10, 18, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # do_xcom_push=False: the output is never used, and a slow XCom write retried by the task
    # client once failed a successful dbt build on a duplicate key (2026-10-19).
    default_args={"pool": "duckdb_writer", "retries": 0, "do_xcom_push": False},
    tags=["urbanflow", "dbt"],
)
def dbt_build():
    freshness = BashOperator(task_id="dbt_source_freshness", bash_command=f"{DBT} source freshness {PROJECT}")
    build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build {PROJECT}", trigger_rule="all_done")
    docs = BashOperator(task_id="dbt_docs_generate", bash_command=f"{DBT} docs generate --static {PROJECT}")

    @task(trigger_rule="one_failed", pool="default_pool")
    def run_failed_watcher() -> None:
        raise RuntimeError("An upstream task failed (stale sources or a dbt error): failing the run so it shows.")

    freshness >> build >> docs
    [freshness, build, docs] >> run_failed_watcher()


dbt_build()
