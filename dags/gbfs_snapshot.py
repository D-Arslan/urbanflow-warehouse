"""Snapshot of the Vélib' GBFS station_status feed into the landing zone.

Every 5 minutes the raw JSON is written unchanged to
data/landing/gbfs/station_status/dt=YYYY-MM-DD/HHMM.json.
The name comes from the run (run_after), not the wall clock: a retry of the
same run overwrites the same file instead of adding a duplicate.
"""
import json
from datetime import timezone
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, get_current_context, task

STATION_STATUS_URL = (
    "https://velib-metropole-opendata.smovengo.cloud"
    "/opendata/Velib_Metropole/station_status.json"
)
LANDING_DIR = Path("/opt/airflow/data/landing/gbfs/station_status")


@dag(
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 10, 9, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30)},
    tags=["urbanflow", "ingestion"],
)
def gbfs_snapshot():
    @task
    def land_station_status() -> str:
        run_at = get_current_context()["dag_run"].run_after.astimezone(timezone.utc)
        response = requests.get(STATION_STATUS_URL, timeout=15)
        response.raise_for_status()
        if not json.loads(response.content)["data"]["stations"]:
            raise ValueError("GBFS payload has no stations")

        target = LANDING_DIR / f"dt={run_at:%Y-%m-%d}" / f"{run_at:%H%M}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".json.tmp")
        tmp.write_bytes(response.content)  # raw bytes, unchanged
        tmp.replace(target)  # atomic rename: readers never see a partial file
        return str(target)

    land_station_status()


gbfs_snapshot()
