"""Snapshots of the Vélib' GBFS feeds into the landing zone, raw and unchanged.

- gbfs_snapshot, every 5 minutes: station_status to
  data/landing/gbfs/station_status/dt=YYYY-MM-DD/HHMM.json
- gbfs_station_information, daily at 00:05 UTC: station_information (names, coordinates,
  capacity) to data/landing/gbfs/station_information/dt=YYYY-MM-DD/station_information.json;
  one copy per day keeps the history of the reference (renamed or resized stations).

Names come from the run (run_after, UTC), not the wall clock: a retry of the same run
overwrites the same file instead of adding a duplicate.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, get_current_context, task

GBFS_ROOT = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
LANDING_ROOT = Path("/opt/airflow/data/landing/gbfs")
DEFAULT_ARGS = {"retries": 2, "retry_delay": pendulum.duration(seconds=30)}


def run_at() -> datetime:
    return get_current_context()["dag_run"].run_after.astimezone(timezone.utc)


def land_feed(feed: str, when: datetime, file_name: str) -> str:
    """Fetch one GBFS feed and write its raw bytes to <feed>/dt=<UTC day of when>/<file_name>."""
    response = requests.get(f"{GBFS_ROOT}/{feed}.json", timeout=15)
    response.raise_for_status()
    if not json.loads(response.content)["data"]["stations"]:
        raise ValueError(f"GBFS {feed} payload has no stations")

    target = LANDING_ROOT / feed / f"dt={when:%Y-%m-%d}" / file_name
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.tmp")
    tmp.write_bytes(response.content)  # raw bytes, unchanged
    tmp.replace(target)  # atomic rename: readers never see a partial file
    return str(target)


@dag(
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 10, 9, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["urbanflow", "ingestion"],
)
def gbfs_snapshot():
    @task
    def land_station_status() -> str:
        when = run_at()
        return land_feed("station_status", when, f"{when:%H%M}.json")

    land_station_status()


@dag(
    schedule="5 0 * * *",
    start_date=pendulum.datetime(2026, 10, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["urbanflow", "ingestion"],
)
def gbfs_station_information():
    @task
    def land_station_information() -> str:
        return land_feed("station_information", run_at(), "station_information.json")

    land_station_information()


gbfs_snapshot()
gbfs_station_information()
