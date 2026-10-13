"""Snapshots of the Vélib' GBFS feeds into the landing zone, raw and unchanged.

- gbfs_snapshot, every 5 minutes: station_status to
  data/landing/gbfs/station_status/dt=YYYY-MM-DD/HHMM.json
- gbfs_station_information, daily at 00:05 UTC: station_information (names, coordinates,
  capacity) to data/landing/gbfs/station_information/dt=YYYY-MM-DD/station_information.json;
  one copy per day keeps the history of the reference (renamed or resized stations).

Names come from the run (run_after, UTC), not the wall clock: a retry of the same run
overwrites the same file instead of adding a duplicate. A capture that happens too late for
its name is skipped (see skip_if_late).
"""
import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

GBFS_ROOT = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
LANDING_ROOT = Path("/opt/airflow/data/landing/gbfs")
DEFAULT_ARGS = {"retries": 2, "retry_delay": pendulum.duration(seconds=30)}
STATUS_MAX_DELAY = timedelta(seconds=240)  # under one 5-minute slot; a normal capture takes ~1 s


def run_at() -> datetime:
    return get_current_context()["dag_run"].run_after.astimezone(timezone.utc)


def skip_if_late(when: datetime, captured_at: datetime) -> None:
    """Skip a station_status capture that does not belong to its 5-minute slot.

    The file is named after the run (when) but holds the feed as fetched (captured_at). If the
    host sleeps through a run, the fetch happens at wake-up: 2026-10-11/1930.json held the feed
    of 2026-10-12 09:30. Skipping loses nothing: the run scheduled at wake-up captures the same
    feed under its own, correct name (1930 was identical to 2026-10-12/0930). Renaming the late
    capture to its real slot would only duplicate that run. The slot stays empty: an honest gap.
    """
    delay = captured_at - when
    if delay > STATUS_MAX_DELAY:
        raise AirflowSkipException(
            f"Captured {delay} after the run time {when:%Y-%m-%d %H:%M} UTC (max {STATUS_MAX_DELAY}): skipped"
        )


def skip_if_other_day(when: datetime, captured_at: datetime) -> None:
    """Skip a station_information capture made on another UTC day than its dt.

    The reference is daily: a copy taken late but on the same day (09:31 instead of 00:05)
    still describes that day; one taken the next day belongs to the next dt.
    """
    if captured_at.date() != when.date():
        raise AirflowSkipException(
            f"Captured on {captured_at:%Y-%m-%d}, not on its day {when:%Y-%m-%d}: skipped"
        )


def land_feed(
    feed: str, when: datetime, file_name: str, check: Callable[[datetime, datetime], None]
) -> str:
    """Fetch one GBFS feed and write its raw bytes to <feed>/dt=<UTC day of when>/<file_name>.

    check(when, captured_at) runs once the payload is in hand, before anything is written.
    """
    response = requests.get(f"{GBFS_ROOT}/{feed}.json", timeout=15)
    response.raise_for_status()
    check(when, datetime.now(timezone.utc))
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
        return land_feed("station_status", when, f"{when:%H%M}.json", skip_if_late)

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
        return land_feed("station_information", run_at(), "station_information.json", skip_if_other_day)

    land_station_information()


gbfs_snapshot()
gbfs_station_information()
