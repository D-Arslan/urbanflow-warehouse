"""Daily load of the GBFS landing zone into DuckDB (raw.gbfs_station_status).

One run loads one UTC day: every snapshot under dt=YYYY-MM-DD/ becomes one row per
station and snapshot. The day is replaced in a single transaction (DELETE then
INSERT), so re-running or backfilling a day yields the same rows.
Writes go through the duckdb_writer pool (1 slot): DuckDB allows a single writer.
"""
import os
from pathlib import Path

import duckdb
import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

LANDING_DIR = Path("/opt/airflow/data/landing/gbfs/station_status")
WAREHOUSE_PATH = os.environ["WAREHOUSE_PATH"]

# Explicit contract on the source: unknown fields are ignored, missing ones become NULL.
# Names stay as in GBFS; renaming and casting belong to the dbt staging layer.
STATIONS_TYPE = (
    "STRUCT(stations STRUCT("
    "station_id BIGINT, stationCode VARCHAR, "
    "num_bikes_available INTEGER, num_docks_available INTEGER, "
    "num_bikes_available_types JSON, "
    "is_installed INTEGER, is_renting INTEGER, is_returning INTEGER, "
    "last_reported BIGINT)[])"
)

CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.gbfs_station_status (
    dt                        DATE      NOT NULL,
    _snapshot_at              TIMESTAMP NOT NULL,  -- UTC, from the landing file name
    station_id                BIGINT,
    stationCode               VARCHAR,
    num_bikes_available       INTEGER,
    num_docks_available       INTEGER,
    num_bikes_available_types JSON,
    is_installed              INTEGER,
    is_renting                INTEGER,
    is_returning              INTEGER,
    last_reported             BIGINT,              -- epoch seconds, as delivered
    _source_file              VARCHAR   NOT NULL,
    _loaded_at                TIMESTAMPTZ NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO raw.gbfs_station_status
SELECT
    $day::DATE,
    strptime($day || regexp_extract(filename, '([0-9]{{4}})\\.json$', 1), '%Y-%m-%d%H%M'),
    s.station_id, s.stationCode, s.num_bikes_available, s.num_docks_available,
    s.num_bikes_available_types, s.is_installed, s.is_renting, s.is_returning,
    s.last_reported,
    filename,
    current_timestamp
FROM (
    SELECT filename, unnest(data.stations) AS s
    FROM read_json('{pattern}', columns = {{'data': '{stations_type}'}},
                   filename = true, hive_partitioning = false)
)
"""


@dag(
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 9, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=2)},
    tags=["urbanflow", "load"],
)
def daily_load():
    @task(pool="duckdb_writer")
    def load_station_status_day() -> int:
        day = get_current_context()["data_interval_start"].date()
        day_dir = LANDING_DIR / f"dt={day:%Y-%m-%d}"
        if not any(day_dir.glob("*.json")):
            raise FileNotFoundError(f"No GBFS snapshot landed for {day} in {day_dir}")

        insert = INSERT_SQL.format(pattern=f"{day_dir}/*.json", stations_type=STATIONS_TYPE)
        # Closing the connection without COMMIT rolls back: a failed run leaves the day untouched.
        with duckdb.connect(WAREHOUSE_PATH) as con:
            con.execute(CREATE_SQL)
            con.execute("BEGIN TRANSACTION")
            con.execute("DELETE FROM raw.gbfs_station_status WHERE dt = ?", [day])
            con.execute(insert, {"day": f"{day:%Y-%m-%d}"})
            rows = con.execute(
                "SELECT count(*) FROM raw.gbfs_station_status WHERE dt = ?", [day]
            ).fetchone()[0]
            con.execute("COMMIT")
        print(f"raw.gbfs_station_status: {rows} rows for dt={day}")
        return rows

    load_station_status_day()


daily_load()
