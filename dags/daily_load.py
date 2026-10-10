"""Daily load of the GBFS landing zone into DuckDB.

One run loads one UTC day D, in two independent tasks:
- raw.gbfs_station_status: every snapshot under station_status/dt=D/ becomes one row per
  station and snapshot;
- raw.gbfs_station_information: the reference copy of day D, one row per station; dt keeps
  the daily history (renamed or resized stations stay visible).
Each table's day is replaced in a single transaction (DELETE then INSERT), so re-running or
backfilling a day yields the same rows.
Writes go through the duckdb_writer pool (1 slot): DuckDB allows a single writer.
"""
import os
from datetime import date
from pathlib import Path

import duckdb
import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.interval import CronDataIntervalTimetable

LANDING_ROOT = Path("/opt/airflow/data/landing/gbfs")
WAREHOUSE_PATH = os.environ["WAREHOUSE_PATH"]
STATION_INFORMATION_SINCE = date(2026, 10, 10)  # first day with a landed station_information

# Explicit contracts on the sources: unknown fields are ignored, missing ones become NULL.
# Names stay as in GBFS; renaming and casting belong to the dbt staging layer.
STATUS_STATIONS_TYPE = (
    "STRUCT(stations STRUCT("
    "station_id BIGINT, stationCode VARCHAR, "
    "num_bikes_available INTEGER, num_docks_available INTEGER, "
    "num_bikes_available_types JSON, "
    "is_installed INTEGER, is_renting INTEGER, is_returning INTEGER, "
    "last_reported BIGINT)[])"
)
INFO_STATIONS_TYPE = (
    "STRUCT(stations STRUCT("
    "station_id BIGINT, stationCode VARCHAR, name VARCHAR, "
    "lat DOUBLE, lon DOUBLE, capacity INTEGER, "
    "rental_methods JSON, station_opening_hours JSON)[])"
)

CREATE_STATUS_SQL = """
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
    _loaded_at                TIMESTAMP NOT NULL   -- naive UTC, like _snapshot_at
);
"""

INSERT_STATUS_SQL = """
INSERT INTO raw.gbfs_station_status
SELECT
    $day::DATE,
    strptime($day || regexp_extract(filename, '([0-9]{{4}})\\.json$', 1), '%Y-%m-%d%H%M'),
    s.station_id, s.stationCode, s.num_bikes_available, s.num_docks_available,
    s.num_bikes_available_types, s.is_installed, s.is_renting, s.is_returning,
    s.last_reported,
    filename,
    CAST(now() AT TIME ZONE 'UTC' AS TIMESTAMP)
FROM (
    SELECT filename, unnest(data.stations) AS s
    FROM read_json('{pattern}', columns = {{'data': '{stations_type}'}},
                   filename = true, hive_partitioning = false)
)
"""

CREATE_INFO_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.gbfs_station_information (
    dt                    DATE      NOT NULL,
    station_id            BIGINT,
    stationCode           VARCHAR,
    name                  VARCHAR,
    lat                   DOUBLE,
    lon                   DOUBLE,
    capacity              INTEGER,
    rental_methods        JSON,
    station_opening_hours JSON,
    lastUpdatedOther      BIGINT,              -- feed-level epoch seconds, as delivered
    _source_file          VARCHAR   NOT NULL,
    _loaded_at            TIMESTAMP NOT NULL   -- naive UTC
);
"""

INSERT_INFO_SQL = """
INSERT INTO raw.gbfs_station_information
SELECT
    $day::DATE,
    s.station_id, s.stationCode, s.name, s.lat, s.lon, s.capacity,
    s.rental_methods, s.station_opening_hours,
    lastUpdatedOther,
    filename,
    CAST(now() AT TIME ZONE 'UTC' AS TIMESTAMP)
FROM (
    SELECT filename, lastUpdatedOther, unnest(data.stations) AS s
    FROM read_json('{path}',
                   columns = {{'lastUpdatedOther': 'BIGINT', 'data': '{stations_type}'}},
                   filename = true, hive_partitioning = false)
)
"""


def interval_day() -> date:
    return get_current_context()["data_interval_start"].date()


def replace_day(create_sql: str, table: str, insert_sql: str, day: date) -> int:
    """Replace one dt of a raw table in a single transaction and return its row count."""
    # Closing the connection without COMMIT rolls back: a failed run leaves the day untouched.
    with duckdb.connect(WAREHOUSE_PATH) as con:
        con.execute(create_sql)
        con.execute("BEGIN TRANSACTION")
        con.execute(f"DELETE FROM {table} WHERE dt = ?", [day])
        con.execute(insert_sql, {"day": f"{day:%Y-%m-%d}"})
        rows = con.execute(f"SELECT count(*) FROM {table} WHERE dt = ?", [day]).fetchone()[0]
        con.execute("COMMIT")
    print(f"{table}: {rows} rows for dt={day}")
    return rows


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
        day = interval_day()
        day_dir = LANDING_ROOT / "station_status" / f"dt={day:%Y-%m-%d}"
        if not any(day_dir.glob("*.json")):
            raise FileNotFoundError(f"No GBFS snapshot landed for {day} in {day_dir}")
        insert = INSERT_STATUS_SQL.format(pattern=f"{day_dir}/*.json", stations_type=STATUS_STATIONS_TYPE)
        return replace_day(CREATE_STATUS_SQL, "raw.gbfs_station_status", insert, day)

    @task(pool="duckdb_writer")
    def load_station_information_day() -> int:
        day = interval_day()
        if day < STATION_INFORMATION_SINCE:
            raise AirflowSkipException(f"station_information is collected since {STATION_INFORMATION_SINCE}")
        path = LANDING_ROOT / "station_information" / f"dt={day:%Y-%m-%d}" / "station_information.json"
        if not path.exists():
            raise FileNotFoundError(f"No station_information landed for {day}: {path}")
        insert = INSERT_INFO_SQL.format(path=path, stations_type=INFO_STATIONS_TYPE)
        return replace_day(CREATE_INFO_SQL, "raw.gbfs_station_information", insert, day)

    load_station_status_day()
    load_station_information_day()


daily_load()
