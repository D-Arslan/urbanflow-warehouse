"""One-off load of the July 2026 derived grid into DuckDB (raw.velib_grid_2026_07).

Historical backfill, not GBFS snapshots: the 5-minute grid built by UrbanFlow from
2026-07-01 to 2026-07-05 (see README, Data provenance). dt is the UTC date of ts.
Triggered by hand (schedule=None). CREATE OR REPLACE rebuilds the whole table
atomically: a re-run yields the same table, a failed run leaves the previous one.
"""
import os
from pathlib import Path

import duckdb
import pendulum
from airflow.sdk import dag, task

from warehouse_assets import RAW_GRID

GRID_DIR = Path("/opt/airflow/data/raw/velib_grid_2026_07")
WAREHOUSE_PATH = os.environ["WAREHOUSE_PATH"]
EXPECTED_DAYS = 5  # frozen source: 2026-07-01 to 2026-07-05

LOAD_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE OR REPLACE TABLE raw.velib_grid_2026_07 AS
SELECT
    station_id, station_code, bin_epoch, ts, bikes, docks,
    CAST(dt AS DATE) AS dt,       -- UTC date of ts, from the dt= partition
    filename         AS _source_file,
    CAST(now() AT TIME ZONE 'UTC' AS TIMESTAMP) AS _loaded_at  -- naive UTC, like ts
FROM read_parquet('{pattern}', hive_partitioning = true, filename = true);
"""


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 9, tz="UTC"),
    catchup=False,
    tags=["urbanflow", "load", "backfill"],
)
def load_velib_grid_2026_07():
    @task(pool="duckdb_writer", outlets=[RAW_GRID])
    def load_grid() -> dict:
        if not any(GRID_DIR.glob("dt=*/*.parquet")):
            raise FileNotFoundError(f"No Parquet partition in {GRID_DIR}: run scripts/import_velib_grid_2026_07.py first")

        with duckdb.connect(WAREHOUSE_PATH) as con:
            con.execute(LOAD_SQL.format(pattern=f"{GRID_DIR}/dt=*/*.parquet"))
            rows, days = con.execute(
                "SELECT count(*), count(DISTINCT dt) FROM raw.velib_grid_2026_07"
            ).fetchone()
        print(f"raw.velib_grid_2026_07: {rows} rows, {days} distinct dt (expected {EXPECTED_DAYS})")
        if rows == 0 or days != EXPECTED_DAYS:
            raise ValueError(f"Unexpected load: {rows} rows, {days} distinct dt (expected {EXPECTED_DAYS})")
        return {"rows": rows, "days": days}

    load_grid()


load_velib_grid_2026_07()
