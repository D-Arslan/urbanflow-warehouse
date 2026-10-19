"""Airflow assets of the warehouse: one per raw table that triggers the dbt build.

Producers (outlets) and the consumer (schedule) import them from here, so both sides always
refer to the same asset.
"""
from airflow.sdk import Asset

RAW_GBFS = Asset(name="warehouse.raw.gbfs", uri="duckdb://warehouse/raw/gbfs")
RAW_GRID = Asset(name="warehouse.raw.velib_grid_2026_07", uri="duckdb://warehouse/raw/velib_grid_2026_07")
