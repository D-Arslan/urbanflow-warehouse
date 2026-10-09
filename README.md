# UrbanFlow Warehouse

Batch data warehouse extending [UrbanFlow](https://github.com/D-Arslan/UrbanFlow), a real-time
Vélib' pipeline: Airflow lands GBFS snapshots, DuckDB stores them, dbt models them.

**Status: work in progress.**
- Sprint 1 (in progress): Airflow 3 (LocalExecutor) and DuckDB in place; `gbfs_snapshot` DAG
  lands the raw `station_status` feed every 5 minutes into a date-partitioned landing zone.
- Sprint 2: dbt (staging / intermediate / marts, data quality and freshness tests).

## Run locally

    cp .env.example .env      # then set real secrets
    docker compose build
    docker compose up -d      # UI on http://127.0.0.1:8080

## Known data gap: July 2026 raw JSON is lost

The raw GBFS messages collected by UrbanFlow in July 2026 no longer exist. Kafka kept them for
20 days, and the MinIO cold store holds no July partition: the streaming job did not run
continuously during the collection and partitioned on the feed's `last_reported` instead of
capture time. What survives is a derived 5-minute grid (2026-07-01 to 2026-07-05). It will be
loaded as a separate table, `raw.velib_grid_2026_07`, with its own grain and schema, and
reconciled with the live GBFS snapshots in a dbt staging model.
