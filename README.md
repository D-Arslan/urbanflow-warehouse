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

## Known data gaps

Missing 5-minute bins, all stations at once. Every future gap is added here.

| Period (UTC, missing bins) | Cause | Source of the cause |
|---|---|---|
| 2026-07-01 12:20–16:05 (6 gaps, 25 bins); 2026-07-02 03:25–08:40 (64 bins) | Not recorded | None: UrbanFlow's docs and logs do not mention it |
| 2026-10-10 01:10–08:15 (86 bins; last snapshot 01:05, next 08:20) | Windows Update restarted the host at 01:07; Docker Desktop did not restart and the services had no restart policy | Windows System log, event 1074 (`MoUsoCoreWorker`, `TrustedInstaller`) |

Since 2026-10-10 the long-running services use `restart: unless-stopped`, so they come back
whenever Docker Desktop starts.

## Data provenance: raw.velib_grid_2026_07

Derived historical backfill, not GBFS snapshots: the 5-minute grid built by UrbanFlow's
`build_grid` from the July 2026 Kafka collection. Imported once with
`scripts/import_velib_grid_2026_07.py`, loaded by the manual DAG `load_velib_grid_2026_07`.

| | |
|---|---|
| Source | `UrbanFlow/ml/data/grid/part-00000-b760a509-…-c000.snappy.parquet` |
| Source date | 2026-07-05T10:55:58Z (file mtime) |
| Size | 7 268 200 bytes |
| SHA-256 | `b885ab58619e94e46e0ceaa4a66a3f3317b845b9206cf483369eed3663929b3a` |
| Schema | `station_id BIGINT, station_code VARCHAR, bin_epoch BIGINT, ts TIMESTAMP, bikes INTEGER, docks INTEGER` |
| Coverage | 1 602 412 rows, 1516 stations, 2026-07-01 11:25 → 2026-07-05 10:50 |
| Checks | `ts = bin_epoch` on every row; `(station_id, bin_epoch)` unique |

Every 5-minute bin present holds all 1516 stations; a missing bin is missing for all of them.

| dt | rows | bins (rows / 1516) | expected bins | missing bins (UTC) |
|---|---|---|---|---|
| 2026-07-01 | 191 016 | 126 | 151 (11:25 → 23:55) | 25: 12:20–12:35, 12:55–13:40, 13:55–14:00, 14:15–14:20, 14:30–14:35, 15:45–16:05 |
| 2026-07-02 | 339 584 | 224 | 288 | 64: 03:25–08:40 |
| 2026-07-03 | 436 608 | 288 | 288 | none |
| 2026-07-04 | 436 608 | 288 | 288 | none |
| 2026-07-05 | 198 596 | 131 | 131 (00:00 → 10:50) | none |

07-03 and 07-04 are complete (1516 stations × 288 bins); 07-01 and 07-02 have collection gaps
(cause not recorded in UrbanFlow). A completeness test will flag these in the dbt layer.

The UrbanFlow README reports 1 566 818 rows / 1510 stations / end 10:35: that is the filtered
ML dataset derived from this grid (stations without continuous history removed), not the grid
itself.

`ts` is naive UTC. Partitions `dt=YYYY-MM-DD` are the **UTC date of ts** (not Paris time),
aligned with the `dt=` partitions of the GBFS landing zone. Files live in
`data/raw/velib_grid_2026_07/`, which is not versioned.
