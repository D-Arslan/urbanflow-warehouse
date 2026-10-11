# UrbanFlow Warehouse

Batch data warehouse extending [UrbanFlow](https://github.com/D-Arslan/UrbanFlow), a real-time
Vélib' pipeline: Airflow lands GBFS snapshots, DuckDB stores them, dbt models them.

**Status: work in progress.**
- Sprint 1 (done, 2026-10-10): Airflow 3 (LocalExecutor) and DuckDB, three DAGs:
  - `gbfs_snapshot`, every 5 minutes: lands the raw `station_status` JSON, unchanged, to
    `data/landing/gbfs/station_status/dt=YYYY-MM-DD/HHMM.json` (atomic write, name from the run).
  - `daily_load`, daily at 00:00 UTC: replaces one UTC day in `raw.gbfs_station_status`
    (DELETE + INSERT in one transaction); fails if the day has no snapshot.
  - `load_velib_grid_2026_07`, manual: rebuilds `raw.velib_grid_2026_07` (CREATE OR REPLACE);
    fails unless it loads 5 distinct days.
  
  DuckDB writers share the `duckdb_writer` pool (1 slot): one writer at a time.
- Sprint 2 (next): dbt (staging / intermediate / marts, data quality and freshness tests).

Invariants verified at the close of Sprint 1:

| Invariant | Check | Result |
|---|---|---|
| `daily_load` is idempotent | 2026-10-09 loaded by the scheduled run, then by two `airflow backfill create --reprocess-behavior completed` | 281 015 rows each time = sum of stations over the 185 landed files; 0 duplicate (station, snapshot) |
| `load_velib_grid_2026_07` is idempotent | two runs | 1 602 412 rows and 5 distinct `dt` each time |
| July grid is consistent | import script checks | `ts = bin_epoch` on every row, `(station_id, bin_epoch)` unique; coverage per day below |

## Run locally

    cp .env.example .env      # then set real secrets
    docker compose build
    docker compose up -d      # UI on http://127.0.0.1:8080
    # DAGs start paused:
    docker compose exec airflow-scheduler airflow dags unpause gbfs_snapshot
    docker compose exec airflow-scheduler airflow dags unpause daily_load
    # dbt (own venv in the image; exec, not run, so the Airflow entrypoint is bypassed):
    docker compose exec airflow-scheduler /opt/airflow/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt

The July grid is optional: import it with `scripts/import_velib_grid_2026_07.py` (command in its
docstring), then trigger `load_velib_grid_2026_07`.

## Known data gap: July 2026 raw JSON is lost

The raw GBFS messages collected by UrbanFlow in July 2026 no longer exist. Kafka kept them for
20 days, and the MinIO cold store holds no July partition: the streaming job did not run
continuously during the collection and partitioned on the feed's `last_reported` instead of
capture time. What survives is a derived 5-minute grid (2026-07-01 to 2026-07-05). It is
loaded as a separate table, `raw.velib_grid_2026_07`, with its own grain and schema, and will be
reconciled with the live GBFS snapshots in the dbt layer.

## Known data gaps

Missing 5-minute bins, all stations at once. Every future gap is added here.

| Period (UTC, missing bins) | Cause | Source of the cause |
|---|---|---|
| 2026-07-01 12:20–16:05 (6 gaps, 25 bins); 2026-07-02 03:25–08:40 (64 bins) | Not recorded | None: UrbanFlow's docs and logs do not mention it |
| 2026-10-10 01:10–08:15 (86 bins; last snapshot 01:05, next 08:20) | Windows Update restarted the host at 01:07; Docker Desktop did not restart and the services had no restart policy | Windows System log, event 1074 (`MoUsoCoreWorker`, `TrustedInstaller`) |

Since 2026-10-10 the long-running services use `restart: unless-stopped`, so they come back
whenever Docker Desktop starts.

Collection policy: 2026-10-11 is collected as a complete day; after that, collection is
opportunistic (it runs while the host is on). The reference dataset for analysis remains the
July 2026 grid.

dbt source freshness warns when the laptop has been asleep: collection is opportunistic by
design, see Known data gaps.

## Known data quirks

Facts about the source feeds, measured in `raw`; the dbt layer handles them explicitly.

| Quirk | Measured | Handling |
|---|---|---|
| Degraded `station_status` snapshots: `stationCode` null and `num_bikes_available_types` empty (`[{},{}]`) for every station | 4 snapshots on 2026-10-09 (08:34, 12:35, 13:35, 14:35), 6076 rows; none on 2026-10-10 | staging keeps them, with the mechanical / e-bike split as null (unknown, not zero); `station_code` rebuilt downstream from `station_information` (join on `station_id`) |
| Stations with `capacity = 0` in `station_information` | 2 of 1519 on 2026-10-10 (`21502` Verdun - Place Henri IV, `23404` Bouvier - Carnot); both report `is_installed = 0`, 0 bikes and 0 docks in every snapshot of 2026-10-09/10; `21502` was active in the July grid | the dbt test `bikes + docks <= capacity` runs with `severity: warn` and excludes `capacity = 0` |

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
