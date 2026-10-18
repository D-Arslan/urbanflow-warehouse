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
- Sprint 2 (in progress): `gbfs_station_information` DAG (daily reference copy) and a dbt
  project (staging / intermediate / marts, data tests, source freshness), see Data model.

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

## Data model (dbt)

Layers: `staging` (views, one per raw table: rename, cast, no join) → `intermediate`
(`int_station_status_5min`: GBFS snapshots and July grid unioned on the grain station × 5-minute
bin, UTC, with a `source` column) → `marts` (tables with enforced contracts, read by a dashboard).
All times are naive UTC; `hour_of_day_local` is the only Paris-time column.

| Mart | Grain (one row =) | Key | Built from |
|---|---|---|---|
| `dim_station` | a station ever observed in any source | `station_id` | latest `station_information` copy + every station seen in the snapshots or the grid |
| `fct_station_availability_hourly` | a station × a UTC hour with at least one bin | (`station_id`, `hour_ts`) | `int_station_status_5min` |
| `fct_daily_coverage` | a UTC day × a source | (`coverage_date`, `source`) | `int_station_status_5min` |

Rules:
- **No row for an hour without any bin.** A fact records observations; the coverage of the
  collection has its own table, `fct_daily_coverage`. Partial hours keep their row and show it in
  `bins_present` (1–12).
- **Closed bins are excluded.** A bin with 0 bikes and 0 docks would be both empty and full: it is
  counted in `bins_closed` and left out of the averages, `bins_empty`, `bins_full` and the shares.
  Integer counters sit next to the shares so the dashboard can re-aggregate at any grain
  (`sum(bins_empty) / sum(bins_present - bins_closed)`).
- **0/0 in the July grid.** The grid has no `is_installed` field: a 0-bike, 0-dock bin cannot be
  told apart from a value UrbanFlow recorded as 0. It is counted as closed in both sources.
- **`dim_station.in_latest_reference`** means present in the most recent `station_information`
  copy, not in service: out-of-service stations have `capacity = 0`. The 3 stations seen only in
  the July grid (`1022`, `10037`, `51003`) are `false` and have no name, coordinates or capacity.
- **Reference changes stay visible.** `station_information` is kept per day: station `40010`
  (Haut du Mont-Mesly) went from a capacity of 21 to 0 on 2026-10-13.
- **`fct_daily_coverage`**: `pct_stations_complete` is 1.0 on every day so far (each snapshot holds
  every station), and `is_current_day` is never true because `daily_load` loads day D on D+1; both
  are kept for the cases described in the model's `.yml`.

Tests: `unique` / `not_null` on every key, `relationships` from the hourly fact to `dim_station`,
`accepted_values`, a custom generic test `non_negative`, singular tests (snapshot vs feed time,
counter consistency, reference flag, sources not overlapping, daily completeness as a warning), and
a unit test of `hour_of_day_local` across the change to winter time on 2026-10-25.

Materialization: tables, not incremental. Measured on one run on 2026-10-18, a full `dbt build`
(7 models, 59 data tests, 1 unit test) takes 8.21 s; `int_station_status_5min` (3 018 120 rows)
builds in 0.78 s and the hourly fact (282 258 rows) in 0.73 s. Switch the hourly fact to incremental
(`delete+insert` on the day, the rule `daily_load` already follows) when a full build exceeds
5 minutes.

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
| 2026-10-11 10:20–19:00 (105 bins), 19:30–23:55 (54 bins); includes the 3 quarantined slots (Known data quirks) | Host asleep (Windows modern standby) | System log, Kernel-Power 506/42 |
| 2026-10-12 00:00–09:25 (114 bins) | Host asleep (Windows modern standby) | System log, Kernel-Power 506/42 |
| 2026-10-12 13:10–23:55 (130 bins); 2026-10-13 00:00–09:20 (113 bins). Count on 2026-10-12: 114 missing 00:00–09:25 (row above) + 44 present 09:30–13:05 + 130 missing 13:10–23:55 = 288 | Host asleep (Windows modern standby) | System log, Kernel-Power 506 (sleep at 13:06 UTC), Power-Troubleshooter 1 (wake at 09:27 UTC) |
| 2026-10-13 13:30–18:25 (60 bins) | Host asleep (Windows modern standby) | System log, Kernel-Power 506 at 13:28 UTC, 507 at 18:30 UTC |
| 2026-10-13 20:35–23:55 (41 bins); 2026-10-14 00:00–08:15 (100 bins) | Host asleep (Windows modern standby) | System log, Kernel-Power 506 at 20:32 UTC; brief wake at 03:27 then sleep (42); 507 at 08:20 UTC |
| 2026-10-14 12:30–13:50 (17 bins) | Host asleep (Windows modern standby) | System log, Kernel-Power 506 at 12:27 UTC, 507 at 13:57 UTC |
| 2026-10-14 14:10–23:55 (118 bins); 2026-10-15 00:00–09:50 (119 bins) | Host asleep (Windows modern standby) | System log, Kernel-Power 506 at 14:08 UTC; brief wake at 20:23 then sleep (42); Power-Troubleshooter 1 at 09:56 UTC |
| 2026-10-15 14:35–16:45 (27 bins) | Host asleep (Windows modern standby) | System log, Kernel-Power 506 at 14:30 UTC, 507 at 16:49 UTC |
| 2026-10-15 19:20–23:55 (56 bins); 2026-10-16 00:00–08:50 (107 bins) | Host asleep on battery (unplugged) | System log, Kernel-Power 105 `AcOnline=false` at 19:16 UTC, 506 at 19:16, 42 (hibernate) at 01:47; Power-Troubleshooter 1 at 08:54 |
| 2026-10-16 19:15–23:55 (57 bins); 2026-10-17 00:00–02:20 (29 bins) | Host asleep on battery (unplugged) | System log, Kernel-Power 105 `AcOnline=false` at 19:10 UTC, 506 at 19:10; brief wake at 02:26 |
| 2026-10-17 02:30–11:20 (107 bins) | Host asleep on battery (unplugged) | System log, Kernel-Power 42 (hibernate) at 02:27 UTC; Power-Troubleshooter 1 at 11:25 |
| 2026-10-17 11:40–18:40 (85 bins) | Host asleep on battery (unplugged) | System log, Kernel-Power 506 at 11:35 UTC, 507 at 18:46 |
| 2026-10-17 21:20–23:55 (32 bins); 2026-10-18 00:00–09:15 (112 bins) | Host asleep on battery (unplugged) | System log, Kernel-Power 42 (hibernate) at 21:16 UTC; 105 `AcOnline=true` at 09:21 |

No collector error over these periods: no failed or retried `gbfs_snapshot` task, containers up
without restart. At each wake-up the late runs were skipped by the guard (8 runs from 2026-10-13
to 2026-10-15) and `daily_load` caught up the missed nights.

Decision (2026-10-15): on mains power the host no longer sleeps (sleep: never, lid close: do
nothing), so collection is continuous on mains power from the evening of 2026-10-15. It stays
best effort on a laptop; `fct_daily_coverage` measures the coverage actually obtained.

No gap on AC power since 15 Oct evening: every gap from 2026-10-15 19:20 to 2026-10-18 09:15
started after the host was unplugged (Kernel-Power 105 `AcOnline=false`) and ended when it woke
up; the sleep settings on battery are left as they are on a laptop. 2026-10-18 is checked again
once the day is over.

Since 2026-10-10 the long-running services use `restart: unless-stopped`, so they come back
whenever Docker Desktop starts.

Collection policy: collection is opportunistic (it runs while the host is on); 2026-10-11 was
planned as a complete day but the host went to sleep (table above). The power settings were
left unchanged until 2026-10-15, then changed (decision above). The reference dataset for
analysis remains the July 2026 grid.

A missed run is caught up: `daily_load` for 2026-10-11 started at 01:34 UTC during a brief
wake, finished at 09:32 UTC once the host was awake, and loaded 200 508 rows = 132 landed files
× 1519 stations. Three of those files were later found mislabelled and quarantined (Known data
quirks); after the reload the day holds 129 files, 195 951 rows.

dbt source freshness warns when the laptop has been asleep: collection is opportunistic by
design, see Known data gaps.

## Known data quirks

Facts about the source feeds, measured in `raw`; the dbt layer handles them explicitly.

| Quirk | Measured | Handling |
|---|---|---|
| Degraded `station_status` snapshots: `stationCode` null and `num_bikes_available_types` empty (`[{},{}]`) for every station | 4 snapshots on 2026-10-09 (08:34, 12:35, 13:35, 14:35), 6076 rows; none on 2026-10-10 | staging keeps them, with the mechanical / e-bike split as null (unknown, not zero); `station_code` rebuilt downstream from `station_information` (join on `station_id`) |
| Stations with `capacity = 0` in `station_information` | 2 of 1519 on 2026-10-10 (`21502` Verdun - Place Henri IV, `23404` Bouvier - Carnot); both report `is_installed = 0`, 0 bikes and 0 docks in every snapshot of 2026-10-09/10; `21502` was active in the July grid | the dbt test `bikes + docks <= capacity` runs with `severity: warn` and excludes `capacity = 0` |
| Late runs mislabelled: a run delayed by host sleep fetched the feed at wake-up but kept its scheduled name | 3 `station_status` files of 2026-10-11: `1020`, `1605`, `1930` held the feed of 16:07, 19:04 and 2026-10-12 09:30 (`lastUpdatedOther`), 3 to 14 h after their name | moved to `data/landing/_quarantine/` and the day reloaded (129 files, 195 951 rows). Guard: `gbfs_snapshot` skips a capture more than 240 s after its run time (unit tests in `tests/test_snapshot_guards.py`). Detection: the dbt test `assert_snapshot_matches_feed_time` fails when `snapshot_at` and `feed_updated_at` differ by more than 300 s (correct captures: 125 s at most); run on the 3 quarantined files, the same rule flags all of them (20 861 s, 10 765 s, 50 455 s). In production, the sleep of 2026-10-12/13 delayed two runs, both skipped, logged as `Captured 6:40:21.823798 after the run time 2026-10-12 13:10 UTC (max 0:04:00): skipped` and `Captured 13:38:11.154947 after the run time 2026-10-12 19:50 UTC (max 0:04:00): skipped`; the first run after wake-up (2026-10-13 09:25, captured at 09:28:17, feed time 09:27:17) was within 240 s and written |
| `station_code` null for every station in 4 bins of the July grid | 2026-07-02 12:55 and 13:55, 2026-07-04 07:00 and 09:00 UTC: 6064 rows (4 × 1516) | kept as delivered in `stg_velib_grid_2026_07`; the station code is taken downstream from the station's other rows |

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

## Backlog

- **Move the collector to the OVH VPS (container + cron) to get continuous collection independent
  of the laptop.** Why: every gap since 2026-10-10 comes from the host (a Windows Update restart,
  then sleep; since the mains-power setting of 2026-10-15, only sleep on battery), never from the
  collector. Size, measured on 2026-10-18 over the landed files: `station_status` 453 KiB per
  snapshot × 288 per day ≈ 127 MiB/day of raw JSON (≈ 3.7 GiB per 30 days),
  `station_information` ≈ 0.3 MiB/day. Not implemented.
