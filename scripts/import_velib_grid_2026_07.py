"""One-off import of the July 2026 UrbanFlow 5-minute grid into data/raw/.

Run from the repository root, in PowerShell (the source is mounted read-only):
    docker run --rm --entrypoint python `
      -v D:/UrbanFlow/ml/data/grid:/src:ro `
      -v ${PWD}/data:/data -v ${PWD}/scripts:/scripts:ro `
      urbanflow-warehouse/airflow:3.3.2 `
      /scripts/import_velib_grid_2026_07.py /src /data/raw/velib_grid_2026_07

Rewrites the Spark output (part files only, no _SUCCESS or .crc) as Parquet partitioned
by dt, the UTC date of ts (not Paris time), aligned with the dt= partitions of the GBFS
landing zone. Prints the provenance to copy into the README. Fails, without writing
anything, if ts disagrees with bin_epoch or (station_id, bin_epoch) is not unique.
"""
import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("src", type=Path, help="Spark output directory of the grid")
    parser.add_argument("dest", type=Path, help="target directory, rewritten as dt=YYYY-MM-DD/")
    args = parser.parse_args()

    src_files = sorted(args.src.glob("*.parquet"))  # skips _SUCCESS and hidden .crc files
    if not src_files:
        sys.exit(f"No Parquet file in {args.src}")

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE TEMP TABLE grid AS SELECT * FROM read_parquet({[str(f) for f in src_files]!r})")

    print("== Provenance ==")
    for f in src_files:
        stat = f.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(timespec="seconds")
        print(f"file    {f.name}\nbytes   {stat.st_size}\nmtime   {mtime} (UTC)\nsha256  {sha256(f)}")

    print("== Source schema ==")
    for name, col_type, *_ in con.execute("DESCRIBE grid").fetchall():
        print(f"{name:<14}{col_type}")

    rows, stations, ts_min, ts_max = con.execute(
        "SELECT count(*), count(DISTINCT station_id), min(ts), max(ts) FROM grid"
    ).fetchone()
    # make_timestamp(microseconds) gives a naive UTC TIMESTAMP, comparable to ts without a time
    # zone cast; BIGINT first so an int32 bin_epoch cannot overflow in the multiplication.
    ts_mismatch = con.execute(
        "SELECT count(*) FROM grid"
        " WHERE ts IS DISTINCT FROM make_timestamp(CAST(bin_epoch AS BIGINT) * 1000000)"
    ).fetchone()[0]
    duplicates = con.execute(
        "SELECT count(*) - count(DISTINCT (station_id, bin_epoch)) FROM grid"
    ).fetchone()[0]
    print("== Checks ==")
    print(f"rows    {rows}\nstations {stations}\nts      {ts_min} -> {ts_max} (UTC)")
    print(f"ts_mismatch (ts != bin_epoch)        {ts_mismatch}")
    print(f"duplicates (station_id, bin_epoch)   {duplicates}")

    if ts_mismatch or duplicates:
        sys.exit(f"Import aborted: {ts_mismatch} ts mismatches, {duplicates} duplicates. Nothing written.")

    # dt = UTC date of ts, as in the GBFS landing zone. OVERWRITE makes a re-run idempotent.
    args.dest.parent.mkdir(parents=True, exist_ok=True)  # COPY creates dest, not its parents
    con.execute(f"""
        COPY (SELECT *, CAST(ts AS DATE) AS dt FROM grid ORDER BY station_id, bin_epoch)
        TO '{args.dest}' (FORMAT parquet, PARTITION_BY (dt), OVERWRITE true)
    """)

    print("== Written (read back) ==")
    for dt, n in con.execute(
        f"SELECT dt, count(*) FROM read_parquet('{args.dest}/dt=*/*.parquet', hive_partitioning = true) "
        "GROUP BY dt ORDER BY dt"
    ).fetchall():
        print(f"dt={dt}  {n} rows")


if __name__ == "__main__":
    main()
