"""Unit tests of the capture guards in dags/gbfs_snapshot.py.

pytest-compatible, and runnable without it (the Airflow image has no pytest):
    docker compose run --rm --no-deps -v ./tests:/opt/airflow/tests airflow-scheduler \
      bash -c "python /opt/airflow/tests/test_snapshot_guards.py"
"""
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/opt/airflow/dags")

from airflow.sdk.exceptions import AirflowSkipException  # noqa: E402
from gbfs_snapshot import skip_if_late, skip_if_other_day  # noqa: E402

STATUS_RUN = datetime(2026, 10, 11, 19, 30, tzinfo=timezone.utc)  # the 1930.json run
INFO_RUN = datetime(2026, 10, 12, 0, 5, tzinfo=timezone.utc)      # station_information of 2026-10-12


def skipped(check, when, captured_at) -> bool:
    try:
        check(when, captured_at)
    except AirflowSkipException:
        return True
    return False


def test_status_capture_1s_late_is_written():
    assert not skipped(skip_if_late, STATUS_RUN, STATUS_RUN + timedelta(seconds=1))


def test_status_capture_240s_late_is_written():
    assert not skipped(skip_if_late, STATUS_RUN, STATUS_RUN + timedelta(seconds=240))


def test_status_capture_241s_late_is_skipped():
    assert skipped(skip_if_late, STATUS_RUN, STATUS_RUN + timedelta(seconds=241))


def test_status_real_case_1930_captured_next_morning_is_skipped():
    assert skipped(skip_if_late, STATUS_RUN, datetime(2026, 10, 12, 9, 31, 11, tzinfo=timezone.utc))


def test_information_captured_later_same_day_is_written():
    # Real case: the 00:05 run of 2026-10-12 fetched at 09:31 after host sleep.
    assert not skipped(skip_if_other_day, INFO_RUN, datetime(2026, 10, 12, 9, 31, 10, tzinfo=timezone.utc))


def test_information_captured_next_day_is_skipped():
    assert skipped(skip_if_other_day, INFO_RUN, datetime(2026, 10, 13, 0, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_")]
    for name, fn in tests:
        fn()
        print(f"PASS {name}")
    print(f"{len(tests)} passed")
