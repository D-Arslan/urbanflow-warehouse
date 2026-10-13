-- A snapshot is named after its scheduled run (snapshot_at) but holds the feed as fetched.
-- If the fetch happened later (host asleep through the run), the content belongs to another
-- slot: on 2026-10-11, 1930.json held the feed of 2026-10-12 09:30. The DAG now skips such
-- captures (gbfs_snapshot.skip_if_late, 240 s); this test catches any that would still land.
-- Correct captures stay within about two minutes of the feed time (max 125 s measured on
-- 2026-10-09/11); 300 s is above that noise and below one 5-minute slot.
-- One row per offending snapshot.

select
    snapshot_at,
    any_value(feed_updated_at)                                     as feed_updated_at,
    max(abs(date_diff('second', snapshot_at, feed_updated_at)))    as lag_seconds,
    any_value(_source_file)                                        as source_file
from {{ ref('stg_gbfs_station_status') }}
group by snapshot_at
having max(abs(date_diff('second', snapshot_at, feed_updated_at))) > 300
