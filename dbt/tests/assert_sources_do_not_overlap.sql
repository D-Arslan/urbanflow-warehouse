-- Documents a current assumption: the two sources cover disjoint periods (July 2026 grid,
-- GBFS snapshots since 2026-10-09), so coverage_date alone is unique in fct_daily_coverage.
-- One row per day covered by both sources; if this fails, the union in int_station_status_5min
-- and every per-day figure need a decision on which source wins.

select coverage_date, count(*) as sources, list(source order by source) as source_list
from {{ ref('fct_daily_coverage') }}
group by coverage_date
having count(*) > 1
