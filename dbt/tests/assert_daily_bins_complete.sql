{{ config(severity='warn') }}

-- One row per source and UTC day with fewer than 288 five-minute bins (a full calendar day).
-- Read from fct_daily_coverage, so the day spine (days with no bin included, upper bound = last
-- day observed), the counts and the current-day exclusion are computed in one place. The
-- current UTC day is excluded: it is not over, so it would warn on every run and the noise
-- would hide the days that are really incomplete.
-- Causes of the gaps: README, Known data gaps.

select coverage_date, source, bins_present, bins_expected, bins_missing
from {{ ref('fct_daily_coverage') }}
where not is_complete_day
  and not is_current_day
