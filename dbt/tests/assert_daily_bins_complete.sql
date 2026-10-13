{{ config(severity='warn') }}

-- One row per source and UTC day with fewer than 288 five-minute bins (a full calendar day).
-- Days are generated between each source's first and last day, so a day with no bin at all is
-- reported too. The upper bound is the last day observed, not today: if collection stops for
-- three days, those days are reported only once a new snapshot is loaded. Accepted: the
-- source freshness check (warn 2 h, error 26 h) is what flags a collection that has stopped.
-- The current UTC day is excluded: it is not over, so it would warn on every run and the noise
-- would hide the days that are really incomplete.
-- Causes of the gaps: README, Known data gaps.

with bins as (

    select source, cast(bin_ts as date) as bin_date, count(distinct bin_ts) as bins_present
    from {{ ref('int_station_status_5min') }}
    group by source, cast(bin_ts as date)

),

days as (

    select source, cast(unnest(generate_series(min(bin_date), max(bin_date), interval 1 day)) as date) as bin_date
    from bins
    group by source

)

select
    days.source,
    days.bin_date,
    coalesce(bins.bins_present, 0)          as bins_present,
    288                                     as bins_expected,
    288 - coalesce(bins.bins_present, 0)    as bins_missing
from days
left join bins using (source, bin_date)
where days.bin_date < cast(now() at time zone 'UTC' as date)
  and coalesce(bins.bins_present, 0) < 288
