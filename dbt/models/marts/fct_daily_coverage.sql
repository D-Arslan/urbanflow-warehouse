-- Grain: one row per UTC day and source (coverage_date, source). Days are generated between
-- the source's first and last observed day, so a day with no bin at all has a row too. The
-- upper bound is the last day observed, not today: a stop of several days shows up only once
-- a new snapshot is loaded (the source freshness check flags a stopped collection).
-- Station completeness is measured against the bins actually collected that day, not 288:
-- against 288 it would be 0 on every incomplete day; against the collected bins it shows
-- stations dropping out while the collection was running.
-- is_current_day is evaluated at build time.

with bins as (

    select source, cast(bin_ts as date) as coverage_date, bin_ts, station_id
    from {{ ref('int_station_status_5min') }}

),

per_day as (

    select
        source,
        coverage_date,
        count(distinct bin_ts)      as bins_present,
        count(distinct station_id)  as stations_observed
    from bins
    group by source, coverage_date

),

per_station as (

    select source, coverage_date, station_id, count(*) as station_bins
    from bins
    group by source, coverage_date, station_id

),

complete_stations as (

    select
        per_station.source,
        per_station.coverage_date,
        count(*) filter (where per_station.station_bins = per_day.bins_present) as stations_complete
    from per_station
    inner join per_day using (source, coverage_date)
    group by per_station.source, per_station.coverage_date

),

days as (

    select
        source,
        cast(unnest(generate_series(min(coverage_date), max(coverage_date), interval 1 day)) as date) as coverage_date
    from per_day
    group by source

)

select
    days.coverage_date,
    days.source,
    cast(coalesce(per_day.bins_present, 0) as integer)                      as bins_present,
    288                                                                     as bins_expected,
    cast(288 - coalesce(per_day.bins_present, 0) as integer)                as bins_missing,
    cast(coalesce(per_day.stations_observed, 0) as integer)                 as stations_observed,
    cast(coalesce(complete_stations.stations_complete, 0) as integer)       as stations_complete,
    complete_stations.stations_complete / nullif(per_day.stations_observed, 0) as pct_stations_complete,
    coalesce(per_day.bins_present, 0) = 288                                 as is_complete_day,
    days.coverage_date = cast(now() at time zone 'UTC' as date)            as is_current_day
from days
left join per_day using (source, coverage_date)
left join complete_stations using (source, coverage_date)
