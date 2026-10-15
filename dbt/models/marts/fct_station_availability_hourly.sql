-- Grain: one row per station and UTC hour with at least one 5-minute bin (station_id, hour_ts).
-- An hour without any bin has no row: the fact records observations; coverage lives in
-- fct_daily_coverage. Closed bins (0 bikes and 0 docks) are counted apart and excluded from
-- the averages and shares, since such a bin would be both empty and full. Integer counters sit
-- next to the shares: a share does not re-aggregate, the counters do, at any grain.
-- hour_ts stays UTC; hour_of_day_local is the hour in Europe/Paris, where Vélib' usage is read.

with bins as (

    select
        station_id,
        date_trunc('hour', bin_ts)                                  as hour_ts,
        source,
        num_bikes_available,
        num_docks_available,
        num_bikes_available = 0 and num_docks_available = 0         as is_closed
    from {{ ref('int_station_status_5min') }}

),

hourly as (

    select
        station_id,
        hour_ts,
        source,  -- grouped on, so an hour mixing sources would break the unique test
        count(*)                                                            as bins_present,
        count(*) filter (where is_closed)                                   as bins_closed,
        count(*) filter (where not is_closed and num_bikes_available = 0)   as bins_empty,
        count(*) filter (where not is_closed and num_docks_available = 0)   as bins_full,
        avg(num_bikes_available) filter (where not is_closed)               as avg_bikes_available,
        avg(num_docks_available) filter (where not is_closed)               as avg_docks_available,
        min(num_bikes_available) filter (where not is_closed)               as min_bikes_available
    from bins
    group by station_id, hour_ts, source

)

select
    station_id,
    hour_ts,
    cast(hour_ts as date)                                                   as hour_date,
    cast(hour(hour_ts) as integer)                                          as hour_of_day,
    cast(isodow(hour_ts) as integer)                                        as day_of_week,
    -- naive UTC -> instant (UTC) -> wall clock in Paris: follows summer/winter time
    cast(hour(timezone('Europe/Paris', timezone('UTC', hour_ts))) as integer) as hour_of_day_local,
    source,
    cast(bins_present as integer)                                           as bins_present,
    cast(bins_closed as integer)                                            as bins_closed,
    cast(bins_empty as integer)                                             as bins_empty,
    cast(bins_full as integer)                                              as bins_full,
    -- null when every bin of the hour is closed: nothing usable was measured
    bins_empty / nullif(bins_present - bins_closed, 0)                      as pct_empty,
    bins_full / nullif(bins_present - bins_closed, 0)                       as pct_full,
    avg_bikes_available,
    avg_docks_available,
    min_bikes_available
from hourly
