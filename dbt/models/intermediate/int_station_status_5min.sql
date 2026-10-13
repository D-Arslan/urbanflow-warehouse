-- Grain: one row per station and 5-minute bin (station_id, bin_ts), both sources unioned.
-- bin_ts is UTC in both sources (see the model's .yml). GBFS keeps the latest snapshot of each
-- bin, the rule build_grid applied to the July grid, so a row means the same thing whatever
-- its source. Only the columns the marts need are kept.

with gbfs as (

    select
        station_id,
        bin_ts,
        'gbfs' as source,
        num_bikes_available,
        num_docks_available
    from {{ ref('stg_gbfs_station_status') }}
    qualify row_number() over (partition by station_id, bin_ts order by snapshot_at desc) = 1

),

grid as (

    select
        station_id,
        bin_ts,
        'grid' as source,
        num_bikes_available,
        num_docks_available
    from {{ ref('stg_velib_grid_2026_07') }}

)

select * from gbfs
union all
select * from grid
