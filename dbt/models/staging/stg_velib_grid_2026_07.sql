-- Grain: one row per station and 5-minute bin (station_id, bin_ts), UTC.
-- Derived backfill from UrbanFlow's build_grid: the latest measure of each bin. Its clock is
-- the poller's Unix epoch (time.time()), so ts is UTC, like the GBFS snapshots. ts is already
-- the start of the bin; bin_epoch is dropped as redundant (the import checked ts = bin_epoch
-- on every row). Columns take the GBFS names so both sources union directly.
-- station_code is null in 4 bins (README, Known data quirks).

with source as (

    select * from {{ source('raw', 'velib_grid_2026_07') }}

),

renamed as (

    select
        station_id,
        station_code,
        ts       as bin_ts,
        dt       as bin_date,
        bikes    as num_bikes_available,
        docks    as num_docks_available,

        _source_file,
        _loaded_at

    from source

)

select * from renamed
