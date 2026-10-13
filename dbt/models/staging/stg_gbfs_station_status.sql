-- Grain: one row per station and snapshot (station_id, snapshot_at).
-- Renames GBFS fields to snake_case, casts 0/1 flags to boolean and epoch seconds to naive
-- UTC timestamps. station_code is kept as delivered: the feed sent it null in a few snapshots
-- (README, Known data quirks); it is rebuilt from station_information downstream, since a
-- staging model reads a single source.

with source as (

    select * from {{ source('raw', 'gbfs_station_status') }}

),

renamed as (

    select
        station_id,
        stationCode                                    as station_code,
        _snapshot_at                                   as snapshot_at,
        time_bucket(interval 5 minute, _snapshot_at)   as bin_ts,  -- start of the 5-minute bin
        dt                                             as snapshot_date,

        num_bikes_available,
        -- null when the feed sent an empty breakdown ([{},{}]): unknown, not zero
        cast(list_sum(cast(json_extract(num_bikes_available_types, '$[*].mechanical') as integer[])) as integer)
                                                       as num_mechanical_bikes_available,
        cast(list_sum(cast(json_extract(num_bikes_available_types, '$[*].ebike') as integer[])) as integer)
                                                       as num_ebikes_available,
        num_docks_available,

        cast(is_installed as boolean)                  as is_installed,
        cast(is_renting as boolean)                    as is_renting,
        cast(is_returning as boolean)                  as is_returning,
        make_timestamp(last_reported * 1000000)        as last_reported_at,  -- naive UTC
        make_timestamp(lastUpdatedOther * 1000000)     as feed_updated_at,   -- naive UTC, feed-level

        _source_file,
        _loaded_at

    from source

)

select * from renamed
