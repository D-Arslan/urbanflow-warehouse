-- Grain: one row per station and reference day (station_id, info_date).
-- The reference is collected once a day (dt); keeping every day shows renamed or resized
-- stations. Renames GBFS fields, converts the feed epoch to a naive UTC timestamp.

with source as (

    select * from {{ source('raw', 'gbfs_station_information') }}

),

renamed as (

    select
        station_id,
        stationCode                                  as station_code,
        name                                         as station_name,
        dt                                           as info_date,
        lat                                          as latitude,
        lon                                          as longitude,
        capacity,
        rental_methods,
        station_opening_hours,
        make_timestamp(lastUpdatedOther * 1000000)   as feed_updated_at,  -- naive UTC

        _source_file,
        _loaded_at

    from source

)

select * from renamed
