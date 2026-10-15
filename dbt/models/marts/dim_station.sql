-- Grain: one row per station ever observed (station_id), from every source: the reference
-- (station_information), the GBFS snapshots and the July grid (through int_station_status_5min).
-- Attributes come from the station's most recent reference copy. Stations missing from the
-- latest copy get in_latest_reference = false and no attributes (3 stations seen only in the
-- July grid); their code comes from their own snapshots.

with reference as (

    select *
    from {{ ref('stg_gbfs_station_information') }}
    qualify row_number() over (partition by station_id order by info_date desc) = 1

),

latest_reference as (

    select max(info_date) as info_date
    from {{ ref('stg_gbfs_station_information') }}

),

observed as (

    select
        station_id,
        min(bin_ts) as first_seen_at,
        max(bin_ts) as last_seen_at
    from {{ ref('int_station_status_5min') }}
    group by station_id

),

-- Fallback code, for stations absent from the reference: the most recent non-null code seen
-- in the GBFS snapshots or the July grid (degraded snapshots have it null).
observed_code as (

    select station_id, station_code
    from (
        select station_id, station_code, snapshot_at as seen_at
        from {{ ref('stg_gbfs_station_status') }}
        where station_code is not null
        union all
        select station_id, station_code, bin_ts as seen_at
        from {{ ref('stg_velib_grid_2026_07') }}
        where station_code is not null
    )
    qualify row_number() over (partition by station_id order by seen_at desc) = 1

),

stations as (

    select station_id from reference
    union
    select station_id from observed

)

select
    stations.station_id,
    coalesce(reference.station_code, observed_code.station_code)    as station_code,
    reference.station_name,
    reference.latitude,
    reference.longitude,
    reference.capacity,
    reference.info_date                                             as reference_date,
    coalesce(reference.info_date = latest_reference.info_date, false) as in_latest_reference,
    observed.first_seen_at,
    observed.last_seen_at
from stations
left join reference using (station_id)
left join observed_code using (station_id)
left join observed using (station_id)
cross join latest_reference
