-- in_latest_reference must agree with the reference columns, one row per inconsistent station:
-- - flag true: the attributes come from the latest copy and are all present;
-- - flag false: the station is absent from the latest copy, so its reference_date is null
--   (never referenced, the 3 July-grid stations) or older than the latest copy (a station
--   removed later keeps its last known attributes).

with latest as (

    select max(reference_date) as reference_date
    from {{ ref('dim_station') }}

)

select dim_station.*
from {{ ref('dim_station') }}
cross join latest
where (
        in_latest_reference
        and (dim_station.reference_date is distinct from latest.reference_date
             or station_name is null or latitude is null or longitude is null or capacity is null)
    )
    or (
        not in_latest_reference
        and dim_station.reference_date is not distinct from latest.reference_date
    )
