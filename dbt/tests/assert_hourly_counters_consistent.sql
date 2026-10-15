-- The hourly counters must add up, one row per inconsistent station-hour:
-- closed bins are part of the bins present; an open bin is empty, full or neither, never both
-- (0 bikes and 0 docks is closed); shares stay within [0, 1].

select *
from {{ ref('fct_station_availability_hourly') }}
where bins_closed > bins_present
   or bins_empty + bins_full > bins_present - bins_closed
   or pct_empty not between 0 and 1
   or pct_full not between 0 and 1
