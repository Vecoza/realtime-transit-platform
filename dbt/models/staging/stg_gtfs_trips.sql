{#- Timetable versions are read from their Parquet export, which is written when they load. -#}
select feed_id, trip_id, route_id, service_id, direction_id, start_offset_seconds
from {{ read_archive('gtfs/trips') }}
