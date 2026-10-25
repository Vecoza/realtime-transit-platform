select feed_id, trip_id, stop_sequence, stop_id, arrival_offset_seconds,
       departure_offset_seconds, coalesce(timepoint, 1) = 1 as is_timing_point
from {{ read_archive('gtfs/stop_times') }}
