select feed_id, stop_id, stop_code, stop_name
from {{ read_archive('gtfs/stops') }}
