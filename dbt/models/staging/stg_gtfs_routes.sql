select feed_id, route_id, route_short_name, route_type
from {{ read_archive('gtfs/routes') }}
