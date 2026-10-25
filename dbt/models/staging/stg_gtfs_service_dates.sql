select feed_id, service_date, service_id
from {{ read_archive('gtfs/service_dates') }}
