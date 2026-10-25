{#- Route names and modes from the newest timetable version that has the route, for joining
    the live tables in PostgreSQL, which carry only route ids. -#}
select
    route_id,
    route_short_name,
    {{ transport_mode('route_type') }} as mode,
    feed_id
from {{ ref('stg_gtfs_routes') }}
qualify row_number() over (partition by route_id order by feed_id desc) = 1
