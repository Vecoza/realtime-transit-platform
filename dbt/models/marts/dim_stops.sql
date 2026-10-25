{#- Stop codes and names from the newest timetable version that has the stop. -#}
select
    stop_id,
    stop_code,
    stop_name,
    feed_id
from {{ ref('stg_gtfs_stops') }}
qualify row_number() over (partition by stop_id order by feed_id desc) = 1
