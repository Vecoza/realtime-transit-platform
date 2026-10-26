{#- Matched arrivals and departures, each marked when it is a suspect sign-on: a vehicle still
    reporting a trip whose planned end passed more than the configured margin earlier. -#}
with trip_ends as (
    select feed_id, trip_id, max(arrival_offset_seconds) as last_arrival_offset_seconds
    from {{ ref('stg_gtfs_stop_times') }}
    group by feed_id, trip_id
)

select
    e.*,
    coalesce(
        {{ is_suspect_sign_on('e.tst', 'e.oday', 't.last_arrival_offset_seconds') }}, false
    ) as is_suspect_sign_on
from {{ ref('stg_stop_events') }} as e
left join trip_ends as t
  on t.feed_id = e.feed_id and t.trip_id = e.trip_id
where e.match_status = 'matched'
