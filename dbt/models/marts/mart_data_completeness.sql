{#- Trips the timetable planned against trips seen in the stream, per route and service day.
    The stream covered a minute when the platform received stop events during it; receipt
    time is used, so a trickle of buffered messages about earlier minutes does not count as
    coverage. A trip is expected when one of its planned departures falls in a covered
    minute, and seen when it reported an arrival or departure that is not a suspect sign-on.
    Metro and ferries report positions only, so their routes are left out. -#}
with covered_minutes as (
    select distinct date_trunc('minute', received_at) as minute
    from {{ ref('stg_stop_events') }}
),

expected as (
    select distinct s.service_date, s.feed_id, s.trip_id, s.route_id, r.route_short_name,
           {{ transport_mode('r.route_type') }} as mode
    from {{ ref('int_scheduled_departures') }} as s
    join covered_minutes as c
      on c.minute = date_trunc('minute', s.planned_departure)
    join {{ ref('stg_gtfs_routes') }} as r
      on r.feed_id = s.feed_id and r.route_id = s.route_id
    where r.route_type not in ({{ var('modes_without_stop_events') | join(', ') }})
),

seen as (
    select distinct oday as service_date, feed_id, trip_id
    from {{ ref('int_stop_events') }}
    where not is_suspect_sign_on
)

select
    e.service_date,
    e.route_id,
    any_value(e.route_short_name) as route_short_name,
    any_value(e.mode) as mode,
    count(*) as expected_trips,
    count(s.trip_id) as seen_trips,
    round(count(s.trip_id) / count(*), 4) as seen_share
from expected as e
left join seen as s
  on s.service_date = e.service_date and s.feed_id = e.feed_id and s.trip_id = e.trip_id
group by e.service_date, e.route_id
