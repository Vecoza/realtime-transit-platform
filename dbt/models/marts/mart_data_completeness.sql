{#- Trips the timetable planned against trips seen in the stream, per route and service day.
    A trip is expected when at least one of its planned departures lies inside the period the
    stream covered that day, and seen when it reported an arrival or departure. Metro and
    ferries report positions only, so their routes are left out. -#}
with coverage as (
    select oday as service_date, min(tst) as first_event, max(tst) as last_event
    from {{ ref('stg_stop_events') }}
    where match_status = 'matched'
    group by oday
),

expected as (
    select distinct s.service_date, s.feed_id, s.trip_id, s.route_id, r.route_short_name,
           {{ transport_mode('r.route_type') }} as mode
    from {{ ref('int_scheduled_departures') }} as s
    join coverage as c
      on c.service_date = s.service_date
     and s.planned_departure between c.first_event and c.last_event
    join {{ ref('stg_gtfs_routes') }} as r
      on r.feed_id = s.feed_id and r.route_id = s.route_id
    where r.route_type not in ({{ var('modes_without_stop_events') | join(', ') }})
),

seen as (
    select distinct oday as service_date, feed_id, trip_id
    from {{ ref('stg_stop_events') }}
    where match_status = 'matched'
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
