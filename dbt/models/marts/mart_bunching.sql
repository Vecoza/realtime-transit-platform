{#- Headway between consecutive departures of a route and direction at a stop, against the
    headway the timetable planned between the same two trips. A pair counts when both trips
    were observed at that stop. -#}
with scheduled as (
    select
        *,
        lag(trip_id) over stop_order as previous_trip_id,
        lag(stop_sequence) over stop_order as previous_stop_sequence,
        lag(planned_departure) over stop_order as previous_planned_departure
    from {{ ref('int_scheduled_departures') }}
    window stop_order as (
        partition by service_date, route_id, direction_id, stop_id
        order by planned_departure, trip_id
    )
),

pairs as (
    select
        s.service_date,
        s.feed_id,
        s.route_id,
        s.direction_id,
        s.stop_id,
        s.planned_departure,
        epoch(s.planned_departure - s.previous_planned_departure) as scheduled_headway_s,
        epoch(current_trip.departed_at - previous_trip.departed_at) as observed_headway_s
    from scheduled as s
    join {{ ref('int_departures') }} as current_trip
      on current_trip.service_date = s.service_date
     and current_trip.feed_id = s.feed_id
     and current_trip.trip_id = s.trip_id
     and current_trip.stop_sequence = s.stop_sequence
    join {{ ref('int_departures') }} as previous_trip
      on previous_trip.service_date = s.service_date
     and previous_trip.feed_id = s.feed_id
     and previous_trip.trip_id = s.previous_trip_id
     and previous_trip.stop_sequence = s.previous_stop_sequence
    where s.planned_departure > s.previous_planned_departure
)

select
    p.service_date,
    {{ local_hour('p.planned_departure') }} as hour_local,
    p.route_id,
    any_value(r.route_short_name) as route_short_name,
    any_value({{ transport_mode('r.route_type') }}) as mode,
    p.direction_id,
    p.stop_id,
    any_value(st.stop_code) as stop_code,
    any_value(st.stop_name) as stop_name,
    count(*) as observed_pairs,
    count(*) filter (where observed_headway_s < {{ var('bunched_below_ratio') }} * scheduled_headway_s)
        as bunched_pairs,
    count(*) filter (where observed_headway_s > {{ var('gap_above_ratio') }} * scheduled_headway_s)
        as gap_pairs,
    round(avg(scheduled_headway_s), 1) as avg_scheduled_headway_s,
    round(avg(observed_headway_s), 1) as avg_observed_headway_s,
    round(median(observed_headway_s / scheduled_headway_s), 3) as median_headway_ratio
from pairs as p
left join {{ ref('stg_gtfs_routes') }} as r
  on r.feed_id = p.feed_id and r.route_id = p.route_id
left join {{ ref('stg_gtfs_stops') }} as st
  on st.feed_id = p.feed_id and st.stop_id = p.stop_id
group by p.service_date, hour_local, p.route_id, p.direction_id, p.stop_id
