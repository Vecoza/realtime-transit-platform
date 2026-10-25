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
    service_date,
    {{ local_hour('planned_departure') }} as hour_local,
    route_id,
    direction_id,
    stop_id,
    count(*) as observed_pairs,
    count(*) filter (where observed_headway_s < {{ var('bunched_below_ratio') }} * scheduled_headway_s)
        as bunched_pairs,
    count(*) filter (where observed_headway_s > {{ var('gap_above_ratio') }} * scheduled_headway_s)
        as gap_pairs,
    round(avg(scheduled_headway_s), 1) as avg_scheduled_headway_s,
    round(avg(observed_headway_s), 1) as avg_observed_headway_s,
    round(median(observed_headway_s / scheduled_headway_s), 3) as median_headway_ratio
from pairs
group by service_date, hour_local, route_id, direction_id, stop_id
