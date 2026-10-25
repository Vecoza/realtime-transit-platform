{#- One observed departure per trip and stop on its service day, with the platform's delay.
    Coupled units of a train or tram each report the same trip; the first departure counts. -#}
select
    e.oday as service_date,
    e.feed_id,
    e.trip_id,
    e.route_id,
    e.direction_id,
    e.stop_id,
    e.stop_sequence,
    st.is_timing_point,
    e.planned_time,
    e.tst as departed_at,
    e.measured_delay_s
from {{ ref('stg_stop_events') }} as e
join {{ ref('stg_gtfs_stop_times') }} as st
  on st.feed_id = e.feed_id and st.trip_id = e.trip_id and st.stop_sequence = e.stop_sequence
where e.event_type = 'dep' and e.match_status = 'matched'
qualify row_number() over (
    partition by e.oday, e.feed_id, e.trip_id, e.stop_sequence
    order by e.tst, e.operator_id, e.vehicle_number
) = 1
