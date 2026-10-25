{#- Every departure the timetable planned on the observed service days, in the feed the
    events were matched to on that day (the most frequent one if there were several). -#}
with service_feeds as (
    select service_date, feed_id
    from {{ ref('int_departures') }}
    group by service_date, feed_id
    qualify row_number() over (
        partition by service_date order by count(*) desc, feed_id desc
    ) = 1
)

select
    f.service_date,
    t.feed_id,
    t.trip_id,
    t.route_id,
    t.direction_id,
    st.stop_id,
    st.stop_sequence,
    st.is_timing_point,
    {{ service_day_start('f.service_date') }}
        + to_seconds(st.departure_offset_seconds) as planned_departure,
    {{ service_day_start('f.service_date') }}
        + to_seconds(t.start_offset_seconds) as trip_planned_start
from service_feeds as f
join {{ ref('stg_gtfs_service_dates') }} as sd
  on sd.feed_id = f.feed_id and sd.service_date = f.service_date
join {{ ref('stg_gtfs_trips') }} as t
  on t.feed_id = sd.feed_id and t.service_id = sd.service_id
join {{ ref('stg_gtfs_stop_times') }} as st
  on st.feed_id = t.feed_id and st.trip_id = t.trip_id
