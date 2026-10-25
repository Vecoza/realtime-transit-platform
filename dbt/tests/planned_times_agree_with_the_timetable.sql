{#- The planned time the sink stored equals the departure in the exported timetable. -#}
select d.*
from {{ ref('int_departures') }} as d
join {{ ref('int_scheduled_departures') }} as s
  using (service_date, feed_id, trip_id, stop_sequence)
where d.planned_time <> s.planned_departure
