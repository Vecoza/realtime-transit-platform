{#- Departure delay per stop and local hour, at every stop where vehicles departed. -#}
{% set early = var('on_time_early_s') %}
{% set late = var('on_time_late_s') %}

select
    d.service_date,
    {{ local_hour('d.planned_time') }} as hour_local,
    d.stop_id,
    any_value(s.stop_code) as stop_code,
    any_value(s.stop_name) as stop_name,
    count(*) as departures,
    count(distinct d.route_id) as routes,
    round(
        count(*) filter (where measured_delay_s >= -{{ early }} and measured_delay_s < {{ late }})
            / count(*),
        4
    ) as on_time_share,
    round(avg(d.measured_delay_s), 1) as avg_delay_s,
    round(quantile_cont(d.measured_delay_s, 0.9), 1) as p90_delay_s,
    bool_or(d.is_timing_point) as is_timing_point
from {{ ref('int_departures') }} as d
left join {{ ref('stg_gtfs_stops') }} as s
  on s.feed_id = d.feed_id and s.stop_id = d.stop_id
group by d.service_date, hour_local, d.stop_id
