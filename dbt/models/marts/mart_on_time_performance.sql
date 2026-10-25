{#- Departure punctuality per route and local hour, at timing points only: elsewhere the
    published times are approximate. -#}
{% set early = var('on_time_early_s') %}
{% set late = var('on_time_late_s') %}

with departures as (
    select
        service_date,
        {{ local_hour('planned_time') }} as hour_local,
        feed_id,
        route_id,
        measured_delay_s
    from {{ ref('int_departures') }}
    where is_timing_point
)

select
    d.service_date,
    d.hour_local,
    d.route_id,
    any_value(r.route_short_name) as route_short_name,
    any_value({{ transport_mode('r.route_type') }}) as mode,
    count(*) as departures,
    count(*) filter (where measured_delay_s >= -{{ early }} and measured_delay_s < {{ late }})
        as on_time,
    count(*) filter (where measured_delay_s < -{{ early }}) as early,
    count(*) filter (where measured_delay_s >= {{ late }}) as late,
    round(
        count(*) filter (where measured_delay_s >= -{{ early }} and measured_delay_s < {{ late }})
            / count(*),
        4
    ) as on_time_share,
    round(avg(measured_delay_s), 1) as avg_delay_s,
    round(quantile_cont(measured_delay_s, 0.9), 1) as p90_delay_s
from departures as d
left join {{ ref('stg_gtfs_routes') }} as r
  on r.feed_id = d.feed_id and r.route_id = d.route_id
group by d.service_date, d.hour_local, d.route_id
