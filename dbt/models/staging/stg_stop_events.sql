{#- Arrivals and departures: PostgreSQL for recent days, Parquet for archived ones. -#}
{% set columns %}
    tst, operator_id, vehicle_number, event_type, oday, route_id, direction_id, feed_id,
    trip_id, stop_id, stop_sequence, match_status, planned_time, measured_delay_s, live_delay_s
{% endset %}

with archived_days as (
    {{ archived_days('stop_events') }}
),

recent as (
    select {{ columns }}
    from pg.rt.stop_events
    where cast(timezone('UTC', tst) as date) not in (select utc_day from archived_days)
)

{% if has_archive_files('stop_events') %}
, archived as (
    select {{ columns }}
    from {{ read_archive('stop_events') }}
    where utc_day in (select utc_day from archived_days)
)

select * from recent
union all
select * from archived
{% else %}
select * from recent
{% endif %}
