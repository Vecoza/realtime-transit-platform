{#- Reconciliation: every archived day still has exactly the rows that were verified. -#}
{% if has_archive_files('stop_events') %}
with logged as (
    select cast(object_key as date) as utc_day, row_count
    from pg.meta.archive_runs
    where source_table = 'rt.stop_events' and step = 'dropped'
),

archived as (
    select utc_day, count(*) as row_count
    from {{ read_archive('stop_events') }}
    group by utc_day
)

select *
from logged
left join archived using (utc_day)
where coalesce(archived.row_count, 0) <> logged.row_count
{% else %}
select 1 where false
{% endif %}
