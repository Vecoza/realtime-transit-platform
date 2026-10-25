with mart as (
    select service_date, sum(departures) as departures
    from {{ ref('mart_stop_punctuality') }}
    group by service_date
),

source as (
    select service_date, count(*) as departures
    from {{ ref('int_departures') }}
    group by service_date
)

select *
from source
full join mart using (service_date)
where mart.departures is distinct from source.departures
