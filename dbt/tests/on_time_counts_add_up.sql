select *
from {{ ref('mart_on_time_performance') }}
where on_time + early + late <> departures
