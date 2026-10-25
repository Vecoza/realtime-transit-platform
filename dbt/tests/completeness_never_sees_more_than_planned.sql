select *
from {{ ref('mart_data_completeness') }}
where seen_trips > expected_trips
