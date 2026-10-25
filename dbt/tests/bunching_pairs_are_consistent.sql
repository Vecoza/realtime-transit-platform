select *
from {{ ref('mart_bunching') }}
where bunched_pairs + gap_pairs > observed_pairs
   or (bunched_pairs > 0 and median_headway_ratio is null)
