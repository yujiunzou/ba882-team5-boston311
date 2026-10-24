-- One row per neighborhood_group: the shared level both systems can be compared at
-- (new-system fine areas folded into legacy-style groups via the neighborhood_map seed).
-- Descriptive only: case counts belong in fct_service_requests / summary marts.
select
    neighborhood_group,
    string_agg(distinct neighborhood, ', ' order by neighborhood) as source_neighborhood_values
from {{ ref('int_service_requests_unified') }}
group by neighborhood_group
