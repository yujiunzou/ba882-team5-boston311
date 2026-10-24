-- One row per neighborhood_group: the shared level both systems can be compared at
-- (new-system fine areas folded into legacy-style groups via the neighborhood_map seed).
select
    neighborhood_group,
    string_agg(distinct neighborhood, ', ' order by neighborhood) as source_neighborhood_values,
    countif(source_system = 'legacy')                              as legacy_case_count,
    countif(source_system = 'new')                                 as new_case_count,
    count(*)                                                       as case_count
from {{ ref('int_service_requests_unified') }}
group by neighborhood_group
