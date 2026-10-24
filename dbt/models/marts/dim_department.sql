-- One row per readable department name (legacy codes mapped via the department_map seed).
select
    department_name,
    string_agg(distinct department, ', ' order by department) as source_department_values,
    countif(source_system = 'legacy')                          as legacy_case_count,
    countif(source_system = 'new')                             as new_case_count,
    count(*)                                                   as case_count
from {{ ref('int_service_requests_unified') }}
group by department_name
