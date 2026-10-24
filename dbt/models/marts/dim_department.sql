-- One row per readable department name (legacy codes mapped via the department_map seed).
-- Descriptive only: case counts belong in fct_service_requests / summary marts.
select
    department_name,
    string_agg(distinct department, ', ' order by department) as source_department_values
from {{ ref('int_service_requests_unified') }}
group by department_name
