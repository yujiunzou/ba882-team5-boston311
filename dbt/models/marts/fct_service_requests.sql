-- One row per service request across both systems.
-- Joins to dims: service_type_key -> dim_service_type, department_name -> dim_department,
-- neighborhood_group -> dim_neighborhood.
select
    concat(i.source_system, ':', coalesce(i.service_type, '(unknown)')) as service_type_key,
    i.*
from {{ ref('int_service_requests_unified') }} as i
