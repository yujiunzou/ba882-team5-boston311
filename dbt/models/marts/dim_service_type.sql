-- One row per (source_system, service_type). The two systems use different type names,
-- so both are part of the key. A shared service_category will come from the service-type seed later.
select
    concat(source_system, ':', coalesce(service_type, '(unknown)')) as service_type_key,
    source_system,
    coalesce(service_type, '(unknown)')                             as service_type,
    count(*)                                                        as case_count,
    min(opened_date)                                                as first_opened_date,
    max(opened_date)                                                as last_opened_date
from {{ ref('int_service_requests_unified') }}
group by 1, 2, 3
