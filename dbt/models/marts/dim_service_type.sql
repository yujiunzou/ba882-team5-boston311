-- One row per (source_system, service_type). The two systems use different type names,
-- so both are part of the key. service_category (from the service_type_map seed) is the shared level.
-- Descriptive only: case counts belong in fct_service_requests / summary marts.
select distinct
    concat(source_system, ':', coalesce(service_type, '(unknown)')) as service_type_key,
    source_system,
    coalesce(service_type, '(unknown)')                             as service_type,
    service_category
from {{ ref('int_service_requests_unified') }}
