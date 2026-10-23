{{ config(severity='warn') }}
-- Warns about closed cases whose close time is before their open time.
select service_request_key, opened_at, closed_at
from {{ ref('int_service_requests_unified') }}
where closed_at < opened_at
