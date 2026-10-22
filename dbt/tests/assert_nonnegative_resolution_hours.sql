SELECT
    service_request_key,
    resolution_hours
FROM {{ ref('fct_service_requests') }}
WHERE resolution_hours < 0
