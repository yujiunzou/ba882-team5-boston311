WITH unified AS (
    SELECT *
    FROM {{ ref('int_service_requests_unified') }}
),

metrics AS (
    SELECT
        *,
        CONCAT(source_system, ':', case_id) AS service_request_key,

        case_status = 'CLOSED' AS is_closed,

        CASE
            WHEN case_status = 'CLOSED'
                 AND closed_at IS NOT NULL
                 AND opened_at IS NOT NULL
            THEN TIMESTAMP_DIFF(closed_at, opened_at, MINUTE) / 60.0
            ELSE NULL
        END AS resolution_hours,

        CASE
            WHEN case_status = 'CLOSED'
                 AND closed_at IS NOT NULL
                 AND sla_due_at IS NOT NULL
            THEN closed_at <= sla_due_at
            ELSE NULL
        END AS met_sla,

        CASE
            WHEN sla_due_at IS NULL THEN 'NO_SLA'
            WHEN case_status = 'CLOSED' AND closed_at IS NULL
                THEN 'MISSING_CLOSE_DATE'
            WHEN case_status = 'CLOSED' AND closed_at <= sla_due_at
                THEN 'MET'
            WHEN case_status = 'CLOSED' AND closed_at > sla_due_at
                THEN 'BREACHED'
            WHEN CURRENT_TIMESTAMP() > sla_due_at
                THEN 'OPEN_OVERDUE'
            ELSE 'OPEN_ON_TIME'
        END AS sla_status

    FROM unified
)

SELECT * FROM metrics
