WITH legacy AS (
    SELECT
        'Legacy' AS source_system,
        case_enquiry_id AS case_id,
        open_dt AS opened_at,
        closed_dt AS closed_at,
        sla_target_dt AS sla_due_at,

        CASE
            WHEN UPPER(TRIM(case_status)) = 'CLOSED' THEN 'CLOSED'
            WHEN UPPER(TRIM(case_status)) = 'OPEN' THEN 'OPEN'
            ELSE 'UNKNOWN'
        END AS case_status,

        UPPER(TRIM(on_time)) AS on_time,
        subject AS category_level_1,
        reason AS category_level_2,
        `type` AS service_type,
        department,
        neighborhood,
        latitude,
        longitude,
        closure_reason

    FROM {{ ref('stg_legacy_311') }}
),

new_cases AS (
    SELECT
        'New' AS source_system,
        case_id,
        open_date AS opened_at,
        close_date AS closed_at,
        target_close_date AS sla_due_at,

        CASE
            WHEN UPPER(TRIM(case_status)) = 'CLOSED' THEN 'CLOSED'
            WHEN UPPER(TRIM(case_status)) = 'IN PROGRESS' THEN 'IN_PROGRESS'
            WHEN UPPER(TRIM(case_status)) = 'CREATE' THEN 'CREATED'
            WHEN UPPER(TRIM(case_status)) = 'NEEDS REALLOCATION' THEN 'NEEDS_REALLOCATION'
            WHEN UPPER(TRIM(case_status)) = 'SUBMIT' THEN 'SUBMITTED'
            ELSE 'UNKNOWN'
        END AS case_status,

        UPPER(TRIM(on_time)) AS on_time,
        case_topic AS category_level_1,
        CAST(NULL AS STRING) AS category_level_2,
        service_name AS service_type,
        assigned_department AS department,
        neighborhood,
        latitude,
        longitude,
        closure_reason

    FROM {{ ref('stg_new_311') }}
)

SELECT * FROM legacy
UNION ALL
SELECT * FROM new_cases
