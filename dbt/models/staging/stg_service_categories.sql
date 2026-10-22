WITH legacy_categories AS (
    SELECT DISTINCT
        'Legacy' AS system,
        subject AS category_level_1,
        reason AS category_level_2,
        `type` AS service_type
    FROM {{ ref('stg_legacy_311') }}
),

new_categories AS (
    SELECT DISTINCT
        'New' AS system,
        case_topic AS category_level_1,
        CAST(NULL AS STRING) AS category_level_2,
        service_name AS service_type
    FROM {{ ref('stg_new_311') }}
)

SELECT * FROM legacy_categories
UNION ALL
SELECT * FROM new_categories
