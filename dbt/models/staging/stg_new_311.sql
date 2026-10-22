WITH source AS (
    SELECT *
    FROM {{ source('boston311_raw', 'raw_new_311') }}
)

SELECT
    NULLIF(TRIM(case_id), '') AS case_id,

    SAFE_CAST(NULLIF(TRIM(open_date), '') AS TIMESTAMP) AS open_date,
    NULLIF(TRIM(case_topic), '') AS case_topic,
    NULLIF(TRIM(service_name), '') AS service_name,
    NULLIF(TRIM(assigned_department), '') AS assigned_department,
    NULLIF(TRIM(assigned_team), '') AS assigned_team,
    NULLIF(TRIM(case_status), '') AS case_status,
    NULLIF(TRIM(closure_reason), '') AS closure_reason,
    NULLIF(TRIM(closure_comments), '') AS closure_comments,

    SAFE_CAST(NULLIF(TRIM(close_date), '') AS TIMESTAMP) AS close_date,
    SAFE_CAST(NULLIF(TRIM(target_close_date), '') AS TIMESTAMP) AS target_close_date,

    NULLIF(TRIM(on_time), '') AS on_time,
    NULLIF(TRIM(report_source), '') AS report_source,
    NULLIF(TRIM(full_address), '') AS full_address,
    NULLIF(TRIM(street_number), '') AS street_number,
    NULLIF(TRIM(street_name), '') AS street_name,
    NULLIF(TRIM(zip_code), '') AS zip_code,
    NULLIF(TRIM(neighborhood), '') AS neighborhood,
    NULLIF(TRIM(public_works_district), '') AS public_works_district,
    NULLIF(TRIM(city_council_district), '') AS city_council_district,
    NULLIF(TRIM(fire_district), '') AS fire_district,
    NULLIF(TRIM(police_district), '') AS police_district,
    NULLIF(TRIM(ward), '') AS ward,
    NULLIF(TRIM(precinct), '') AS precinct,
    NULLIF(TRIM(submitted_photo), '') AS submitted_photo,
    NULLIF(TRIM(closed_photo), '') AS closed_photo,

    SAFE_CAST(NULLIF(TRIM(longitude), '') AS FLOAT64) AS longitude,
    SAFE_CAST(NULLIF(TRIM(latitude), '') AS FLOAT64) AS latitude

FROM source
