WITH source AS (
    SELECT *
    FROM {{ source('boston311_raw', 'raw_legacy_311') }}
)

SELECT
    NULLIF(TRIM(case_enquiry_id), '') AS case_enquiry_id,

    SAFE_CAST(NULLIF(TRIM(open_dt), '') AS TIMESTAMP) AS open_dt,
    SAFE_CAST(NULLIF(TRIM(sla_target_dt), '') AS TIMESTAMP) AS sla_target_dt,
    SAFE_CAST(NULLIF(TRIM(closed_dt), '') AS TIMESTAMP) AS closed_dt,

    NULLIF(TRIM(on_time), '') AS on_time,
    NULLIF(TRIM(case_status), '') AS case_status,
    NULLIF(TRIM(closure_reason), '') AS closure_reason,
    NULLIF(TRIM(case_title), '') AS case_title,
    NULLIF(TRIM(subject), '') AS subject,
    NULLIF(TRIM(reason), '') AS reason,
    NULLIF(TRIM(type), '') AS type,
    NULLIF(TRIM(queue), '') AS queue,
    NULLIF(TRIM(department), '') AS department,

    NULLIF(TRIM(submitted_photo), '') AS submitted_photo,
    NULLIF(TRIM(closed_photo), '') AS closed_photo,
    NULLIF(TRIM(location), '') AS location,
    NULLIF(TRIM(fire_district), '') AS fire_district,
    NULLIF(TRIM(pwd_district), '') AS pwd_district,
    NULLIF(TRIM(city_council_district), '') AS city_council_district,
    NULLIF(TRIM(police_district), '') AS police_district,
    NULLIF(TRIM(neighborhood), '') AS neighborhood,
    NULLIF(TRIM(neighborhood_services_district), '') AS neighborhood_services_district,
    NULLIF(TRIM(ward), '') AS ward,
    NULLIF(TRIM(precinct), '') AS precinct,
    NULLIF(TRIM(location_street_name), '') AS location_street_name,
    NULLIF(TRIM(location_zipcode), '') AS location_zipcode,

    SAFE_CAST(NULLIF(TRIM(latitude), '') AS FLOAT64) AS latitude,
    SAFE_CAST(NULLIF(TRIM(longitude), '') AS FLOAT64) AS longitude,

    NULLIF(TRIM(geom_4326), '') AS geom_4326,
    NULLIF(TRIM(source.source), '') AS source

FROM source
