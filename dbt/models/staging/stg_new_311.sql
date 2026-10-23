with raw_data as (
    select *
    from {{ source('boston311_raw', 'raw_new_311') }}
),

ranked as (
    select
        raw_data.*,
        row_number() over (
            partition by nullif(trim(case_id), '')
            order by
                _loaded_at desc nulls last,
                _source desc,
                to_json_string(raw_data) desc
        ) as _row_num
    from raw_data
    where nullif(trim(case_id), '') is not null
),

latest as (
    select * except (_row_num)
    from ranked
    where _row_num = 1
)

select
    nullif(trim(case_id), '') as case_id,

    -- The city's '+00' suffix is misleading according to PR #5.
    -- Remove it before casting to Boston-local DATETIME (no timezone shift).
    safe_cast(regexp_replace(nullif(trim(open_date), ''), r'(Z|\+00(?::?00)?)$', '') as datetime) as open_date,
    nullif(trim(case_topic), '') as case_topic,
    nullif(trim(service_name), '') as service_name,
    nullif(trim(assigned_department), '') as assigned_department,
    nullif(trim(assigned_team), '') as assigned_team,
    lower(nullif(trim(case_status), '')) as case_status,
    nullif(trim(closure_reason), '') as closure_reason,
    nullif(trim(closure_comments), '') as closure_comments,
    safe_cast(regexp_replace(nullif(trim(close_date), ''), r'(Z|\+00(?::?00)?)$', '') as datetime) as close_date,
    safe_cast(regexp_replace(nullif(trim(target_close_date), ''), r'(Z|\+00(?::?00)?)$', '') as datetime) as target_close_date,
    upper(nullif(trim(on_time), '')) as on_time,
    nullif(trim(report_source), '') as report_source,
    nullif(trim(full_address), '') as full_address,
    nullif(trim(street_number), '') as street_number,
    nullif(trim(street_name), '') as street_name,
    nullif(trim(zip_code), '') as zip_code,
    nullif(trim(neighborhood), '') as neighborhood,
    nullif(trim(public_works_district), '') as public_works_district,
    nullif(trim(city_council_district), '') as city_council_district,
    nullif(trim(fire_district), '') as fire_district,
    nullif(trim(police_district), '') as police_district,
    nullif(trim(ward), '') as ward,
    nullif(trim(precinct), '') as precinct,
    nullif(trim(submitted_photo), '') as submitted_photo,
    nullif(trim(closed_photo), '') as closed_photo,
    safe_cast(nullif(trim(longitude), '') as float64) as longitude,
    safe_cast(nullif(trim(latitude), '') as float64) as latitude,

    _loaded_at,
    _source
from latest
