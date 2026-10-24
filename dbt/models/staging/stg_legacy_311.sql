with raw_data as (
    select *
    from {{ source('boston311_raw', 'raw_legacy_311') }}
),

ranked as (
    select
        raw_data.*,
        row_number() over (
            partition by nullif(trim(case_enquiry_id), '')
            order by
                _loaded_at desc nulls last,
                _source desc,
                to_json_string(raw_data) desc
        ) as _row_num
    from raw_data
    where nullif(trim(case_enquiry_id), '') is not null
),

latest as (
    select * except (_row_num)
    from ranked
    where _row_num = 1
      -- exclude the city's internal test record (department 'Test', team 'TestQueue')
      and coalesce(trim(department), '') != 'Test'
)

select
    nullif(trim(case_enquiry_id), '') as case_enquiry_id,

    -- Boston city data are local wall-clock times, not UTC instants.
    -- SAFE_CAST supports both space and T date separators from backfill/API.
    safe_cast(nullif(trim(open_dt), '') as datetime) as open_dt,
    safe_cast(nullif(trim(sla_target_dt), '') as datetime) as sla_target_dt,
    safe_cast(nullif(trim(closed_dt), '') as datetime) as closed_dt,

    upper(nullif(trim(on_time), '')) as on_time,
    lower(nullif(trim(case_status), '')) as case_status,
    nullif(trim(closure_reason), '') as closure_reason,
    nullif(trim(case_title), '') as case_title,
    nullif(trim(subject), '') as subject,
    nullif(trim(reason), '') as reason,
    nullif(trim(`type`), '') as `type`,
    nullif(trim(queue), '') as queue,
    nullif(trim(department), '') as department,
    nullif(trim(submitted_photo), '') as submitted_photo,
    nullif(trim(closed_photo), '') as closed_photo,
    nullif(trim(location), '') as location,
    nullif(trim(fire_district), '') as fire_district,
    nullif(trim(pwd_district), '') as pwd_district,
    nullif(trim(city_council_district), '') as city_council_district,
    nullif(trim(police_district), '') as police_district,
    nullif(trim(neighborhood), '') as neighborhood,
    nullif(trim(neighborhood_services_district), '') as neighborhood_services_district,
    nullif(trim(ward), '') as ward,
    nullif(trim(precinct), '') as precinct,
    nullif(trim(location_street_name), '') as location_street_name,
    nullif(trim(location_zipcode), '') as location_zipcode,
    safe_cast(nullif(trim(latitude), '') as float64) as latitude,
    safe_cast(nullif(trim(longitude), '') as float64) as longitude,
    nullif(trim(geom_4326), '') as geom_4326,
    nullif(trim(`source`), '') as `source`,

    -- Ingestion fields from PR #5 (not raw 311 case attributes).
    _loaded_at,
    _source
from latest