-- One row per service request across the legacy and new 311 systems.
-- Staging already keeps the latest row per case; here we align names,
-- combine both systems, and define shared business fields once.

with legacy as (
    select
        'legacy'                      as source_system,
        case_enquiry_id               as case_id,
        open_dt                       as opened_at,
        closed_dt                     as closed_at,
        sla_target_dt                 as target_close_at,
        case_status,
        on_time,
        closure_reason,
        `type`                        as service_type,
        cast(null as string)          as case_topic,
        subject,
        reason,
        case_title,
        department,
        queue                         as team,
        `source`                      as report_source,
        location                      as full_address,
        location_street_name          as street_name,
        location_zipcode              as zip_code,
        neighborhood,
        ward,
        precinct,
        city_council_district,
        fire_district,
        police_district,
        latitude,
        longitude,
        cast(null as string)          as closure_comments,
        submitted_photo,
        closed_photo,
        _loaded_at,
        _source,
        -- 2024/2025 legacy files are a one-time snapshot; their open statuses are never refreshed (PR #5)
        _source not in ('backfill_legacy_2024', 'backfill_legacy_2025') as is_status_current
    from {{ ref('stg_legacy_311') }}
),

new_cases as (
    select
        'new'                         as source_system,
        case_id,
        open_date                     as opened_at,
        close_date                    as closed_at,
        target_close_date             as target_close_at,
        case_status,
        on_time,
        closure_reason,
        service_name                  as service_type,
        case_topic,
        cast(null as string)          as subject,
        cast(null as string)          as reason,
        cast(null as string)          as case_title,
        assigned_department           as department,
        assigned_team                 as team,
        report_source,
        full_address,
        street_name,
        zip_code,
        neighborhood,
        ward,
        precinct,
        city_council_district,
        fire_district,
        police_district,
        latitude,
        longitude,
        closure_comments,
        submitted_photo,
        closed_photo,
        _loaded_at,
        _source,
        true                          as is_status_current
    from {{ ref('stg_new_311') }}
),

-- UNION ALL matches columns by position, so both CTEs list them in the same order.
-- Safe here: each staging model has one row per case, and no case ID appears in both systems (checked in #9).
combined as (
    select * from legacy
    union all
    select * from new_cases
)

select
    concat(combined.source_system, ':', combined.case_id)                          as service_request_key,
    combined.* replace (
        lpad(zip_code, 5, '0')                            as zip_code,  -- restore lost leading zero: 2135 -> 02135
        nullif(safe_cast(regexp_extract(ward, r'\d+') as int64), 0) as ward  -- 'Ward 7', '07', '7' -> 7; ward 0 = no ward -> NULL
    ),
    case when coalesce(case_status, '') = 'closed' then 'closed' else 'open' end as status_group,
    coalesce(case_status, '') = 'closed'                         as is_closed,
    date(opened_at)                                              as opened_date,
    case
        when coalesce(case_status, '') = 'closed' and opened_at is not null and closed_at is not null
            then datetime_diff(closed_at, opened_at, minute) / 60.0
    end                                                          as resolution_hours,
    target_close_at is not null                                  as has_target,
    case
        when target_close_at is null then null
        when coalesce(case_status, '') = 'closed'
            then case when closed_at is null then null else closed_at > target_close_at end
        when is_status_current
            then current_datetime('America/New_York') > target_close_at
        else null
    end                                                          as is_overdue,
    -- readable department name (legacy codes mapped via seed; new-system names already readable)
    coalesce(dm.department_name, combined.department, '(unknown)')      as department_name,
    -- shared, coarser neighborhood level that both systems can be compared at
    coalesce(nm.neighborhood_group, combined.neighborhood, '(unknown)') as neighborhood_group
from combined
left join {{ ref('department_map') }} as dm
    on dm.source_system = combined.source_system and dm.department = combined.department
left join {{ ref('neighborhood_map') }} as nm
    on nm.neighborhood = combined.neighborhood
