with legacy_categories as (
    select distinct
        'Legacy' as system,
        nullif(trim(subject), '') as category_level_1,
        nullif(trim(reason), '') as category_level_2,
        nullif(trim(`type`), '') as service_type
    from {{ ref('stg_legacy_311') }}
    where coalesce(
        nullif(trim(subject), ''),
        nullif(trim(reason), ''),
        nullif(trim(`type`), '')
    ) is not null
),

new_categories as (
    select distinct
        'New' as system,
        nullif(trim(case_topic), '') as category_level_1,
        cast(null as string) as category_level_2,
        nullif(trim(service_name), '') as service_type
    from {{ ref('stg_new_311') }}
    where coalesce(
        nullif(trim(case_topic), ''),
        nullif(trim(service_name), '')
    ) is not null
)

-- UNION ALL keeps one unique row for each category combination PER SYSTEM.
-- Do not force service_type NOT NULL: the new system has missing names.
select system, category_level_1, category_level_2, service_type
from legacy_categories

union all

select system, category_level_1, category_level_2, service_type
from new_categories

