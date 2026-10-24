-- Fails if combining the two systems lost or added rows.
with staging as (
    select (select count(*) from {{ ref('stg_legacy_311') }})
         + (select count(*) from {{ ref('stg_new_311') }}) as n
),
unified as (
    select count(*) as n from {{ ref('int_service_requests_unified') }}
)
select staging.n as staging_rows, unified.n as unified_rows
from staging cross join unified
where staging.n != unified.n
