{{ config(materialized='table') }}

-- Cheap monitoring surface: one row per pipeline stage, so a scheduler or a
-- dashboard can alert when a stage drops to zero rows.

select 'stg_akbar_raw' as stage, count(*) as row_count from {{ ref('stg_akbar_raw') }}
union all
select 'int_akbar_cleaned', count(*) from {{ ref('int_akbar_cleaned') }}
union all
select 'int_akbar_rejected', count(*) from {{ ref('int_akbar_rejected') }}
union all
select 'int_akbar_connections', count(*) from {{ ref('int_akbar_connections') }}
union all
select 'ta_standard_akbar', count(*) from {{ ref('ta_standard_akbar') }}
union all
select 'ta_standard_akbar_eu_eligible', count(*) from {{ ref('ta_standard_akbar') }} where "EUEligible"
union all
select 'ta_standard_akbar_time_limit_l1', count(*) from {{ ref('ta_standard_akbar') }} where "IsTimeLimitL1"
