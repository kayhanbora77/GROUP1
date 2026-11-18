{{ config(materialized='view') }}

-- Replacement for the AKBAR_REJECTED table that f11 wrote directly.
select *
from {{ ref('int_akbar_scored') }}
where "RejectionReason" is not null
