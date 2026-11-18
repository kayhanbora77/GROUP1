{{ config(materialized='view') }}

-- Replacement for the AKBAR_CLEANED table that f11 wrote directly.
select * exclude ("RejectionReason")
from {{ ref('int_akbar_scored') }}
where "RejectionReason" is null
