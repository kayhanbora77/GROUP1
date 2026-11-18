{{ config(materialized='view') }}

-- Entry point of the dbt DAG: the rows loaded by filter0/f01_create_table.py.
-- Optional var('row_limit') keeps dev/test runs to a few thousand rows; the
-- production run leaves it null and sees all 11.2M rows.

select *
from {{ source('akbar', 'AKBAR_RAW') }}
{% if var('row_limit', none) is not none %}
qualify row_number() over (order by "Id") <= {{ var('row_limit') }}
{% endif %}
