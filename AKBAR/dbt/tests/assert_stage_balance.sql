-- Every raw row must land in exactly one of the two branches.
with raw as (select count(*) as n from {{ ref('stg_akbar_raw') }}),
     clean as (select count(*) as n from {{ ref('int_akbar_cleaned') }}),
     rejected as (select count(*) as n from {{ ref('int_akbar_rejected') }})
select raw.n as raw_rows, clean.n as clean_rows, rejected.n as rejected_rows
from raw, clean, rejected
where raw.n <> clean.n + rejected.n
