-- LegNo must run 1..n inside each ConnectionID: no gaps, no duplicates.
with per_conn as (
    select
        "ConnectionID",
        count(*)                                    as legs,
        count(distinct "LegNo")                     as distinct_legs,
        min("LegNo")                                as min_leg,
        max("LegNo")                                as max_leg
    from {{ ref('ta_standard_akbar') }}
    where "ConnectionID" is not null
    group by "ConnectionID"
)
select *
from per_conn
where min_leg <> 1
   or max_leg <> legs
   or distinct_legs <> legs
limit 20
