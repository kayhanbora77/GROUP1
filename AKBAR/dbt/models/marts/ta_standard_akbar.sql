{{ config(materialized='table') }}

-- Filter 2b (f22_set_timelimits.py), set-based instead of load-pandas-and-UPDATE.
-- Publishes the same 32 columns as main.TA_STANDARD_AKBAR, with the three flag
-- columns as real BOOLEANs (they are VARCHAR/NULL in the legacy table).

with conn as (
    select * from {{ ref('int_akbar_connections') }}
),

eu as (
    select "ConnectionID", "EUEligible", "EUEligibilityRule"
    from {{ ref('int_akbar_eu_eligibility') }}
),

airports as (
    -- One row per IATA code: joining AIRPORTS twice un-deduped would fan out legs.
    select
        upper(trim("CodeIataAirport")) as code,
        any_value("NameCountry")       as country
    from {{ source('akbar', 'AIRPORTS') }}
    where "CodeIataAirport" is not null
    group by 1
),

limits_tbl as (
    select
        upper(trim("Country")) as country,
        cast("LimitL1" as double) as limit_l1,
        cast("LimitL2" as double) as limit_l2
    from {{ source('akbar', 'TIME_LIMITS') }}
),

special as (
    select * from (values
        ('BA', 6.0, 6.0), ('TK', 2.0, 2.0), ('PC', 2.0, 2.0), ('JU', 2.0, 2.0),
        ('FH', 2.0, 2.0), ('VF', 2.0, 2.0), ('VS', 6.0, 6.0), ('XQ', 2.0, 2.0)
    ) as t(code, l1, l2)
),

-- Per-leg view with the departure/arrival country limits attached.
legs as (
    select
        c."ConnectionID",
        c."AirlineCode",
        c."FlightNumber",
        try_cast(c."DepartureDate" as timestamp)                as dep_ts,
        coalesce(nullif(upper(trim(c."AirlineCode")), ''),
                 regexp_extract(upper(trim(c."FlightNumber")), '^([A-Z]{2,3})', 1)) as airline,
        coalesce(dl.limit_l1, 0.0)                              as dep_l1,
        coalesce(al.limit_l1, 0.0)                              as arr_l1,
        coalesce(dl.limit_l2, 0.0)                              as dep_l2,
        coalesce(al.limit_l2, 0.0)                              as arr_l2
    from conn c
    left join airports d on upper(trim(c."FromAirport")) = d.code
    left join limits_tbl dl on d.country = dl.country
    left join airports a on upper(trim(c."ToAirport")) = a.code
    left join limits_tbl al on a.country = al.country
),

-- One row per journey: the earliest leg sets the airline and the departure date.
journey as (
    select
        "ConnectionID",
        arg_min(airline, dep_ts)    as airline1,
        min(dep_ts)                 as dep_ts,
        max(dep_l1)                 as dep_l1,
        max(arr_l1)                 as arr_l1,
        max(dep_l2)                 as dep_l2,
        max(arr_l2)                 as arr_l2
    from legs
    group by "ConnectionID"
),

time_limits as (
    select
        j."ConnectionID",
        case when j.dep_l1 = 0 and j.arr_l1 = 0 and j.dep_l2 = 0 and j.arr_l2 = 0
             then coalesce(s.l1, 0.0)
             else greatest(j.dep_l1, j.arr_l1)
        end as limit_l1,
        case when j.dep_l1 = 0 and j.arr_l1 = 0 and j.dep_l2 = 0 and j.arr_l2 = 0
             then coalesce(s.l2, 0.0)
             else greatest(j.dep_l2, j.arr_l2)
        end as limit_l2,
        (date '{{ var("reference_date") }}' - j.dep_ts::date) / 365.25 as age_years
    from journey j
    left join special s on j.airline1 = s.code
)

select
    c.* replace (
        coalesce(e."EUEligible", false)                       as "EUEligible",
        -- f22 only updates EU-eligible journeys; keeping the same gate here
        -- stops a non-EU journey from ever carrying a time-limit flag.
        coalesce(coalesce(e."EUEligible", false)
                 and t.limit_l1 >= t.age_years, false)        as "IsTimeLimitL1",
        coalesce(coalesce(e."EUEligible", false)
                 and t.limit_l2 >= t.age_years, false)        as "IsTimeLimitL2"
    )
from conn c
left join eu e on c."ConnectionID" = e."ConnectionID"
left join time_limits t on c."ConnectionID" = t."ConnectionID"
