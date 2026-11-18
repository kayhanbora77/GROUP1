{{ config(materialized='table') }}

-- Filter 2a (f21_set_eueligible.py) expressed as SQL over ref'd models.
-- One row per ConnectionID; the rule that decided the flag is kept for audit.

with eu as (
    select distinct upper(trim("CodeIataAirport")) as code
    from {{ source('akbar', 'AIRPORTS') }}
    where "CodeIso2Country" not in ('TR', 'MA')
      and "CodeIataAirport" is not null
),

tr as (
    select distinct upper(trim("CodeIataAirport")) as code
    from {{ source('akbar', 'AIRPORTS') }}
    where "CodeIso2Country" = 'TR'
      and "CodeIataAirport" is not null
),

legs as (
    select
        "ConnectionID"                                              as connection_id,
        try_cast("LegNo" as bigint)                                 as leg_no,
        upper(trim("FromAirport"))                                  as from_ap,
        upper(trim("LastLegAirport"))                               as last_ap,
        nullif(
            coalesce(
                nullif(upper(trim("AirlineCode")), ''),
                regexp_extract(upper(trim("FlightNumber")), '^([A-Z]{2,3})', 1)
            ),
            ''
        )                                                           as airline
    from {{ ref('int_akbar_connections') }}
    where "ConnectionID" is not null
),

journey as (
    select
        connection_id,
        arg_min(from_ap, leg_no)                                    as first_ap,
        max(last_ap)                                                as last_ap,
        arg_min(airline, leg_no)                                    as airline1,
        coalesce(bool_or(airline in ('BA', 'TK', 'PC', 'JU', 'FH', 'VF', 'VS', 'XQ')), false)
                                                                    as any_special_non_eu
    from legs
    group by connection_id
),

flags as (
    select
        j.connection_id,
        j.airline1,
        j.any_special_non_eu,
        fe.code is not null                     as first_eu,
        le.code is not null                     as last_eu,
        ft.code is not null                     as first_tr,
        lt.code is not null                     as last_tr,
        j.first_ap in ('BEG', 'INI', 'KVO')     as first_srb
    from journey j
    left join eu fe on fe.code = j.first_ap
    left join eu le on le.code = j.last_ap
    left join tr ft on ft.code = j.first_ap
    left join tr lt on lt.code = j.last_ap
)

select
    connection_id as "ConnectionID",
    case
        when first_tr and last_tr                                        then 'RULE_1_TR_TO_TR'
        when first_tr and airline1 in ('LH', 'XQ', 'QR')                 then 'RULE_2_TR_DEPARTURE_SPECIAL_CARRIER'
        when first_srb and airline1 = 'JU'                               then 'RULE_3_SERBIA_JU'
        when any_special_non_eu                                          then 'RULE_4_SPECIAL_CARRIER'
        when not first_eu and not last_eu                                then 'RULE_5_BOTH_NON_EU'
        when first_eu                                                    then 'RULE_6_EU_DEPARTURE'
        else                                                                  'RULE_7_DEFAULT_NOT_ELIGIBLE'
    end as "EUEligibilityRule",
    case
        when first_tr and last_tr                                        then false
        when first_tr and airline1 in ('LH', 'XQ', 'QR')                 then true
        when first_srb and airline1 = 'JU'                               then true
        when any_special_non_eu                                          then true
        when not first_eu and not last_eu                                then false
        when first_eu                                                    then true
        else                                                                  false
    end as "EUEligible"
from flags
