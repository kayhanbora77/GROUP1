-- The current main.TA_STANDARD_AKBAR has all three flags NULL on 11.24M rows,
-- because f13 was re-run after f21/f22. This test makes that state impossible:
-- any journey without a decided EU flag, or with a time-limit flag but no EU
-- flag, fails the build.
select
    count(*)                                                   as offending_rows,
    count(*) filter (where "EUEligible" is null)               as eu_null,
    count(*) filter (where "IsTimeLimitL1" is null)            as l1_null,
    count(*) filter (where "IsTimeLimitL2" is null)            as l2_null,
    count(*) filter (where ("IsTimeLimitL1" or "IsTimeLimitL2") and not "EUEligible")
                                                               as timelimit_without_eu
from {{ ref('ta_standard_akbar') }}
where "ConnectionID" is not null
  and (
        "EUEligible" is null
     or "IsTimeLimitL1" is null
     or "IsTimeLimitL2" is null
     or (("IsTimeLimitL1" or "IsTimeLimitL2") and not "EUEligible")
  )
having count(*) > 0
