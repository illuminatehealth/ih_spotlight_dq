-- For unmatched PSW_TRC_CCLF member-months, surface any enrollment rows for the
-- same member in the same month, adjacent months, or under a different payer/plan.

with claims as (
    select
        mc.data_source,
        mc.payer,
        mc.[plan] as plan_name,
        mc.person_id,
        mc.claim_id,
        mc.paid_amount,
        year(coalesce(mc.claim_line_start_date, mc.claim_start_date)) * 100
            + month(coalesce(mc.claim_line_start_date, mc.claim_start_date)) as claim_year_month_int
    from core.medical_claim as mc
    where mc.data_source = 'PSW_TRC_CCLF'
      and coalesce(mc.claim_line_start_date, mc.claim_start_date) is not null
      and mc.paid_amount is not null
      and year(coalesce(mc.claim_line_start_date, mc.claim_start_date)) * 100
            + month(coalesce(mc.claim_line_start_date, mc.claim_start_date)) < 202401
),
psw_enrollment as (
    select distinct
        mm.data_source,
        mm.payer,
        mm.[plan] as plan_name,
        mm.person_id,
        try_convert(int, mm.year_month) as year_month_int
    from core.member_months as mm
    where mm.data_source = 'PSW_TRC_CCLF'
      and try_convert(int, mm.year_month) is not null
),
unmatched_member_months as (
    select
        c.person_id,
        c.payer,
        c.plan_name,
        c.claim_year_month_int,
        sum(c.paid_amount) as unmatched_paid_amount,
        count(distinct c.claim_id) as unmatched_claims
    from claims as c
    left join psw_enrollment as e
        on c.data_source = e.data_source
        and c.payer = e.payer
        and c.plan_name = e.plan_name
        and c.person_id = e.person_id
        and c.claim_year_month_int = e.year_month_int
    where e.person_id is null
    group by
        c.person_id,
        c.payer,
        c.plan_name,
        c.claim_year_month_int
),
all_enrollment as (
    select distinct
        mm.data_source,
        mm.payer,
        mm.[plan] as plan_name,
        mm.person_id,
        try_convert(int, mm.year_month) as year_month_int
    from core.member_months as mm
    where try_convert(int, mm.year_month) is not null
)
select
    u.person_id,
    u.claim_year_month_int,
    u.payer as claim_payer,
    u.plan_name as claim_plan_name,
    u.unmatched_paid_amount,
    u.unmatched_claims,
    e.data_source as enrollment_data_source,
    e.payer as enrollment_payer,
    e.plan_name as enrollment_plan_name,
    e.year_month_int as enrollment_year_month_int,
    case
        when e.year_month_int = u.claim_year_month_int
            and e.data_source = 'PSW_TRC_CCLF'
            and e.payer <> u.payer
            and e.plan_name = u.plan_name then 'Same month, payer mismatch'
        when e.year_month_int = u.claim_year_month_int
            and e.data_source = 'PSW_TRC_CCLF'
            and e.payer = u.payer
            and e.plan_name <> u.plan_name then 'Same month, plan mismatch'
        when e.year_month_int = u.claim_year_month_int
            and e.data_source <> 'PSW_TRC_CCLF' then 'Same month, different data source'
        when e.year_month_int between u.claim_year_month_int - 1 and u.claim_year_month_int + 1
            then 'Adjacent month'
        else 'Other enrollment row'
    end as enrollment_relationship
from unmatched_member_months as u
left join all_enrollment as e
    on e.person_id = u.person_id
    and e.year_month_int between u.claim_year_month_int - 1 and u.claim_year_month_int + 1
where e.person_id is not null
order by
    u.unmatched_paid_amount desc,
    u.person_id,
    u.claim_year_month_int,
    e.year_month_int;
