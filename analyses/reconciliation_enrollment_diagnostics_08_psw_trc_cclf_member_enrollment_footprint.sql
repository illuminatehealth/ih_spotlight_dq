-- For unmatched PSW_TRC_CCLF members, show whether they appear in member_months
-- at all, in PSW_TRC_CCLF only, or only under other data sources/payers/plans.

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
unmatched_members as (
    select
        c.person_id,
        sum(c.paid_amount) as unmatched_paid_amount,
        count(distinct c.claim_id) as unmatched_claims,
        min(c.claim_year_month_int) as first_unmatched_month,
        max(c.claim_year_month_int) as last_unmatched_month
    from claims as c
    left join psw_enrollment as e
        on c.data_source = e.data_source
        and c.payer = e.payer
        and c.plan_name = e.plan_name
        and c.person_id = e.person_id
        and c.claim_year_month_int = e.year_month_int
    where e.person_id is null
    group by c.person_id
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
    u.unmatched_paid_amount,
    u.unmatched_claims,
    u.first_unmatched_month,
    u.last_unmatched_month,
    case when exists (
        select 1
        from all_enrollment as e
        where e.person_id = u.person_id
    ) then 1 else 0 end as has_any_enrollment_anywhere,
    case when exists (
        select 1
        from all_enrollment as e
        where e.person_id = u.person_id
          and e.data_source = 'PSW_TRC_CCLF'
    ) then 1 else 0 end as has_any_enrollment_in_psw_trc_cclf,
    (
        select min(e.year_month_int)
        from all_enrollment as e
        where e.person_id = u.person_id
    ) as first_enrollment_month_anywhere,
    (
        select max(e.year_month_int)
        from all_enrollment as e
        where e.person_id = u.person_id
    ) as last_enrollment_month_anywhere,
    (
        select count(distinct e.data_source)
        from all_enrollment as e
        where e.person_id = u.person_id
    ) as enrollment_data_source_count,
    (
        select count(distinct e.payer)
        from all_enrollment as e
        where e.person_id = u.person_id
    ) as enrollment_payer_count,
    (
        select count(distinct e.plan_name)
        from all_enrollment as e
        where e.person_id = u.person_id
    ) as enrollment_plan_count
from unmatched_members as u
order by u.unmatched_paid_amount desc, u.person_id;
