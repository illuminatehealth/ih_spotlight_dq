-- For PSW_TRC_CCLF only, rank the members driving the paid amount drop
-- because they do not have an exact enrollment match on the reconciliation keys:
-- data_source + payer + plan + person_id + year_month.

with claims as (
    select
        mc.data_source,
        mc.payer,
        mc.[plan] as plan_name,
        mc.person_id,
        mc.claim_id,
        mc.paid_amount,
        coalesce(mc.claim_line_start_date, mc.claim_start_date) as claim_date,
        year(coalesce(mc.claim_line_start_date, mc.claim_start_date)) * 100
            + month(coalesce(mc.claim_line_start_date, mc.claim_start_date)) as claim_year_month_int
    from core.medical_claim as mc
    where mc.data_source = 'PSW_TRC_CCLF'
      and coalesce(mc.claim_line_start_date, mc.claim_start_date) is not null
      and mc.paid_amount is not null
      and year(coalesce(mc.claim_line_start_date, mc.claim_start_date)) * 100
            + month(coalesce(mc.claim_line_start_date, mc.claim_start_date)) < 202401
),
enrollment as (
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
unmatched_claims as (
    select
        c.*
    from claims as c
    left join enrollment as e
        on c.data_source = e.data_source
        and c.payer = e.payer
        and c.plan_name = e.plan_name
        and c.person_id = e.person_id
        and c.claim_year_month_int = e.year_month_int
    where e.person_id is null
)
select
    person_id,
    sum(paid_amount) as unmatched_paid_amount,
    count(*) as unmatched_claim_lines,
    count(distinct claim_id) as unmatched_claims,
    min(claim_year_month_int) as first_unmatched_month,
    max(claim_year_month_int) as last_unmatched_month,
    min(claim_date) as first_unmatched_date,
    max(claim_date) as last_unmatched_date,
    count(distinct payer) as payer_count,
    count(distinct plan_name) as plan_count
from unmatched_claims
group by person_id
order by unmatched_paid_amount desc, unmatched_claims desc;
