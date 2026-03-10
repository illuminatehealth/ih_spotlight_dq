-- Diagnose how much paid amount is lost by the reconciliation enrollment join.
-- Tuva compiles core__medical_claim to core.medical_claim and
-- core__member_months to core.member_months.

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
    where coalesce(mc.claim_line_start_date, mc.claim_start_date) is not null
      and mc.paid_amount is not null
),
enrollment as (
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
    c.claim_year_month_int,
    c.data_source,
    c.payer,
    c.plan_name,
    sum(c.paid_amount) as paid_without_join,
    sum(case when e.person_id is not null then c.paid_amount else 0 end) as paid_with_join,
    sum(case when e.person_id is null then c.paid_amount else 0 end) as dropped_paid,
    100.0 * sum(case when e.person_id is null then c.paid_amount else 0 end)
        / nullif(sum(c.paid_amount), 0) as dropped_pct
from claims as c
left join enrollment as e
    on c.data_source = e.data_source
    and c.payer = e.payer
    and c.plan_name = e.plan_name
    and c.person_id = e.person_id
    and c.claim_year_month_int = e.year_month_int
where c.claim_year_month_int < 202401
group by
    c.claim_year_month_int,
    c.data_source,
    c.payer,
    c.plan_name
having sum(case when e.person_id is null then c.paid_amount else 0 end) > 0
order by dropped_paid desc;
