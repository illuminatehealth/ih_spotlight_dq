-- Compare drop-off rates before 2024 versus 2024 and later by source/payer/plan.

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
),
joined as (
    select
        c.claim_year_month_int,
        c.data_source,
        c.payer,
        c.plan_name,
        c.claim_id,
        c.paid_amount,
        case when e.person_id is not null then 1 else 0 end as matched
    from claims as c
    left join enrollment as e
        on c.data_source = e.data_source
        and c.payer = e.payer
        and c.plan_name = e.plan_name
        and c.person_id = e.person_id
        and c.claim_year_month_int = e.year_month_int
)
select
    data_source,
    payer,
    plan_name,
    case when claim_year_month_int < 202401 then 'pre_2024' else '2024_plus' end as period_bucket,
    sum(paid_amount) as paid_without_join,
    sum(case when matched = 1 then paid_amount else 0 end) as paid_with_join,
    sum(case when matched = 0 then paid_amount else 0 end) as dropped_paid,
    100.0 * sum(case when matched = 0 then paid_amount else 0 end) / nullif(sum(paid_amount), 0) as dropped_pct
from joined
group by
    data_source,
    payer,
    plan_name,
    case when claim_year_month_int < 202401 then 'pre_2024' else '2024_plus' end
order by dropped_pct desc, dropped_paid desc;
