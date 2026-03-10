-- Check whether simple trim/upper normalization on join fields would recover
-- a material amount of dropped paid amount.

with claims as (
    select
        mc.data_source,
        mc.payer,
        mc.[plan] as plan_name,
        upper(ltrim(rtrim(coalesce(mc.data_source, '')))) as data_source_norm,
        upper(ltrim(rtrim(coalesce(mc.payer, '')))) as payer_norm,
        upper(ltrim(rtrim(coalesce(mc.[plan], '')))) as plan_name_norm,
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
        upper(ltrim(rtrim(coalesce(mm.data_source, '')))) as data_source_norm,
        upper(ltrim(rtrim(coalesce(mm.payer, '')))) as payer_norm,
        upper(ltrim(rtrim(coalesce(mm.[plan], '')))) as plan_name_norm,
        mm.person_id,
        try_convert(int, mm.year_month) as year_month_int
    from core.member_months as mm
    where try_convert(int, mm.year_month) is not null
),
matched as (
    select
        c.*,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.year_month_int = c.claim_year_month_int
              and e.data_source = c.data_source
              and e.payer = c.payer
              and e.plan_name = c.plan_name
        ) then 1 else 0 end as raw_match,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.year_month_int = c.claim_year_month_int
              and e.data_source_norm = c.data_source_norm
              and e.payer_norm = c.payer_norm
              and e.plan_name_norm = c.plan_name_norm
        ) then 1 else 0 end as normalized_match
    from claims as c
    where c.claim_year_month_int < 202401
)
select
    claim_year_month_int,
    sum(case when raw_match = 0 and normalized_match = 1 then paid_amount else 0 end) as recovered_paid,
    count(distinct case when raw_match = 0 and normalized_match = 1 then claim_id end) as recovered_claims
from matched
group by claim_year_month_int
having sum(case when raw_match = 0 and normalized_match = 1 then paid_amount else 0 end) > 0
order by claim_year_month_int;
