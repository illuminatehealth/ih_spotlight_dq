-- Bucket dropped paid amounts into likely failure modes relative to the
-- reconciliation enrollment join.

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
            + month(coalesce(mc.claim_line_start_date, mc.claim_start_date)) as claim_year_month_int,
        datefromparts(
            year(coalesce(mc.claim_line_start_date, mc.claim_start_date)),
            month(coalesce(mc.claim_line_start_date, mc.claim_start_date)),
            1
        ) as claim_month_start
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
        try_convert(int, mm.year_month) as year_month_int,
        datefromparts(
            try_convert(int, mm.year_month) / 100,
            try_convert(int, mm.year_month) % 100,
            1
        ) as month_start
    from core.member_months as mm
    where try_convert(int, mm.year_month) is not null
),
classified as (
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
        ) then 1 else 0 end as has_exact,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.year_month_int = c.claim_year_month_int
        ) then 1 else 0 end as has_person_month,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.year_month_int = c.claim_year_month_int
              and e.data_source = c.data_source
        ) then 1 else 0 end as has_person_month_ds,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.year_month_int = c.claim_year_month_int
              and e.data_source = c.data_source
              and e.payer = c.payer
        ) then 1 else 0 end as has_person_month_ds_payer,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.data_source = c.data_source
              and e.payer = c.payer
              and e.plan_name = c.plan_name
              and e.month_start between dateadd(month, -1, c.claim_month_start)
                                   and dateadd(month, 1, c.claim_month_start)
        ) then 1 else 0 end as has_adjacent_month
    from claims as c
    where c.claim_year_month_int < 202401
)
select
    claim_year_month_int,
    case
        when person_id is null then 'NULL person_id'
        when has_person_month_ds_payer = 1 then 'Plan mismatch'
        when has_person_month_ds = 1 then 'Payer mismatch'
        when has_person_month = 1 then 'Data source mismatch'
        when has_adjacent_month = 1 then 'Enrollment month off by +/-1'
        else 'No enrollment for person/month'
    end as mismatch_reason,
    sum(paid_amount) as dropped_paid,
    count(*) as dropped_rows,
    count(distinct claim_id) as dropped_claims
from classified
where has_exact = 0
group by
    claim_year_month_int,
    case
        when person_id is null then 'NULL person_id'
        when has_person_month_ds_payer = 1 then 'Plan mismatch'
        when has_person_month_ds = 1 then 'Payer mismatch'
        when has_person_month = 1 then 'Data source mismatch'
        when has_adjacent_month = 1 then 'Enrollment month off by +/-1'
        else 'No enrollment for person/month'
    end
order by claim_year_month_int, dropped_paid desc;
