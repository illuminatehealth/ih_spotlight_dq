-- Pull the largest dropped claims for manual review.

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
              and e.data_source = c.data_source
              and e.payer = c.payer
        ) then 1 else 0 end as same_ds_payer_person_month,
        case when exists (
            select 1
            from enrollment as e
            where e.person_id = c.person_id
              and e.month_start between dateadd(month, -1, c.claim_month_start)
                                   and dateadd(month, 1, c.claim_month_start)
        ) then 1 else 0 end as same_person_adjacent_month
    from claims as c
    where c.claim_year_month_int < 202401
)
select top (200)
    claim_year_month_int,
    data_source,
    payer,
    plan_name,
    person_id,
    claim_id,
    paid_amount,
    case
        when person_id is null then 'NULL person_id'
        when same_ds_payer_person_month = 1 then 'Likely plan mismatch'
        when same_person_adjacent_month = 1 then 'Likely month shift'
        else 'No obvious enrollment match'
    end as likely_reason
from classified
where has_exact = 0
order by paid_amount desc, claim_year_month_int;
