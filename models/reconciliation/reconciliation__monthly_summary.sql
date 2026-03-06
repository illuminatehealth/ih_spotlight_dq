{{ config(
    materialized = 'table',
    schema = 'spotlight',
    tags = ['spotlight', 'reconciliation_dashboard']
) }}

with run_meta as (

    select
        {{ dq_run_id() }} as run_id,
        {{ dq_run_ts() }} as run_ts

),

calendar_month as (
    select distinct
        year_month_int,
        year_month
    from {{ ref('reference_data__calendar') }}
    where day = 1
),

member_months_normalized as (
    select
        mm.data_source,
        mm.payer,
        mm.{{ the_tuva_project.quote_column('plan') }} as plan_name,
        mm.person_id,
        cast(mm.year_month as {{ dbt.type_int() }}) as year_month_int
    from {{ ref('core__member_months') }} as mm
),

medical_claims_normalized as (
    select
        mc.data_source,
        mc.payer,
        mc.{{ the_tuva_project.quote_column('plan') }} as plan_name,
        mc.person_id,
        mc.claim_id,
        mc.paid_amount,
        c.year_month_int
    from {{ ref('core__medical_claim') }} as mc
    inner join {{ ref('reference_data__calendar') }} as c
        on coalesce(mc.claim_line_start_date, mc.claim_start_date) = c.full_date
),

enrolled_member_months as (
    select distinct
        data_source,
        payer,
        plan_name,
        person_id,
        year_month_int
    from member_months_normalized
),

medical_claims_with_enrollment as (
    select
        mc.data_source,
        mc.payer,
        mc.plan_name,
        mc.person_id,
        mc.claim_id,
        mc.paid_amount,
        mc.year_month_int
    from medical_claims_normalized as mc
    inner join enrolled_member_months as em
        on mc.data_source = em.data_source
        and mc.payer = em.payer
        and mc.plan_name = em.plan_name
        and mc.person_id = em.person_id
        and mc.year_month_int = em.year_month_int
),

month_ranges as (
    select
        data_source,
        payer,
        plan_name,
        min(year_month_int) as min_year_month_int,
        max(year_month_int) as max_year_month_int
    from member_months_normalized
    group by
        data_source,
        payer,
        plan_name

    union all

    select
        data_source,
        payer,
        plan_name,
        min(year_month_int) as min_year_month_int,
        max(year_month_int) as max_year_month_int
    from medical_claims_normalized
    group by
        data_source,
        payer,
        plan_name

    union all

    select
        data_source,
        payer,
        plan_name,
        min(year_month_int) as min_year_month_int,
        max(year_month_int) as max_year_month_int
    from medical_claims_with_enrollment
    group by
        data_source,
        payer,
        plan_name
),

month_bounds as (
    select
        data_source,
        payer,
        plan_name,
        min(min_year_month_int) as min_year_month_int,
        max(max_year_month_int) as max_year_month_int
    from month_ranges
    group by
        data_source,
        payer,
        plan_name
),

month_spine as (
    select
        b.data_source,
        b.payer,
        b.plan_name,
        c.year_month_int,
        c.year_month
    from month_bounds as b
    inner join calendar_month as c
        on c.year_month_int between b.min_year_month_int and b.max_year_month_int
),

member_month_agg as (
    select
        data_source,
        payer,
        plan_name,
        year_month_int,
        count(*) as member_months,
        count(distinct person_id) as members
    from member_months_normalized
    group by
        data_source,
        payer,
        plan_name,
        year_month_int
),

medical_claim_metrics_all_agg as (
    select
        data_source,
        payer,
        plan_name,
        year_month_int,
        sum(paid_amount) as paid_amount_without_enrollment
    from medical_claims_normalized
    group by
        data_source,
        payer,
        plan_name,
        year_month_int
),

medical_claim_metrics_agg as (
    select
        data_source,
        payer,
        plan_name,
        year_month_int,
        count(distinct claim_id) as claims,
        count(*) as claim_lines,
        sum(paid_amount) as paid_amount,
        count(distinct person_id) as members_with_claims
    from medical_claims_with_enrollment
    group by
        data_source,
        payer,
        plan_name,
        year_month_int
)

select
    rm.run_id,
    rm.run_ts,
    s.data_source,
    s.payer,
    s.plan_name as {{ the_tuva_project.quote_column('plan') }},
    s.year_month_int,
    s.year_month,
    coalesce(m.member_months, 0) as member_months,
    coalesce(m.members, 0) as members,
    coalesce(c.claims, 0) as claims,
    coalesce(c.claim_lines, 0) as claim_lines,
    coalesce(c.paid_amount, 0) as paid_amount,
    coalesce(ca.paid_amount_without_enrollment, 0) as paid_amount_without_enrollment,
    coalesce(c.members_with_claims, 0) as members_with_claims,
    cast(coalesce(c.members_with_claims, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as pct_members_with_claims,
    cast(coalesce(c.claims, 0) as {{ dbt.type_numeric() }}) * 1000
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as claims_per_1000,
    cast(coalesce(c.paid_amount, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as pmpm_paid,
    cast(coalesce(ca.paid_amount_without_enrollment, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(m.member_months, 0) as {{ dbt.type_numeric() }}), 0) as pmpm_paid_without_enrollment,
    cast(coalesce(c.paid_amount, 0) as {{ dbt.type_numeric() }})
        / nullif(cast(coalesce(c.claims, 0) as {{ dbt.type_numeric() }}), 0) as avg_paid_per_claim,
    {{ dq_run_ts() }} as tuva_last_run
from month_spine as s
cross join run_meta as rm
left join member_month_agg as m
    on s.data_source = m.data_source
    and s.payer = m.payer
    and s.plan_name = m.plan_name
    and s.year_month_int = m.year_month_int
left join medical_claim_metrics_agg as c
    on s.data_source = c.data_source
    and s.payer = c.payer
    and s.plan_name = c.plan_name
    and s.year_month_int = c.year_month_int
left join medical_claim_metrics_all_agg as ca
    on s.data_source = ca.data_source
    and s.payer = ca.payer
    and s.plan_name = ca.plan_name
    and s.year_month_int = ca.year_month_int
