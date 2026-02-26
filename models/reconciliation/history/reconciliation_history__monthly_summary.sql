{% set dq_plan_unique_key = '[plan]' if target.type == 'fabric' else 'plan' %}

{{ config(
    materialized='incremental',
    schema='spotlight',
    tags=['spotlight', 'reconciliation_dashboard', 'dq_history'],
    unique_key=['run_id', 'data_source', 'payer', dq_plan_unique_key, 'year_month_int'],
    incremental_strategy=dq_history_incremental_strategy(),
    on_schema_change='append_new_columns'
) }}

-- depends_on: {{ ref('reconciliation__monthly_summary') }}

{% if dq_should_persist_results() %}

    select
        run_id,
        run_ts,
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }},
        year_month_int,
        year_month,
        member_months,
        members,
        claims,
        claim_lines,
        paid_amount,
        members_with_claims,
        pct_members_with_claims,
        claims_per_1000,
        pmpm_paid,
        avg_paid_per_claim,
        tuva_last_run
    from {{ ref('reconciliation__monthly_summary') }}

{% else %}

    select
        run_id,
        run_ts,
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }},
        year_month_int,
        year_month,
        member_months,
        members,
        claims,
        claim_lines,
        paid_amount,
        members_with_claims,
        pct_members_with_claims,
        claims_per_1000,
        pmpm_paid,
        avg_paid_per_claim,
        tuva_last_run
    from {{ ref('reconciliation__monthly_summary') }}
    where 1 = 0

{% endif %}
