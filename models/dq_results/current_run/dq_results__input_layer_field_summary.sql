{{ config(
    materialized='table',
    schema='spotlight',
    tags=['spotlight', 'dq_results']
) }}

with run_meta as (

    select
        {{ dq_run_id() }} as run_id,
        {{ dq_run_ts() }} as run_ts

),

eligibility as (

    select
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }} as plan_name,
        race,
        dual_status_code
    from {{ ref('input_layer__eligibility') }}

),

medical_claim as (

    select
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }} as plan_name,
        claim_type,
        drg_code,
        revenue_center_code,
        hcpcs_code,
        diagnosis_code_1,
        diagnosis_code_2,
        diagnosis_code_3,
        procedure_code_1,
        procedure_code_2,
        procedure_code_3,
        discharge_disposition_code,
        admit_source_code,
        admit_type_code,
        bill_type_code,
        place_of_service_code,
        rendering_npi,
        billing_npi,
        facility_npi
    from {{ ref('input_layer__medical_claim') }}

),

pharmacy_claim as (

    select
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }} as plan_name,
        ndc_code
    from {{ ref('input_layer__pharmacy_claim') }}

),

field_observations as (

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'race' as field_name,
        case
            when nullif(e.race, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'dual_status_code' as field_name,
        case
            when nullif(e.dual_status_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from eligibility as e

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'drg_code' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.drg_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'revenue_center_code' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.revenue_center_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'hcpcs_code' as field_name,
        case
            when nullif(m.hcpcs_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'diagnosis_code_1' as field_name,
        case
            when nullif(m.diagnosis_code_1, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'diagnosis_code_2' as field_name,
        case
            when nullif(m.diagnosis_code_2, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'diagnosis_code_3' as field_name,
        case
            when nullif(m.diagnosis_code_3, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'procedure_code_1' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.procedure_code_1, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'procedure_code_2' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.procedure_code_2, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'procedure_code_3' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.procedure_code_3, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'discharge_disposition_code' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.discharge_disposition_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'admit_source_code' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.admit_source_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'admit_type_code' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.admit_type_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'bill_type_code' as field_name,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.bill_type_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'place_of_service_code' as field_name,
        case
            when not (m.claim_type = 'professional') then 'not_applicable'
            when nullif(m.place_of_service_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'rendering_npi' as field_name,
        case
            when nullif(m.rendering_npi, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'billing_npi' as field_name,
        case
            when nullif(m.billing_npi, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'facility_npi' as field_name,
        case
            when nullif(m.facility_npi, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from medical_claim as m

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'ndc_code' as field_name,
        case
            when nullif(p.ndc_code, '') is null then 'null'
            else 'non_null'
        end as mapping_status
    from pharmacy_claim as p

),

aggregated as (

    select
        data_source,
        payer,
        plan_name,
        model_name,
        field_name,
        count(*) as total_rows,
        sum(case when mapping_status <> 'not_applicable' then 1 else 0 end) as applicable_rows,
        sum(case when mapping_status = 'not_applicable' then 1 else 0 end) as not_applicable_rows,
        sum(case when mapping_status = 'null' then 1 else 0 end) as null_rows,
        sum(case when mapping_status = 'non_null' then 1 else 0 end) as non_null_rows
    from field_observations
    group by
        data_source,
        payer,
        plan_name,
        model_name,
        field_name

)

select
    rm.run_id,
    rm.run_ts,
    a.data_source,
    a.payer,
    a.plan_name as {{ the_tuva_project.quote_column('plan') }},
    a.model_name,
    a.field_name,
    a.total_rows,
    a.applicable_rows,
    a.not_applicable_rows,
    a.null_rows,
    a.non_null_rows,
    case when a.total_rows > 0 then 100.0 else 0.0 end as total_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.applicable_rows', 'a.total_rows') }}, 0.0) as applicable_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.not_applicable_rows', 'a.total_rows') }}, 0.0) as not_applicable_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.null_rows', 'a.total_rows') }}, 0.0) as null_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.non_null_rows', 'a.total_rows') }}, 0.0) as non_null_rows_pct
from aggregated as a
cross join run_meta as rm
order by
    a.data_source,
    a.payer,
    a.plan_name,
    a.model_name,
    a.field_name
