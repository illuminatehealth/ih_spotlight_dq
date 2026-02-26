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
        drg_code_type,
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
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'drg_code' as field_name,
        'terminology__ms_drg|terminology__apr_drg' as terminology_table,
        'ms_drg_code|apr_drg_code' as terminology_field,
        case
            when m.claim_type <> 'institutional' then 'not_applicable'
            when nullif(m.drg_code, '') is null then 'null'
            when (m.drg_code_type = 'ms-drg' and ms.ms_drg_code is not null)
                or (m.drg_code_type = 'apr-drg' and apr.apr_drg_code is not null)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__ms_drg') }} as ms
        on m.drg_code = ms.ms_drg_code
        and m.drg_code_type = 'ms-drg'
    left join {{ ref('terminology__apr_drg') }} as apr
        on m.drg_code = apr.apr_drg_code
        and m.drg_code_type = 'apr-drg'

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'race' as field_name,
        'terminology__race' as terminology_table,
        'description' as terminology_field,
        case
            when nullif(e.race, '') is null then 'null'
            when t.description is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e
    left join {{ ref('terminology__race') }} as t
        on e.race = t.description

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'dual_status_code' as field_name,
        'terminology__medicare_dual_eligibility' as terminology_table,
        'dual_status_code' as terminology_field,
        case
            when nullif(e.dual_status_code, '') is null then 'null'
            when t.dual_status_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e
    left join {{ ref('terminology__medicare_dual_eligibility') }} as t
        on e.dual_status_code = t.dual_status_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'revenue_center_code' as field_name,
        'terminology__revenue_center' as terminology_table,
        'revenue_center_code' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.revenue_center_code, '') is null then 'null'
            when t.revenue_center_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__revenue_center') }} as t
        on m.revenue_center_code = t.revenue_center_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'hcpcs_code' as field_name,
        'terminology__hcpcs_level_2' as terminology_table,
        'hcpcs' as terminology_field,
        case
            when nullif(m.hcpcs_code, '') is null then 'null'
            when t.hcpcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__hcpcs_level_2') }} as t
        on m.hcpcs_code = t.hcpcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'diagnosis_code_1' as field_name,
        'terminology__icd_10_cm' as terminology_table,
        'icd_10_cm' as terminology_field,
        case
            when nullif(m.diagnosis_code_1, '') is null then 'null'
            when t.icd_10_cm is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__icd_10_cm') }} as t
        on m.diagnosis_code_1 = t.icd_10_cm

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'diagnosis_code_2' as field_name,
        'terminology__icd_10_cm' as terminology_table,
        'icd_10_cm' as terminology_field,
        case
            when nullif(m.diagnosis_code_2, '') is null then 'null'
            when t.icd_10_cm is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__icd_10_cm') }} as t
        on m.diagnosis_code_2 = t.icd_10_cm

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'diagnosis_code_3' as field_name,
        'terminology__icd_10_cm' as terminology_table,
        'icd_10_cm' as terminology_field,
        case
            when nullif(m.diagnosis_code_3, '') is null then 'null'
            when t.icd_10_cm is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__icd_10_cm') }} as t
        on m.diagnosis_code_3 = t.icd_10_cm

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'procedure_code_1' as field_name,
        'terminology__icd_10_pcs' as terminology_table,
        'icd_10_pcs' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.procedure_code_1, '') is null then 'null'
            when t.icd_10_pcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__icd_10_pcs') }} as t
        on m.procedure_code_1 = t.icd_10_pcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'procedure_code_2' as field_name,
        'terminology__icd_10_pcs' as terminology_table,
        'icd_10_pcs' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.procedure_code_2, '') is null then 'null'
            when t.icd_10_pcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__icd_10_pcs') }} as t
        on m.procedure_code_2 = t.icd_10_pcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'procedure_code_3' as field_name,
        'terminology__icd_10_pcs' as terminology_table,
        'icd_10_pcs' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.procedure_code_3, '') is null then 'null'
            when t.icd_10_pcs is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__icd_10_pcs') }} as t
        on m.procedure_code_3 = t.icd_10_pcs

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'discharge_disposition_code' as field_name,
        'terminology__discharge_disposition' as terminology_table,
        'discharge_disposition_code' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.discharge_disposition_code, '') is null then 'null'
            when t.discharge_disposition_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__discharge_disposition') }} as t
        on m.discharge_disposition_code = t.discharge_disposition_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'admit_source_code' as field_name,
        'terminology__admit_source' as terminology_table,
        'admit_source_code' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.admit_source_code, '') is null then 'null'
            when t.admit_source_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__admit_source') }} as t
        on m.admit_source_code = t.admit_source_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'admit_type_code' as field_name,
        'terminology__admit_type' as terminology_table,
        'admit_type_code' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.admit_type_code, '') is null then 'null'
            when t.admit_type_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__admit_type') }} as t
        on m.admit_type_code = t.admit_type_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'bill_type_code' as field_name,
        'terminology__bill_type' as terminology_table,
        'bill_type_code' as terminology_field,
        case
            when not (m.claim_type = 'institutional') then 'not_applicable'
            when nullif(m.bill_type_code, '') is null then 'null'
            when t.bill_type_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__bill_type') }} as t
        on m.bill_type_code = t.bill_type_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'place_of_service_code' as field_name,
        'terminology__place_of_service' as terminology_table,
        'place_of_service_code' as terminology_field,
        case
            when not (m.claim_type = 'professional') then 'not_applicable'
            when nullif(m.place_of_service_code, '') is null then 'null'
            when t.place_of_service_code is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__place_of_service') }} as t
        on m.place_of_service_code = t.place_of_service_code

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'rendering_npi' as field_name,
        'terminology__provider' as terminology_table,
        'npi' as terminology_field,
        case
            when nullif(m.rendering_npi, '') is null then 'null'
            when t.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__provider') }} as t
        on m.rendering_npi = t.npi

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'billing_npi' as field_name,
        'terminology__provider' as terminology_table,
        'npi' as terminology_field,
        case
            when nullif(m.billing_npi, '') is null then 'null'
            when t.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__provider') }} as t
        on m.billing_npi = t.npi

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'facility_npi' as field_name,
        'terminology__provider' as terminology_table,
        'npi' as terminology_field,
        case
            when nullif(m.facility_npi, '') is null then 'null'
            when t.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m
    left join {{ ref('terminology__provider') }} as t
        on m.facility_npi = t.npi

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'ndc_code' as field_name,
        'terminology__ndc' as terminology_table,
        'ndc' as terminology_field,
        case
            when nullif(p.ndc_code, '') is null then 'null'
            when t.ndc is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p
    left join {{ ref('terminology__ndc') }} as t
        on p.ndc_code = t.ndc

),

aggregated as (

    select
        data_source,
        payer,
        plan_name,
        model_name,
        field_name,
        terminology_table,
        terminology_field,
        count(*) as total_rows,
        sum(case when mapping_status <> 'not_applicable' then 1 else 0 end) as applicable_rows,
        sum(case when mapping_status = 'not_applicable' then 1 else 0 end) as not_applicable_rows,
        sum(case when mapping_status = 'null' then 1 else 0 end) as null_rows,
        sum(case when mapping_status in ('valid', 'invalid') then 1 else 0 end) as non_null_rows,
        sum(case when mapping_status = 'valid' then 1 else 0 end) as valid_rows,
        sum(case when mapping_status = 'invalid' then 1 else 0 end) as invalid_rows
    from field_observations
    group by
        data_source,
        payer,
        plan_name,
        model_name,
        field_name,
        terminology_table,
        terminology_field

)

select
    rm.run_id,
    rm.run_ts,
    a.data_source,
    a.payer,
    a.plan_name as {{ the_tuva_project.quote_column('plan') }},
    a.model_name,
    a.field_name,
    a.terminology_table,
    a.terminology_field,
    a.total_rows,
    a.applicable_rows,
    a.not_applicable_rows,
    a.null_rows,
    a.non_null_rows,
    a.valid_rows,
    a.invalid_rows,
    case when a.total_rows > 0 then 100.0 else 0.0 end as total_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.applicable_rows', 'a.total_rows') }}, 0.0) as applicable_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.not_applicable_rows', 'a.total_rows') }}, 0.0) as not_applicable_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.null_rows', 'a.total_rows') }}, 0.0) as null_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.non_null_rows', 'a.total_rows') }}, 0.0) as non_null_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.valid_rows', 'a.total_rows') }}, 0.0) as valid_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.invalid_rows', 'a.total_rows') }}, 0.0) as invalid_rows_pct,
    coalesce({{ dbt_utils.safe_divide('100.0 * a.valid_rows', 'a.applicable_rows') }}, 0.0) as valid_rows_applicable_pct
from aggregated as a
cross join run_meta as rm
order by
    a.data_source,
    a.payer,
    a.plan_name,
    a.model_name,
    a.field_name
