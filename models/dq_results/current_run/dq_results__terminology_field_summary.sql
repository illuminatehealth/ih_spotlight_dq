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
        gender,
        race,
        dual_status_code,
        original_reason_entitlement_code,
        medicare_status_code,
        birth_date,
        death_date,
        social_security_number,
        replace(social_security_number, '-', '') as social_security_number_digits,
        address,
        city,
        state,
        zip_code,
        replace(zip_code, '-', '') as zip_code_digits,
        phone,
        replace(
            replace(
                replace(
                    replace(
                        replace(phone, '-', ''),
                        '(',
                        ''
                    ),
                    ')',
                    ''
                ),
                ' ',
                ''
            ),
            '.',
            ''
        ) as phone_digits,
        email
    from {{ ref('input_layer__eligibility') }}

),

state_reference as (

    select distinct
        ansi_fips_state_abbreviation as state_value
    from {{ ref('reference_data__ansi_fips_state') }}

    union

    select distinct
        ansi_fips_state_name as state_value
    from {{ ref('reference_data__ansi_fips_state') }}

    union

    select distinct
        ansi_fips_state_code as state_value
    from {{ ref('reference_data__ansi_fips_state') }}

),

medical_claim as (

    select
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }} as plan_name,
        claim_type,
        drg_code_type,
        drg_code,
        claim_start_date,
        claim_end_date,
        claim_line_start_date,
        claim_line_end_date,
        admission_date,
        discharge_date,
        paid_date,
        service_unit_quantity,
        revenue_center_code,
        hcpcs_code,
        hcpcs_modifier_1,
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
        prescribing_provider_npi,
        dispensing_provider_npi,
        dispensing_date,
        ndc_code,
        quantity,
        days_supply,
        refills,
        paid_date
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
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'claim_type' as field_name,
        'validation_rule' as terminology_table,
        'professional|institutional' as terminology_field,
        case
            when m.claim_type is null or m.claim_type = '' then 'null'
            when m.claim_type in ('professional', 'institutional') then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'claim_start_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.claim_start_date is null then 'null'
            when m.claim_start_date > cast('1970-01-01' as date)
                and m.claim_start_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'claim_end_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.claim_end_date is null then 'null'
            when m.claim_end_date > cast('1970-01-01' as date)
                and m.claim_end_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'claim_line_start_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.claim_line_start_date is null then 'null'
            when m.claim_line_start_date > cast('1970-01-01' as date)
                and m.claim_line_start_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'claim_line_end_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.claim_line_end_date is null then 'null'
            when m.claim_line_end_date > cast('1970-01-01' as date)
                and m.claim_line_end_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'admission_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.admission_date is null then 'null'
            when m.admission_date > cast('1970-01-01' as date)
                and m.admission_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'discharge_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.discharge_date is null then 'null'
            when m.discharge_date > cast('1970-01-01' as date)
                and m.discharge_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'service_unit_quantity' as field_name,
        'validation_rule' as terminology_table,
        '> 0 and non_null' as terminology_field,
        case
            when m.service_unit_quantity is null then 'null'
            when m.service_unit_quantity > 0 then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'hcpcs_modifier_1' as field_name,
        'validation_rule' as terminology_table,
        'non_null' as terminology_field,
        case
            when m.hcpcs_modifier_1 is null then 'null'
            else 'valid'
        end as mapping_status
    from medical_claim as m

    union all

    select
        m.data_source,
        m.payer,
        m.plan_name,
        'input_layer__medical_claim' as model_name,
        'paid_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when m.paid_date is null then 'null'
            when m.paid_date > cast('1970-01-01' as date)
                and m.paid_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from medical_claim as m

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
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'gender' as field_name,
        'validation_rule' as terminology_table,
        'male|female|unknown' as terminology_field,
        case
            when e.gender is null or e.gender = '' then 'null'
            when e.gender in ('male', 'female', 'unknown') then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'original_reason_entitlement_code' as field_name,
        'validation_rule' as terminology_table,
        '0|1|2|3' as terminology_field,
        case
            when e.original_reason_entitlement_code is null or e.original_reason_entitlement_code = ''
                then 'null'
            when e.original_reason_entitlement_code in ('0', '1', '2', '3') then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'medicare_status_code' as field_name,
        'validation_rule' as terminology_table,
        '00|10|11|20|21|31|40' as terminology_field,
        case
            when e.medicare_status_code is null or e.medicare_status_code = '' then 'null'
            when e.medicare_status_code in ('00', '10', '11', '20', '21', '31', '40')
                then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'birth_date' as field_name,
        'validation_rule' as terminology_table,
        '>1900-01-01 and < current_date' as terminology_field,
        case
            when e.birth_date is null then 'null'
            when e.birth_date > cast('1900-01-01' as date)
                and e.birth_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'death_date' as field_name,
        'validation_rule' as terminology_table,
        '>1900-01-01 and < current_date' as terminology_field,
        case
            when e.death_date is null then 'null'
            when e.death_date > cast('1900-01-01' as date)
                and e.death_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'social_security_number' as field_name,
        'validation_rule' as terminology_table,
        '9 digits or ###-##-####' as terminology_field,
        case
            when e.social_security_number is null or e.social_security_number = '' then 'null'
            when (
                len(e.social_security_number) = 9
                and patindex('%[^0-9]%', e.social_security_number) = 0
            ) or (
                len(e.social_security_number) = 11
                and substring(e.social_security_number, 4, 1) = '-'
                and substring(e.social_security_number, 7, 1) = '-'
                and len(e.social_security_number_digits) = 9
                and patindex('%[^0-9]%', e.social_security_number_digits) = 0
            )
                then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'address' as field_name,
        'validation_rule' as terminology_table,
        'non_null and non_blank and != ''NULL''' as terminology_field,
        case
            when e.address is null or e.address = '' or e.address = 'NULL' then 'null'
            else 'valid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'city' as field_name,
        'validation_rule' as terminology_table,
        'non_null and non_blank and != ''NULL''' as terminology_field,
        case
            when e.city is null or e.city = '' or e.city = 'NULL' then 'null'
            else 'valid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'state' as field_name,
        'reference_data__ansi_fips_state' as terminology_table,
        'ansi_fips_state_abbreviation|ansi_fips_state_name|ansi_fips_state_code' as terminology_field,
        case
            when e.state is null or e.state = '' or e.state = 'NULL' then 'null'
            when sr.state_value is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e
    left join state_reference as sr
        on e.state = sr.state_value

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'zip_code' as field_name,
        'validation_rule' as terminology_table,
        '5 digits or 9-digit zip (with or without hyphen)' as terminology_field,
        case
            when e.zip_code is null or e.zip_code = '' then 'null'
            when (
                len(e.zip_code) in (5, 9)
                and patindex('%[^0-9]%', e.zip_code) = 0
            ) or (
                len(e.zip_code) = 10
                and substring(e.zip_code, 6, 1) = '-'
                and len(e.zip_code_digits) = 9
                and patindex('%[^0-9]%', e.zip_code_digits) = 0
            )
                then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'phone' as field_name,
        'validation_rule' as terminology_table,
        '10 digits (or formatted equivalent), or 11 digits starting with 1' as terminology_field,
        case
            when e.phone is null or e.phone = '' then 'null'
            when patindex('%[^0-9]%', e.phone_digits) = 0
                and (
                    len(e.phone_digits) = 10
                    or (
                        len(e.phone_digits) = 11
                        and left(e.phone_digits, 1) = '1'
                    )
                )
                then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

    union all

    select
        e.data_source,
        e.payer,
        e.plan_name,
        'input_layer__eligibility' as model_name,
        'email' as field_name,
        'validation_rule' as terminology_table,
        'non_null and contains @' as terminology_field,
        case
            when e.email is null or e.email = '' then 'null'
            when e.email like '%@%' then 'valid'
            else 'invalid'
        end as mapping_status
    from eligibility as e

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

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'prescribing_provider_npi' as field_name,
        'terminology__provider' as terminology_table,
        'npi' as terminology_field,
        case
            when nullif(p.prescribing_provider_npi, '') is null then 'null'
            when t.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p
    left join {{ ref('terminology__provider') }} as t
        on p.prescribing_provider_npi = t.npi

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'dispensing_provider_npi' as field_name,
        'terminology__provider' as terminology_table,
        'npi' as terminology_field,
        case
            when nullif(p.dispensing_provider_npi, '') is null then 'null'
            when t.npi is not null then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p
    left join {{ ref('terminology__provider') }} as t
        on p.dispensing_provider_npi = t.npi

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'dispensing_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when p.dispensing_date is null then 'null'
            when p.dispensing_date > cast('1970-01-01' as date)
                and p.dispensing_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'quantity' as field_name,
        'validation_rule' as terminology_table,
        '> 0' as terminology_field,
        case
            when p.quantity is null then 'null'
            when p.quantity > 0 then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'days_supply' as field_name,
        'validation_rule' as terminology_table,
        '> 0' as terminology_field,
        case
            when p.days_supply is null then 'null'
            when p.days_supply > 0 then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'refills' as field_name,
        'validation_rule' as terminology_table,
        '> 0' as terminology_field,
        case
            when p.refills is null then 'null'
            when p.refills > 0 then 'valid'
            else 'invalid'
        end as mapping_status
    from pharmacy_claim as p

    union all

    select
        p.data_source,
        p.payer,
        p.plan_name,
        'input_layer__pharmacy_claim' as model_name,
        'paid_date' as field_name,
        'validation_rule' as terminology_table,
        '>1970-01-01 and < current_date' as terminology_field,
        case
            when p.paid_date is null then 'null'
            when p.paid_date > cast('1970-01-01' as date)
                and p.paid_date < cast({{ dbt.current_timestamp() }} as date)
                then 'valid'
            else 'invalid'
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
