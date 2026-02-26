{{ config(
    materialized='incremental',
    schema='spotlight',
    tags=['spotlight', 'dq_results', 'dq_history'],
    unique_key=['run_id', 'data_source', 'payer', 'plan', 'model_name', 'field_name'],
    incremental_strategy=dq_history_incremental_strategy(),
    on_schema_change='append_new_columns'
) }}

-- depends_on: {{ ref('dq_results__terminology_field_summary') }}

{% if dq_should_persist_results() %}

    select
        run_id,
        run_ts,
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }},
        model_name,
        field_name,
        terminology_table,
        terminology_field,
        total_rows,
        applicable_rows,
        not_applicable_rows,
        null_rows,
        non_null_rows,
        valid_rows,
        invalid_rows,
        total_rows_pct,
        applicable_rows_pct,
        not_applicable_rows_pct,
        null_rows_pct,
        non_null_rows_pct,
        valid_rows_pct,
        invalid_rows_pct,
        valid_rows_applicable_pct
    from {{ ref('dq_results__terminology_field_summary') }}

{% else %}

    select
        run_id,
        run_ts,
        data_source,
        payer,
        {{ the_tuva_project.quote_column('plan') }},
        model_name,
        field_name,
        terminology_table,
        terminology_field,
        total_rows,
        applicable_rows,
        not_applicable_rows,
        null_rows,
        non_null_rows,
        valid_rows,
        invalid_rows,
        total_rows_pct,
        applicable_rows_pct,
        not_applicable_rows_pct,
        null_rows_pct,
        non_null_rows_pct,
        valid_rows_pct,
        invalid_rows_pct,
        valid_rows_applicable_pct
    from {{ ref('dq_results__terminology_field_summary') }}
    where 1 = 0

{% endif %}
