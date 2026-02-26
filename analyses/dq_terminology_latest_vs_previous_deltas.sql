-- Latest vs previous official run deltas by terminology field grain.
-- Returns one row per field in either run, with row-count and percentage deltas.

with latest as (

    select *
    from {{ ref('dq_history__terminology_field_summary__latest_official_run') }}

),

previous as (

    select *
    from {{ ref('dq_history__terminology_field_summary__previous_official_run') }}

)

select
    coalesce(l.data_source, p.data_source) as data_source,
    coalesce(l.payer, p.payer) as payer,
    coalesce(l.plan, p.plan) as plan,
    coalesce(l.model_name, p.model_name) as model_name,
    coalesce(l.field_name, p.field_name) as field_name,
    coalesce(l.terminology_table, p.terminology_table) as terminology_table,
    coalesce(l.terminology_field, p.terminology_field) as terminology_field,

    l.run_id as latest_run_id,
    l.run_ts as latest_run_ts,
    p.run_id as previous_run_id,
    p.run_ts as previous_run_ts,

    coalesce(l.total_rows, 0) as latest_total_rows,
    coalesce(p.total_rows, 0) as previous_total_rows,
    coalesce(l.total_rows, 0) - coalesce(p.total_rows, 0) as delta_total_rows,

    coalesce(l.invalid_rows, 0) as latest_invalid_rows,
    coalesce(p.invalid_rows, 0) as previous_invalid_rows,
    coalesce(l.invalid_rows, 0) - coalesce(p.invalid_rows, 0) as delta_invalid_rows,

    coalesce(l.invalid_rows_pct, 0.0) as latest_invalid_rows_pct,
    coalesce(p.invalid_rows_pct, 0.0) as previous_invalid_rows_pct,
    coalesce(l.invalid_rows_pct, 0.0) - coalesce(p.invalid_rows_pct, 0.0) as delta_invalid_rows_pct,

    coalesce(l.valid_rows_pct, 0.0) as latest_valid_rows_pct,
    coalesce(p.valid_rows_pct, 0.0) as previous_valid_rows_pct,
    coalesce(l.valid_rows_pct, 0.0) - coalesce(p.valid_rows_pct, 0.0) as delta_valid_rows_pct

from latest as l
full outer join previous as p
    on l.data_source = p.data_source
    and coalesce(l.payer, '') = coalesce(p.payer, '')
    and coalesce(l.plan, '') = coalesce(p.plan, '')
    and l.model_name = p.model_name
    and l.field_name = p.field_name
order by
    1, 2, 3, 4, 5
