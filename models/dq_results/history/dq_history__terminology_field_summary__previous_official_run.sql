{{ config(
    materialized='view',
    schema='spotlight',
    tags=['spotlight', 'dq_results', 'dq_history']
) }}

with ranked_runs as (

    select
        run_id,
        run_ts,
        dense_rank() over (order by run_ts desc, run_id desc) as run_rank
    from (
        select distinct
            run_id,
            run_ts
        from {{ ref('dq_history__terminology_field_summary') }}
    ) as distinct_runs

)

select h.*
from {{ ref('dq_history__terminology_field_summary') }} as h
inner join ranked_runs as r
    on h.run_id = r.run_id
    and h.run_ts = r.run_ts
where r.run_rank = 2
