{{ config(
    materialized='view',
    schema='spotlight',
    tags=['spotlight', 'reconciliation_dashboard', 'dq_history']
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
        from {{ ref('reconciliation_history__monthly_summary') }}
    ) as distinct_runs

)

select h.*
from {{ ref('reconciliation_history__monthly_summary') }} as h
inner join ranked_runs as r
    on h.run_id = r.run_id
    and h.run_ts = r.run_ts
where r.run_rank = 1
