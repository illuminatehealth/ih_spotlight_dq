select *
from {{ ref('dq_results__terminology_field_summary') }}
where total_rows_pct < 0
   or total_rows_pct > 100
   or applicable_rows_pct < 0
   or applicable_rows_pct > 100
   or not_applicable_rows_pct < 0
   or not_applicable_rows_pct > 100
   or null_rows_pct < 0
   or null_rows_pct > 100
   or non_null_rows_pct < 0
   or non_null_rows_pct > 100
   or valid_rows_pct < 0
   or valid_rows_pct > 100
   or invalid_rows_pct < 0
   or invalid_rows_pct > 100
   or valid_rows_applicable_pct < 0
   or valid_rows_applicable_pct > 100
