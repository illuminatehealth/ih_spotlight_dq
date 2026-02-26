select *
from {{ ref('dq_results__input_layer_field_summary') }}
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
