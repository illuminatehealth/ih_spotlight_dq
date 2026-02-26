{% macro dq_should_persist_results() -%}
    {{ return(var('dq_persist_results', false) | as_bool) }}
{%- endmacro %}

{% macro dq_history_incremental_strategy() -%}
    {% set merge_adapters = ['bigquery', 'databricks', 'redshift', 'snowflake', 'spark'] %}
    {% if target.type in merge_adapters %}
        {{ return('merge') }}
    {% else %}
        {{ return('delete+insert') }}
    {% endif %}
{%- endmacro %}
