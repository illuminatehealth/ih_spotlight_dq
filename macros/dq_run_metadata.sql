{% macro dq_run_id() -%}
    '{{ var("dq_run_id", invocation_id) }}'
{%- endmacro %}

{% macro dq_run_ts() -%}
    {{ return(adapter.dispatch('dq_run_ts', 'ih_spotlight_dq')()) }}
{%- endmacro %}

{% macro default__dq_run_ts() -%}
    {% set fallback_run_ts = run_started_at.strftime("%Y-%m-%d %H:%M:%S") %}
    {% set tuva_last_run = var('tuva_last_run', fallback_run_ts) %}
    {% set tuva_last_run_string = tuva_last_run | string | trim %}
    {% set has_timestamp_shape = (
        tuva_last_run_string | length >= 19
        and tuva_last_run_string[4] == '-'
        and tuva_last_run_string[7] == '-'
        and (tuva_last_run_string[10] == ' ' or tuva_last_run_string[10] == 'T')
        and tuva_last_run_string[13] == ':'
        and tuva_last_run_string[16] == ':'
    ) %}

    {% if tuva_last_run is none or tuva_last_run_string == '' or not has_timestamp_shape %}
        cast('{{ fallback_run_ts }}' as {{ dbt.type_timestamp() }})
    {% else %}
        coalesce(
            {{ dbt.safe_cast(dbt.string_literal(tuva_last_run_string), dbt.type_timestamp()) }},
            cast('{{ fallback_run_ts }}' as {{ dbt.type_timestamp() }})
        )
    {% endif %}
{%- endmacro %}
