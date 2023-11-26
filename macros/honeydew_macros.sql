{%- macro honeydew_native_app_call(call_name, workspace_name, parameter) %}
  {%- set query_sql %}
    select HONEYDEW_APP.API.{{ call_name }}('{{ workspace_name }}', '{{ parameter }}');
  {%- endset %}
{%- set results = run_query(query_sql) %}
{%- if execute %}
{%- set results_list = results.columns[0].values() %}
{%- else %}
{%- set results_list = [] %}
{%- endif %}
{%- for query_value in results_list %}
{{ query_value }}
{%- endfor %}
{%- endmacro %}
{%- macro get_honeydew_entity_sql(entity_name) %}
{%- set hd_output = honeydew_native_app_call('GET_ENTITY_SQL', var('honeydew_workspace'), entity_name) %}
{{ hd_output }}
{%- endmacro %}
{%- macro get_honeydew_dataset_sql(dataset_name) %}
{%- set hd_output = honeydew_native_app_call('GET_DYNAMIC_DATASET_SQL', var('honeydew_workspace'), dataset_name) %}
{{ hd_output }}
{%- endmacro %}

