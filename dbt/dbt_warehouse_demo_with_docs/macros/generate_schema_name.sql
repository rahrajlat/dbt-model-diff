{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set forced = var('dbt_diff_schema', none) -%}
  {%- if forced is not none -%}
    {{ forced }}
  {%- else -%}
    {{ default__generate_schema_name(custom_schema_name, node) }}
  {%- endif -%}
{%- endmacro %}