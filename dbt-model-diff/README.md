# dbt-model-diff (Postgres v1)

Compare dbt model **output data** between `main` and a feature branch.

## Setup (required in dbt project)
Add this macro:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set forced = var('dbt_diff_schema', none) -%}
  {%- if forced is not none -%}
    {{ forced }}
  {%- else -%}
    {{ default__generate_schema_name(custom_schema_name, node) }}
  {%- endif -%}
{%- endmacro %}
```

## Install
```bash
pip install -e .
```

## Usage
```bash
dbt-model-diff data-compare my_model --keys id
```

Supports:
- rowcount diff
- added / removed / changed rows
- Postgres only (v1)\n