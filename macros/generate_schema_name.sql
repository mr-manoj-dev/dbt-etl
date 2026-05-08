/*
  ============================================================
  Macro: generate_schema_name
  Purpose:
    Overrides dbt's default schema naming behaviour for BigQuery.
    By default dbt appends the target schema as a suffix to the custom schema
    (e.g. dbt_dev_silver). This macro uses the custom schema name directly,
    producing clean dataset names: silver, gold, seeds.
    In production the target schema itself is used when no custom schema is set.
  ============================================================
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- set default_schema = target.schema -%}

  {%- if custom_schema_name is none -%}
    {{ default_schema }}

  {%- else -%}
    {# Use the custom schema name directly — no prefix with target schema #}
    {{ custom_schema_name | trim }}

  {%- endif -%}

{%- endmacro %}
