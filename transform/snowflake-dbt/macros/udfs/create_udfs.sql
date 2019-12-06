{% macro create_udfs() %}

  create schema if not exists {{target.schema}}_staging;

    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}

  create schema if not exists {{target.schema}}_analytics;

{% endmacro %}
