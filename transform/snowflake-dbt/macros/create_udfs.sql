{% macro create_udfs() %}

create schema if not exists {{target.schema}}_staging;
	
{{sfdc_id_15_to_18()}}

create schema if not exists {{target.schema}}_analytics;

{% endmacro %}