{% macro create_udfs() %}

create schema if not exists {{target.schema}};
	
{{sfdc_id_15_to_18()}};

{{zuora_slugify()}}

{% endmacro %}