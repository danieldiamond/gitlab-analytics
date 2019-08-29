-- Will write to custom schemas not on prod
-- Ensures logging package writes to analytics_meta

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none 
            or
        (target.name in ('prod','ci') and custom_schema_name.lower() == default_schema.lower())
    -%}
        {{ default_schema.lower() | trim }}

    {%- elif custom_schema_name in ('analytics','meta','sensitive','staging') -%}

    	{{ default_schema.lower() }}_{{ custom_schema_name | trim }}

    {%- else -%}

        {{ custom_schema_name.lower() | trim }}

    {%- endif -%}

{%- endmacro %}
