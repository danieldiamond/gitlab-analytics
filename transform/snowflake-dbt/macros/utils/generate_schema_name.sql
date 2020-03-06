-- Will write to custom schemas not on prod
-- Ensures logging package writes to analytics_meta

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set production_targets = ('prod','docs','ci') -%}

    {%- set prefixed_schemas = ('meta','sensitive','staging','temporary') -%}

    {%- if target.name in production_targets -%}
        
        {%- if custom_schema_name in prefixed_schemas -%}

            {{ target.schema.lower() }}_{{ custom_schema_name | trim }}

        {%- else -%}
            
            {{ custom_schema_name.lower() | trim }}

        {%- endif -%}

    {%- else -%}
    
        {{ target.schema.lower() }}_{{ custom_schema_name | trim }}
    
    {%- endif -%}

{%- endmacro %}
