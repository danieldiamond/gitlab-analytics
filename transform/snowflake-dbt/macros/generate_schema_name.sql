{% macro generate_schema_name(custom_schema_name=none) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}_staging

    {%- elif
        (target.name != 'prod' and custom_schema_name is not none and custom_schema_name != 'analytics')
        or
        custom_schema_name == 'meta'
    -%}
        -- Will write to custom schemas not on prod
        -- Avoids analytics_analytics for snowplow package
        -- Ensures logging package writes to analytics_meta
    	{{ default_schema }}_{{ custom_schema_name | trim }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
