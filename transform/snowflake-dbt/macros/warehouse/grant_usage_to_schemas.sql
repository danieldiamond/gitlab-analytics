{%- macro grant_usage_to_schemas() -%}

	{%- set schema_name = target.schema -%}

    {#
        This works in conjunction with the Permifrost roles.yml file. 
        This will only run on production and mainly covers our bases so that
        new models created will be immediately available for querying to the 
        roles listed.
    #}
    
    {%- set non_sensitive = 'dbt_analytics' -%}
    {%- set sensitive = 'dbt_analytics_sensitive' -%}

    {%- if target.name == 'prod' -%}
        grant usage on schema {{ schema_name }} to role {{ non_sensitive }};
        grant select on all tables in schema {{ schema_name }} to role {{ non_sensitive }};
        grant select on all views in schema {{ schema_name }} to role {{ non_sensitive }};

        grant usage on schema {{ schema_name }}_meta to role {{ non_sensitive }};
        grant select on all tables in schema {{ schema_name }}_meta to role {{ non_sensitive }};
        grant select on all views in schema {{ schema_name }}_meta to role {{ non_sensitive }};

        grant usage on schema {{ schema_name }}_staging to role {{ non_sensitive }};
        grant select on all tables in schema {{ schema_name }}_staging to role {{ non_sensitive }};
        grant select on all views in schema {{ schema_name }}_staging to role {{ non_sensitive }};

        grant usage on schema {{ schema_name }}_sensitive to role {{ sensitive }};
        grant select on all tables in schema {{ schema_name }}_sensitive to role {{ sensitive }};
        grant select on all views in schema {{ schema_name }}_sensitive to role {{ sensitive }};
    {%- endif -%}

{%- endmacro -%} 
