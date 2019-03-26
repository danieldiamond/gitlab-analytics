{%- macro grant_usage_to_schemas(schema_name, rolename=reporter) -%}
	{%- set schema_name = target.schema -%}
    {%- if target.name == 'prod' -%}
	    grant usage on schema {{ schema_name }} to role reporter;
		grant select on all tables in schema {{ schema_name }} to role reporter;
	    grant select on all views in schema {{ schema_name }} to role reporter;
	    grant usage on schema {{ schema_name }}_meta to role reporter;
		grant select on all tables in schema {{ schema_name }}_meta to role reporter;
	    grant select on all views in schema {{ schema_name }}_meta to role reporter;
  	{%- else -%} 
  		grant usage on schema {{ schema_name }}_analytics to role reporter;
		grant select on all tables in schema {{ schema_name }}_analytics to role reporter;
	    grant select on all views in schema {{ schema_name }}_analytics to role reporter;
  		grant usage on schema {{ schema_name }}_staging to role reporter;
		grant select on all tables in schema {{ schema_name }}_staging to role reporter;
	    grant select on all views in schema {{ schema_name }}_staging to role reporter;
	    grant usage on schema {{ schema_name }}_meta to role reporter;
		grant select on all tables in schema {{ schema_name }}_meta to role reporter;
	    grant select on all views in schema {{ schema_name }}_meta to role reporter;
	{%- endif -%}
{%- endmacro -%} 
