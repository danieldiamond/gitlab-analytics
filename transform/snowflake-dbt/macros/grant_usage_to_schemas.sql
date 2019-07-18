{%- macro grant_usage_to_schemas(schema_name, rolename=reporter) -%}

	{%- set schema_name = target.schema -%}

	{%- set roles = [
			'analyst_growth',
			'analyst_finance',
			'analyst_core',
			'analyst_sensitive',
			'engineer',
			'intern',
			'loader',
			'product_manager',
			'reporter',
			'reporter_staging',
			'reporter_sensitive',
			'transformer'
		] -%}

	{%- for role_name in roles -%}

		{%- if target.name == 'prod' -%}
			grant usage on schema {{ schema_name }} to role {{ role_name }};
			grant select on all tables in schema {{ schema_name }} to role {{ role_name }};
			grant select on all views in schema {{ schema_name }} to role {{ role_name }};
			grant usage on schema {{ schema_name }}_meta to role {{ role_name }};
			grant select on all tables in schema {{ schema_name }}_meta to role {{ role_name }};
			grant select on all views in schema {{ schema_name }}_meta to role {{ role_name }};
		{%- endif -%}

		{%- if target.name == 'prod' and role_name != 'reporter' -%}
			grant usage on schema {{ schema_name }}_staging to role {{ role_name }};
			grant select on all tables in schema {{ schema_name }}_staging to role {{ role_name }};
			grant select on all views in schema {{ schema_name }}_staging to role {{ role_name }};
		{%- endif -%}

		{%- if target.name == 'prod' and role_name in ('analyst_sensitive','reporter_sensitive') -%}
			grant usage on schema sensitive to role {{ role_name }};
			grant select on all tables in schema sensitive to role {{ role_name }};
			grant select on all views in schema sensitive to role {{ role_name }};
		{%- endif -%}

	{%- endfor -%}

{%- endmacro -%} 
