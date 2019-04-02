{%- macro get_pings_json_keys() -%}
  {%- call statement('stats_used', fetch_result=True) -%}
     SELECT DISTINCT f.path, REPLACE(f.path, '.','_')
     FROM {{ ref('pings_usage_data') }},
        lateral flatten(input => {{ ref('pings_usage_data') }}.stats_used, recursive => True) f
     WHERE IS_OBJECT(f.value) = FALSE
  {%- endcall -%}
{%- endmacro -%}
