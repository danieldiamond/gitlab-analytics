{%- macro schema_union_all(schema_part, table_name) -%}
 
 {% call statement('get_schemata', fetch_result=True) %}

    SELECT DISTINCT '"' || table_schema || '"."' || table_name || '"'
    FROM analytics.information_schema.tables
    WHERE table_schema ILIKE '%{{ schema_part }}%'
    AND table_name ILIKE '{{ table_name }}'
	
  {%- endcall -%}

    {%- set value_list = load_result('get_schemata') -%}

    {%- if value_list and value_list['data'] -%}

        {%- set values = value_list['data'] | map(attribute=0) | list %}

            {% for thing in values %}
                SELECT * 
                FROM {{ thing }}

            {%- if not loop.last %}      
                UNION ALL
            {% endif -%}
            
            {% endfor -%}
    
    {%- else -%}
    
        {{ return(1) }}
    
    {%- endif %}

{%- endmacro -%}
